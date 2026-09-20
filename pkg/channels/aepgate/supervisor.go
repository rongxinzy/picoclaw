package aepgate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// supervisorLogger bridges the picoclaw logger into the aep package.
type supervisorLogger struct{}

func (supervisorLogger) Infof(format string, args ...any)  { logger.Infof(format, args...) }
func (supervisorLogger) Warnf(format string, args ...any)  { logger.Warnf(format, args...) }
func (supervisorLogger) Errorf(format string, args ...any) { logger.Errorf(format, args...) }

const (
	defaultForkTTL     = 30 * time.Minute
	defaultPortRange   = 18900
	childHealthTimeout = 45 * time.Second
)

// fork is one running ephemeral digital employee: a child picoclaw process
// bound to a conversation-scoped AEP account whose data scope is a frozen
// snapshot of one requester's.
type fork struct {
	requesterID string
	agentID     string
	username    string
	password    string
	sessionID   string
	homeDir     string
	port        int
	relaySecret string
	cancel      context.CancelFunc
	lastUsed    time.Time
	expiresAt   time.Time
}

// URL is the fork's aepchat base URL.
func (f *fork) URL() string { return fmt.Sprintf("http://127.0.0.1:%d", f.port) }

// Relay returns the fork's relay endpoint credentials: the base URL and the
// one-time secret authenticating turn-relay calls from the resident gateway.
func (f *fork) Relay() (baseURL, secret string) {
	return f.URL(), f.relaySecret
}

// Supervisor spawns, reuses, and reaps ephemeral forks on behalf of the
// warden. Lifecycle authority lives in a dedicated supervisor account
// (users.write); the resident digital employee never holds provisioning
// rights itself.
type Supervisor struct {
	client       *aep.Client
	baseURL      string
	deploymentID string
	homeTeamID   string
	settings     *config.WardenSettings
	knowledge    *config.KnowledgeConfig

	supervisor *aep.Manager // session carrying lifecycle authority

	mu       sync.Mutex
	forks    map[string]*fork // requester user id → fork
	nextPort int
	binary   string
	// healthTimeout bounds the child health probe; swappable in tests.
	healthTimeout time.Duration
	// reapInterval paces the expiry sweep; swappable in tests.
	reapInterval time.Duration
}

// NewSupervisor logs the supervisor account in and starts the reaper.
func NewSupervisor(cfg *config.Config, settings *config.WardenSettings) (*Supervisor, error) {
	if settings == nil || settings.RuntimeRoleID == "" {
		return nil, fmt.Errorf("aepchat warden requires warden.runtime_role_id")
	}
	if cfg.AEP.SupervisorUsername == "" || cfg.AEP.SupervisorPassword.String() == "" {
		return nil, fmt.Errorf("aepchat warden requires aep.supervisor_username and aep.supervisor_password")
	}
	supervisor := aep.NewManager(aep.Config{
		BaseURL: cfg.AEP.BaseURL, DeploymentID: cfg.AEP.DeploymentID,
		Username: cfg.AEP.SupervisorUsername, Password: cfg.AEP.SupervisorPassword.String(),
		SessionID: "picoclaw-supervisor", Logger: supervisorLogger{},
	})
	if err := supervisor.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("aepchat supervisor login failed: %w", err)
	}
	binary, err := os.Executable()
	if err != nil {
		supervisor.Stop()
		return nil, err
	}
	portRange := defaultPortRange
	if settings.PortRangeStart > 0 {
		portRange = settings.PortRangeStart
	}
	knowledge := &cfg.Knowledge
	s := &Supervisor{
		client: aep.NewClient(cfg.AEP.BaseURL), baseURL: cfg.AEP.BaseURL, deploymentID: cfg.AEP.DeploymentID,
		homeTeamID: cfg.AEP.HomeTeamID, settings: settings, knowledge: knowledge,
		supervisor: supervisor, forks: make(map[string]*fork),
		nextPort: portRange, binary: binary, healthTimeout: childHealthTimeout,
	}
	if s.reapInterval <= 0 {
		s.reapInterval = time.Minute
	}
	go s.reaper(context.Background())
	return s, nil
}

// Stop shuts the supervisor session down; forks are reaped by the caller's
// process exit.
func (s *Supervisor) Stop() {
	s.supervisor.Stop()
}

// Ensure returns the requester's live fork, spawning one if needed. The
// fork's home team is the requester's own team (necessarily inside the
// requester's scope, satisfying the control plane's confinement guard), and
// its data scope is frozen from the requester at creation.
func (s *Supervisor) Ensure(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (*fork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fork, ok := s.forks[requester.UserID]; ok && fork.healthy() {
		fork.lastUsed = time.Now()
		return fork, nil
	}
	fork, err := s.spawn(ctx, requester, requesterCtx)
	if err != nil {
		return nil, err
	}
	s.forks[requester.UserID] = fork
	return fork, nil
}

func (f *fork) healthy() bool {
	return time.Now().Before(f.expiresAt)
}

func (s *Supervisor) spawn(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (*fork, error) {
	// The requester's shallowest own team anchors the fork; the first entry
	// is a deterministic choice, and the control plane re-validates that it
	// lies inside the requester's scope on creation.
	homeTeam := s.homeTeamID
	for _, team := range requesterCtx.OwnTeamIDs {
		homeTeam = team
		break
	}
	suffix := randomSuffix()
	username := fmt.Sprintf("eph-%s-%s", sanitize(requester.UserID), suffix)
	password := "eph-" + randomSuffix() + "-" + randomSuffix()
	relaySecret := randomToken(32)
	ttl := defaultForkTTL
	if s.settings.TTLMinutes > 0 {
		ttl = time.Duration(s.settings.TTLMinutes) * time.Minute
	}
	expiresAt := time.Now().Add(ttl).UTC()
	models := s.residentModels()

	token, err := s.supervisor.AccessToken(ctx)
	if err != nil {
		return nil, err
	}
	record, p := s.client.CreateAgent(ctx, token, aep.EphemeralAgentInput{
		Username:        username,
		DisplayName:     fmt.Sprintf("%s · %s", "Fork", requester.DisplayName),
		Password:        password,
		RoleIDs:         []string{s.settings.RuntimeRoleID},
		TeamIDs:         []string{},
		HomeTeamID:      homeTeam,
		PromptSkillID:   s.settings.PromptSkillID,
		Ephemeral:       true,
		ExpiresAt:       expiresAt.Format(time.RFC3339Nano),
		ScopeFromUserID: requester.UserID,
		ModelIDs:        models,
	})
	if p != nil {
		return nil, fmt.Errorf("ephemeral provisioning failed: %v", p)
	}

	port := s.nextPort
	s.nextPort++
	homeDir, err := os.MkdirTemp("", "aep-fork-")
	if err != nil {
		return nil, err
	}
	childCtx, cancel := context.WithCancel(context.Background())
	fork := &fork{
		requesterID: requester.UserID, agentID: record.ID, username: username,
		password: password, sessionID: "fork-" + suffix, homeDir: homeDir,
		port: port, relaySecret: relaySecret, cancel: cancel, lastUsed: time.Now(), expiresAt: expiresAt,
	}
	if err := s.launchChild(childCtx, fork); err != nil {
		cancel()
		_ = os.RemoveAll(homeDir)
		s.cleanupAccount(context.Background(), fork)
		return nil, fmt.Errorf("ephemeral fork failed to start: %w", err)
	}
	logger.InfoCF("aepchat", "ephemeral fork spawned", map[string]any{
		"requester": requester.DisplayName, "fork": username, "port": port,
		"home_team": homeTeam, "expires_at": expiresAt.Format(time.RFC3339),
	})
	return fork, nil
}

// residentModels lists the deployment models visible to the resident so the
// fork can answer with the same model catalog.
func (s *Supervisor) residentModels() []string {
	manager := aep.DefaultManager()
	if manager == nil {
		return nil
	}
	var ids []string
	for _, model := range manager.Models() {
		if model.Enabled {
			ids = append(ids, model.ID)
		}
	}
	return ids
}

func (s *Supervisor) launchChild(ctx context.Context, fork *fork) error {
	cfg := map[string]any{
		"version": 3,
		"aep": map[string]any{
			"enabled": true, "base_url": s.clientBaseURL(), "deployment_id": s.deploymentID,
			"username": fork.username, "session_id": fork.sessionID,
		},
		"channel_list": map[string]any{"aepchat": map[string]any{"enabled": true, "type": "aepchat"}},
		"model_list":   []any{},
		"knowledge": map[string]any{
			"enabled": s.knowledge.Enabled, "base_url": s.knowledge.BaseURL,
			"team_kb_map": s.knowledge.TeamKBMap, "max_passages": s.knowledge.MaxPassages,
		},
		"agents": map[string]any{"defaults": map[string]any{
			"workspace":             filepath.Join(fork.homeDir, "workspace"),
			"restrict_to_workspace": true, "max_tokens": 1024, "max_tool_iterations": 6,
		}},
		"gateway": map[string]any{"host": "127.0.0.1", "port": fork.port},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(fork.homeDir, "config.json"), data, 0o600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, s.binary, "gateway")
	cmd.Env = append(os.Environ(),
		"PICOCLAW_HOME="+fork.homeDir,
		"PICOCLAW_AEP_PASSWORD="+fork.password,
		"PICOCLAW_AEPCHAT_RELAY_TOKEN="+fork.relaySecret,
		"PICOCLAW_KNOWLEDGE_API_KEY="+s.knowledge.APIKey.String(),
	)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return s.waitHealthy(ctx, fork)
}

func (s *Supervisor) waitHealthy(ctx context.Context, fork *fork) error {
	timeout := s.healthTimeout
	if timeout <= 0 {
		timeout = childHealthTimeout
	}
	deadline := time.Now().Add(timeout)
	url := fork.URL() + "/aepchat/v1/health"
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		probe := &http.Client{Timeout: time.Second}
		resp, err := probe.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("fork on :%d did not become healthy", fork.port)
}

func (s *Supervisor) clientBaseURL() string { return s.baseURL }

// reaper terminates forks whose TTL passed or that have been idle too long,
// revoking their sessions and deleting their accounts.
func (s *Supervisor) reaper(ctx context.Context) {
	ticker := time.NewTicker(s.reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			var expired []*fork
			for id, fork := range s.forks {
				if time.Now().After(fork.expiresAt) {
					expired = append(expired, fork)
					delete(s.forks, id)
				}
			}
			s.mu.Unlock()
			for _, fork := range expired {
				s.cleanupAccount(ctx, fork)
			}
		}
	}
}

// cleanupAccount revokes the fork's session, stops its process, and deletes
// its account. Failures are logged, not fatal: the AEP-side hard expiry is
// the backstop.
func (s *Supervisor) cleanupAccount(ctx context.Context, fork *fork) {
	if fork.cancel != nil {
		fork.cancel()
	}
	token, err := s.supervisor.AccessToken(ctx)
	if err == nil {
		if p := s.client.RevokeSession(ctx, token, fork.sessionID); p != nil {
			logger.Warnf("aepchat: fork session revoke failed: %v", p)
		}
		if p := s.client.DeleteAgent(ctx, token, fork.agentID); p != nil {
			logger.Warnf("aepchat: fork account delete deferred: %v", p)
		}
	}
	_ = os.RemoveAll(fork.homeDir)
	logger.InfoCF("aepchat", "ephemeral fork reaped", map[string]any{"fork": fork.username})
}

func randomSuffix() string {
	return randomToken(5)
}

// randomToken returns n random bytes as hex (2n characters); a fixed
// fallback keeps startup deterministic only under a broken crypto source.
func randomToken(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		for i := range buf {
			buf[i] = 0
		}
	}
	return hex.EncodeToString(buf)
}

func sanitize(id string) string {
	out := make([]byte, 0, len(id))
	for _, c := range []byte(id) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c-'A'+'a')
		default:
			if len(out) > 0 && out[len(out)-1] != '-' {
				out = append(out, '-')
			}
		}
	}
	if len(out) == 0 || out[len(out)-1] == '-' {
		out = append(out, 'x')
	}
	if len(out) > 24 {
		out = out[:24]
	}
	return string(out)
}
