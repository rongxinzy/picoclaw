package providers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

// newFakeAEP serves the control-plane endpoints the session manager touches.
func newFakeAEP(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login":
			_, _ = io.WriteString(w, `{"accessToken":"at-1","refreshToken":"rt-1","modelAccessToken":"mt-1","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"picoclaw-test"}`)
		case "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at-2","refreshToken":"rt-2","modelAccessToken":"mt-2","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"picoclaw-test"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep-control-service","deploymentId":"demo","modelGateway":{"baseUrl":"https://gw.example.test/v1","protocol":"openai-compatible"}}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[`+
				`{"id":"qwen","displayName":"Qwen","upstreamModel":"qwen3-32b","enabled":true,"isDefault":true},`+
				`{"id":"glm","displayName":"GLM","upstreamModel":"glm-5","enabled":true},`+
				`{"id":"retired","displayName":"Retired","upstreamModel":"x","enabled":false}]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-18T08:00:00Z","controlEvents":{"pending":false,"watermark":"0"},"nextHeartbeatAfterSeconds":60}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func aepTestConfig(baseURL string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.ModelName = ""
	cfg.AEP = config.AEPConfig{
		Enabled:      true,
		BaseURL:      baseURL,
		DeploymentID: "demo",
		Username:     "helper-1",
		Password:     *config.NewSecureString("agent-password-123"),
		SessionID:    "picoclaw-test",
	}
	return cfg
}

func TestStartAEPSessionInjectsModelsAndDefault(t *testing.T) {
	fake := newFakeAEP(t)
	cfg := aepTestConfig(fake.URL)

	manager, err := StartAEPSession(cfg)
	if err != nil {
		t.Fatalf("start AEP session: %v", err)
	}
	t.Cleanup(func() {
		manager.Stop()
		SetAEPTokenSource(nil)
	})

	byName := map[string]*config.ModelConfig{}
	for i := range cfg.ModelList {
		byName[cfg.ModelList[i].ModelName] = cfg.ModelList[i]
	}
	qwen, ok := byName["qwen"]
	if !ok || qwen.Provider != "aep" || qwen.Model != "qwen3-32b" || qwen.APIBase != "https://gw.example.test/v1" || !qwen.Enabled {
		t.Fatalf("injected qwen entry = %+v", qwen)
	}
	if _, hasRetired := byName["retired"]; hasRetired {
		t.Fatal("disabled AEP model must not be injected")
	}
	if cfg.Agents.Defaults.GetModelName() != "qwen" {
		t.Fatalf("default model = %q, want the deployment-flagged qwen", cfg.Agents.Defaults.GetModelName())
	}

	// The provider family must now be constructible through the factory.
	factoryProvider, _, err := CreateProviderFromConfig(qwen)
	if err != nil {
		t.Fatalf("create aep provider from injected entry: %v", err)
	}
	if factoryProvider == nil {
		t.Fatal("nil provider")
	}
}

func TestStartAEPSessionReplacesStaleEntriesAndKeepsExplicitDefault(t *testing.T) {
	fake := newFakeAEP(t)
	cfg := aepTestConfig(fake.URL)
	// An operator entry with the same model_name is replaced by AEP truth;
	// an unrelated entry and an explicit default model survive untouched.
	cfg.ModelList = append(cfg.ModelList, &config.ModelConfig{
		ModelName: "qwen", Provider: "openai", Model: "gpt-5.4", Enabled: true,
	})
	cfg.Agents.Defaults.ModelName = "glm"

	manager, err := StartAEPSession(cfg)
	if err != nil {
		t.Fatalf("start AEP session: %v", err)
	}
	t.Cleanup(func() {
		manager.Stop()
		SetAEPTokenSource(nil)
	})

	for i := range cfg.ModelList {
		if cfg.ModelList[i].ModelName == "qwen" && cfg.ModelList[i].Provider != "aep" {
			t.Fatalf("stale qwen entry survived: %+v", cfg.ModelList[i])
		}
	}
	if cfg.Agents.Defaults.GetModelName() != "glm" {
		t.Fatalf("explicit default model was overridden: %q", cfg.Agents.Defaults.GetModelName())
	}
}

func TestStartAEPSessionRejectsIncompleteConfig(t *testing.T) {
	cfg := aepTestConfig("http://unused.example")
	cfg.AEP.Password = config.SecureString{}
	_, err := StartAEPSession(cfg)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected completeness error, got %v", err)
	}
}

func TestAEPSerializedConfigNeverContainsPlaintextPassword(t *testing.T) {
	cfg := aepTestConfig("http://unused.example")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "agent-password-123") {
		t.Fatalf("password leaked into serialized config: %s", data)
	}
	if !strings.Contains(string(data), `"aep"`) {
		t.Fatalf("aep section missing from serialized config: %s", data)
	}
}
