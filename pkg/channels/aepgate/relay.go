// relay.go is the resident-side client for the fork relay endpoint: it
// submits one subordinate's turn to an ephemeral fork and awaits the final
// reply, which the resident sends through its own platform connection.
package aepgate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RelayTurn is one message routed from an IM channel through the resident
// gateway into the requester's ephemeral fork.
type RelayTurn struct {
	ChatID               string `json:"chatID"`
	ChatType             string `json:"chatType"`
	RequesterUserID      string `json:"requesterUserID"`
	RequesterDisplayName string `json:"requesterDisplayName"`
	Text                 string `json:"text"`
	SourceChannel        string `json:"sourceChannel"`
	// TurnID is the caller-chosen idempotency key: the fork replays the
	// remembered outcome for a repeated key instead of re-executing.
	TurnID string `json:"turnId,omitempty"`
}

// RelayClient posts turns to fork relay endpoints. Safe for concurrent use.
type RelayClient struct {
	http *http.Client
}

// NewRelayClient builds a client with a bounded request timeout; the fork's
// own turn timeout (120s) is the effective ceiling, this one is the guard.
func NewRelayClient() *RelayClient {
	return &RelayClient{http: &http.Client{Timeout: 150 * time.Second}}
}

// Call submits one turn and returns the fork's final reply. An empty reply
// is valid (the turn produced no user-visible output).
func (c *RelayClient) Call(ctx context.Context, f *fork, turn RelayTurn) (string, error) {
	baseURL, secret := f.Relay()
	payload, err := json.Marshal(turn)
	if err != nil {
		return "", fmt.Errorf("relay turn encoding: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/aepchat/v1/relay/turns", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("relay turn build: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("relay turn request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("relay turn read: %w", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("relay turn rejected: %d %s", resp.StatusCode, string(body))
	}
	var out struct {
		Reply string `json:"reply"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("relay turn decode: %w", err)
	}
	return out.Reply, nil
}
