package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Client calls another digital employee's A2A endpoint.
type Client struct {
	http *http.Client
}

// NewClient returns a peer caller.
func NewClient() *Client {
	return &Client{http: &http.Client{Timeout: 30 * time.Second}}
}

// InvokeOptions carries the mandatory delegation context.
type InvokeOptions struct {
	BaseURL string
	Token   string // the CALLER's AEP access token
	Caller  string // calling agent identity (e.g. username)
	ActAs   string // the original requester the task acts on
	Chain   []string
}

// Invoke sends one message and returns the created task.
func (c *Client) Invoke(ctx context.Context, opts InvokeOptions, text string) (*Task, error) {
	chain := make([]string, 0, len(opts.Chain)+1)
	chain = append(chain, opts.Chain...)
	if opts.Caller != "" {
		chain = append(chain, opts.Caller)
	}
	request := map[string]any{
		"jsonrpc": "2.0",
		"id":      uuid.NewString(),
		"method":  "message/send",
		"params": map[string]any{
			"message": Message{
				Role:      "user",
				Parts:     []Part{{Kind: "text", Text: text}},
				MessageID: "msg-" + uuid.NewString(),
				Metadata: map[string]string{
					MetadataActAs:  opts.ActAs,
					MetadataCaller: opts.Caller,
					MetadataChain:  strings.Join(chain, ">"),
				},
			},
		},
	}
	var task Task
	if err := c.rpc(ctx, opts.BaseURL, opts.Token, request, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// GetTask fetches one task snapshot.
func (c *Client) GetTask(ctx context.Context, baseURL, token, taskID string) (*Task, error) {
	request := map[string]any{
		"jsonrpc": "2.0",
		"id":      uuid.NewString(),
		"method":  "tasks/get",
		"params":  map[string]any{"taskId": taskID},
	}
	var task Task
	if err := c.rpc(ctx, baseURL, token, request, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// InvokeAndWait sends one message and polls until the task is terminal.
func (c *Client) InvokeAndWait(ctx context.Context, opts InvokeOptions, text string, timeout time.Duration) (*Task, error) {
	task, err := c.Invoke(ctx, opts, text)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := c.GetTask(ctx, opts.BaseURL, opts.Token, task.ID)
		if err != nil {
			return nil, err
		}
		if snapshot.State == TaskStateCompleted || snapshot.State == TaskStateFailed {
			return snapshot, nil
		}
		select {
		case <-ctx.Done():
			return snapshot, ctx.Err()
		case <-time.After(1500 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("a2a task %s did not finish within %s", task.ID, timeout)
}

func (c *Client) rpc(ctx context.Context, baseURL, token string, request any, out any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/a2a/", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("a2a: request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("a2a: endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("a2a: response decoding failed: %w", err)
	}
	if envelope.Error != nil {
		return fmt.Errorf("a2a: peer rejected the call (%d): %s", envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return fmt.Errorf("a2a: response carried neither result nor error")
	}
	return json.Unmarshal(envelope.Result, out)
}
