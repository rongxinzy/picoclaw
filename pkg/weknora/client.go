// Package weknora is a minimal client for a WeKnora knowledge platform
// deployment: the hybrid retrieval endpoint used by digital employees.
package weknora

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Passage is one retrieved chunk with its source attribution.
type Passage struct {
	KnowledgeID string  `json:"knowledge_id"`
	Title       string  `json:"knowledge_title"`
	Content     string  `json:"content"`
	Score       float64 `json:"score"`
}

// Client talks to one WeKnora app deployment with a scoped API key. The key
// itself is KB-restricted server-side, so even a bypassed caller cannot read
// knowledge bases outside its grant.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// NewClient binds a client to a WeKnora base URL (e.g. http://localhost:8092).
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Search runs hybrid retrieval restricted to the given knowledge bases.
func (c *Client) Search(ctx context.Context, query string, kbIDs []string, limit int) ([]Passage, error) {
	if limit <= 0 {
		limit = 5
	}
	body, err := json.Marshal(map[string]any{
		"query":              query,
		"knowledge_base_ids": kbIDs,
		"top_k":              limit,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/knowledge-search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("weknora: request failed: %w", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("weknora: search returned %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var out struct {
		Success bool      `json:"success"`
		Data    []Passage `json:"data"`
		Message string    `json:"message"`
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("weknora: response decoding failed: %w", err)
	}
	if !out.Success {
		return nil, fmt.Errorf("weknora: search rejected: %s", out.Message)
	}
	return out.Data, nil
}
