package weknora

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchPostsScopedQueryAndParsesPassages(t *testing.T) {
	var gotPath, gotMethod, gotKey string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotKey = r.URL.Path, r.Method, r.Header.Get("X-API-Key")
		body, _ := io.ReadAll(r.Body)
		_ = jsonUnmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"success":true,"data":[
			{"knowledge_id":"k1","knowledge_title":"Handbook","content":"passage one","score":0.9},
			{"knowledge_id":"k2","knowledge_title":"Guide","content":"passage two","score":0.4}]}`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "kn-secret")
	passages, err := client.Search(context.Background(), "release", []string{"kb-a", "kb-b"}, 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if gotPath != "/api/v1/knowledge-search" || gotMethod != http.MethodPost || gotKey != "kn-secret" {
		t.Fatalf("request = %s %s key=%q", gotMethod, gotPath, gotKey)
	}
	if gotBody["query"] != "release" {
		t.Fatalf("query = %v", gotBody["query"])
	}
	kbs, _ := gotBody["knowledge_base_ids"].([]any)
	if len(kbs) != 2 || gotBody["top_k"] != float64(2) {
		t.Fatalf("kb scoping = %v top_k=%v", gotBody["knowledge_base_ids"], gotBody["top_k"])
	}
	if len(passages) != 2 || passages[0].Title != "Handbook" || passages[1].Score != 0.4 {
		t.Fatalf("passages = %+v", passages)
	}
}

func TestSearchDefaultsLimitAndRejectsErrors(t *testing.T) {
	// success=false surfaces the server message.
	rejected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"success":false,"message":"scope denied"}`)
	}))
	defer rejected.Close()
	if _, err := NewClient(rejected.URL, "k").Search(context.Background(), "q", []string{"kb"}, 0); err == nil || !strings.Contains(err.Error(), "scope denied") {
		t.Fatalf("expected rejection, got %v", err)
	}

	// Non-200 surfaces the status and body.
	badStatus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"bad key"}`)
	}))
	defer badStatus.Close()
	if _, err := NewClient(badStatus.URL, "k").Search(context.Background(), "q", []string{"kb"}, 3); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected status error, got %v", err)
	}

	// Malformed JSON surfaces a decoding failure.
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `not-json`)
	}))
	defer broken.Close()
	if _, err := NewClient(broken.URL, "k").Search(context.Background(), "q", []string{"kb"}, 3); err == nil || !strings.Contains(err.Error(), "decoding") {
		t.Fatalf("expected decoding error, got %v", err)
	}
}

func TestSearchTransportFailureAndDefaultLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"success":true,"data":[]}`)
	}))
	url := server.URL
	server.Close() // force transport failure against a dead endpoint

	if _, err := NewClient(url, "k").Search(context.Background(), "q", []string{"kb"}, 3); err == nil {
		t.Fatal("expected transport error")
	}

	// limit <= 0 defaults to 5 and still produces a well-formed request.
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = jsonUnmarshal(body, &parsed)
		if parsed["top_k"] != float64(5) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"success":true,"data":[]}`)
	}))
	defer live.Close()
	if _, err := NewClient(live.URL, "k").Search(context.Background(), "q", []string{"kb"}, 0); err != nil {
		t.Fatalf("default limit: %v", err)
	}
}

func jsonUnmarshal(data []byte, out any) error {
	return json.Unmarshal(data, out)
}
