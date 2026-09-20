package aep

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListIdentityMappingsPagesByCursor(t *testing.T) {
	var seenQuery, seenAuth string
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		seenAuth = r.Header.Get("Authorization")
		seenQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/aep/v1/admin/identity-sources/feishu-src/mappings" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"mappings":[{"sourceId":"feishu-src","externalSubjectType":"user","externalId":"ou_alice","localSubjectId":"user-alice","status":"active"},{"sourceId":"feishu-src","externalSubjectType":"user","externalId":"ou_bob","localSubjectId":"user-bob","status":"disabled"}],"nextCursor":"ou_bob"}`))
			return
		}
		_, _ = w.Write([]byte(`{"mappings":[{"sourceId":"feishu-src","externalSubjectType":"user","externalId":"ou_carol","localSubjectId":"user-carol","status":"active"}],"nextCursor":null}`))
	}))
	defer server.Close()
	client := NewClient(server.URL)

	first, next, p := client.ListIdentityMappings(context.Background(), "at", "feishu-src", "user", "", 200)
	if p != nil || len(first) != 2 || next != "ou_bob" {
		t.Fatalf("first page = %+v next=%q p=%v", first, next, p)
	}
	if first[0].LocalSubjectID != "user-alice" || first[1].Status != "disabled" {
		t.Fatalf("mapping decode = %+v", first)
	}
	if !strings.Contains(seenQuery, "subjectType=user") || !strings.Contains(seenQuery, "limit=200") || strings.Contains(seenQuery, "cursor=") {
		t.Fatalf("first query = %q", seenQuery)
	}
	if seenAuth != "Bearer at" {
		t.Fatalf("authorization = %q", seenAuth)
	}

	second, next, p := client.ListIdentityMappings(context.Background(), "at", "feishu-src", "user", next, 200)
	if p != nil || len(second) != 1 || next != "" {
		t.Fatalf("second page = %+v next=%q p=%v", second, next, p)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestListIdentityMappingsValidationAndErrors(t *testing.T) {
	client := NewClient("http://unused.local")

	if _, _, p := client.ListIdentityMappings(context.Background(), "at", "", "user", "", 0); p == nil || p.Status != http.StatusBadRequest {
		t.Fatalf("missing source = %v", p)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"type":"about:blank","title":"Forbidden","status":403,"code":"ACCESS_DENIED"}`)
	}))
	defer server.Close()
	forbidden := NewClient(server.URL)
	if _, _, p := forbidden.ListIdentityMappings(context.Background(), "at", "src", "", "", 0); p == nil || p.Code != "ACCESS_DENIED" {
		t.Fatalf("forbidden = %v", p)
	}
}
