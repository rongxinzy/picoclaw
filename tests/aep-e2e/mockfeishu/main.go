// mockfeishu is a protocol-level Feishu (Lark) open-platform double for E2E
// tests: it serves the websocket bootstrap, REST send APIs, and a real
// pbbp2 websocket that pushes im.message.receive_v1 events built with the
// official SDK's own frame encoder. The picoclaw feishu channel cannot tell
// it from the real platform.
//
// Flags: -http :0 -ws :0 -app-id cli_mock -app-secret mock-secret
// Stdout: {"http":<port>,"ws":<port>} once both listeners are up.
//
// Test surface:
//
//	POST /_test/push  {"chatID","senderOpenID","text","chatType":"p2p|group","mentionBot":bool}
//	GET  /_test/sent  → recorded im/v1 message sends
//	POST /_test/reset → clear recorded sends
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

var (
	appID     = flag.String("app-id", "cli_mock", "expected app id")
	appSecret = flag.String("app-secret", "mock-secret", "expected app secret")
	httpAddr  = flag.String("http", "127.0.0.1:0", "HTTP listen address")
	wsAddr    = flag.String("ws", "127.0.0.1:0", "websocket listen address")
)

type sendRecord struct {
	ReceiveID   string `json:"receive_id"`
	ReceiveType string `json:"receive_id_type"`
	MsgType     string `json:"msg_type"`
	Content     string `json:"content"`
}

type pushRequest struct {
	ChatID       string `json:"chatID"`
	SenderOpenID string `json:"senderOpenID"`
	SenderUserID string `json:"senderUserID"`
	Text         string `json:"text"`
	ChatType     string `json:"chatType"` // p2p | group
	MentionBot   bool   `json:"mentionBot"`
}

type server struct {
	httpPort int
	wsPort   int

	mu       sync.Mutex
	conn     *websocket.Conn
	sent     []sendRecord
	eventSeq int
}

func main() {
	flag.Parse()
	s := &server{}

	wsLn, err := net.Listen("tcp", *wsAddr)
	if err != nil {
		panic(err)
	}
	s.wsPort = wsLn.Addr().(*net.TCPAddr).Port
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/ws", s.handleWS)
	go func() { _ = http.Serve(wsLn, wsMux) }()

	httpLn, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		panic(err)
	}
	s.httpPort = httpLn.Addr().(*net.TCPAddr).Port
	go func() { _ = http.Serve(httpLn, s) }()

	fmt.Printf("{\"http\":%d,\"ws\":%d}\n", s.httpPort, s.wsPort)
	select {}
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	// Drain client frames: ping controls and event acks.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// pushEvent delivers one im.message.receive_v1 event over the websocket as
// a pbbp2 data frame, mirroring the platform's server push.
func (s *server) pushEvent(req pushRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return fmt.Errorf("no websocket client connected")
	}
	s.eventSeq++
	eventID := fmt.Sprintf("evt_%d", s.eventSeq)
	messageID := fmt.Sprintf("om_%d", s.eventSeq)

	content := map[string]any{"text": req.Text}
	var mentions []any
	if req.MentionBot {
		content["text"] = "@_user_1 " + req.Text
		mentions = append(mentions, map[string]any{
			"key":            "@_user_1",
			"id":             map[string]any{"open_id": "ou_mockbot"},
			"mentioned_type": "bot",
			"name":           "MockBot",
		})
	}
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":    eventID,
			"event_type":  "im.message.receive_v1",
			"create_time": fmt.Sprintf("%d", nowMillis()),
			"token":       "",
			"app_id":      *appID,
			"tenant_key":  "t-mock",
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id":   map[string]any{"open_id": req.SenderOpenID, "user_id": req.SenderUserID},
				"sender_type": "user",
				"tenant_key":  "t-mock",
			},
			"message": map[string]any{
				"message_id":   messageID,
				"chat_id":      req.ChatID,
				"chat_type":    req.ChatType,
				"message_type": "text",
				"content":      mustJSON(content),
			},
		},
	}
	if mentions != nil {
		event["event"].(map[string]any)["message"].(map[string]any)["mentions"] = mentions
	}
	payload, _ := json.Marshal(event)
	frame := larkws.Frame{
		SeqID:  uint64(s.eventSeq),
		LogID:  uint64(s.eventSeq),
		Method: 1, // FrameTypeData
		Headers: []larkws.Header{
			{Key: "type", Value: "event"},
			{Key: "message_id", Value: eventID},
			{Key: "trace_id", Value: eventID},
			{Key: "sum", Value: "1"},
			{Key: "seq", Value: "0"},
		},
		PayloadEncoding: "json",
		PayloadType:     "json",
		Payload:         payload,
	}
	encoded, err := frame.Marshal()
	if err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.BinaryMessage, encoded)
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/callback/ws/endpoint" && r.Method == http.MethodPost:
		var body struct {
			AppID     string `json:"AppID"`
			AppSecret string `json:"AppSecret"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.AppID != *appID || body.AppSecret != *appSecret {
			writeJSON(w, http.StatusOK, map[string]any{"code": 514, "msg": "app id or secret mismatch"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0, "msg": "",
			"data": map[string]any{
				"URL": fmt.Sprintf("ws://127.0.0.1:%d/ws?device_id=d1&service_id=1", s.wsPort),
				"ClientConfig": map[string]int{
					"PingInterval":      15,
					"ReconnectCount":    3,
					"ReconnectInterval": 3,
				},
			},
		})
	case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
		var body struct {
			AppID     string `json:"app_id"`
			AppSecret string `json:"app_secret"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.AppID != *appID || body.AppSecret != *appSecret {
			writeJSON(w, http.StatusOK, map[string]any{"code": 10014, "msg": "app secret mismatch"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": 0, "tenant_access_token": "t-mock", "expire": 7200})
	case r.URL.Path == "/open-apis/bot/v3/info":
		writeJSON(w, http.StatusOK, map[string]any{"code": 0, "bot": map[string]any{"open_id": "ou_mockbot", "app_name": "MockBot"}})
	case r.URL.Path == "/_test/push" && r.Method == http.MethodPost:
		var req pushRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if req.ChatType == "" {
			req.ChatType = "p2p"
		}
		if err := s.pushEvent(req); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"pushed": true})
	case r.URL.Path == "/_test/sent" && r.Method == http.MethodGet:
		s.mu.Lock()
		sent := append([]sendRecord(nil), s.sent...)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"sent": sent})
	case r.URL.Path == "/_test/reset" && r.Method == http.MethodPost:
		s.mu.Lock()
		s.sent = nil
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"reset": true})
	case r.Method == http.MethodPost && hasPrefix(r.URL.Path, "/open-apis/im/v1/messages"):
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.sent = append(s.sent, sendRecord{
			ReceiveID:   str(body["receive_id"]),
			ReceiveType: r.URL.Query().Get("receive_id_type"),
			MsgType:     str(body["msg_type"]),
			Content:     str(body["content"]),
		})
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0, "msg": "ok",
			"data": map[string]any{"message_id": fmt.Sprintf("om_out_%d", len(s.sent))},
		})
	default:
		// Catch-all for card patches, reactions, resource fetches: success.
		writeJSON(w, http.StatusOK, map[string]any{"code": 0, "msg": "ok", "data": map[string]any{}})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func mustJSON(v any) string {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
