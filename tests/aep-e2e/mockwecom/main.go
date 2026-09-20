// mockwecom is a protocol-level WeCom 智能机器人 websocket double for E2E
// tests: it accepts aibot_subscribe, answers pings, pushes
// aibot_msg_callback envelopes, and records aibot_respond_msg /
// aibot_send_msg replies.
//
// Flags: -addr 127.0.0.1:0 -bot-id mockbot -secret mock-secret
// Stdout: {"ws":<port>} once the listener is up.
//
// Test surface:
//
//	POST /_test/push  {"chatID","userID","text","chatType":"single|group"}
//	GET  /_test/sent  → recorded respond/send commands
//	POST /_test/reset → clear recorded commands
package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	addr   = flag.String("addr", "127.0.0.1:0", "websocket listen address")
	botID  = flag.String("bot-id", "mockbot", "expected bot id")
	secret = flag.String("secret", "mock-secret", "expected bot secret")
)

type commandRecord struct {
	Cmd  string          `json:"cmd"`
	Body json.RawMessage `json:"body,omitempty"`
}

type pushRequest struct {
	ChatID   string `json:"chatID"`
	UserID   string `json:"userID"`
	Text     string `json:"text"`
	ChatType string `json:"chatType"` // single | group
}

type server struct {
	mu    sync.Mutex
	conn  *websocket.Conn
	sent  []commandRecord
	ready bool
}

func main() {
	flag.Parse()
	s := &server{}

	wsLn, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}
	port := wsLn.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/_test/push", s.handlePush)
	mux.HandleFunc("/_test/sent", s.handleSent)
	mux.HandleFunc("/_test/reset", s.handleReset)
	go func() { _ = http.Serve(wsLn, mux) }()

	fmt.Printf("{\"ws\":%d}\n", port)
	select {}
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	go s.readLoop(conn)
}

func (s *server) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var cmd struct {
			Cmd     string `json:"cmd"`
			Headers struct {
				ReqID string `json:"req_id"`
			} `json:"headers"`
			Body json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(data, &cmd); err != nil {
			continue
		}
		switch cmd.Cmd {
		case "aibot_subscribe":
			var body struct {
				BotID  string `json:"bot_id"`
				Secret string `json:"secret"`
			}
			_ = json.Unmarshal(cmd.Body, &body)
			errcode := 0
			errmsg := "ok"
			if body.BotID != *botID || body.Secret != *secret {
				errcode = 40001
				errmsg = "bot id or secret mismatch"
			} else {
				s.mu.Lock()
				s.ready = true
				s.mu.Unlock()
			}
			s.writeAck(cmd.Headers.ReqID, errcode, errmsg)
		case "ping":
			s.writeAck(cmd.Headers.ReqID, 0, "ok")
		default:
			// respond/send/upload commands: record and ack.
			s.mu.Lock()
			s.sent = append(s.sent, commandRecord{Cmd: cmd.Cmd, Body: cmd.Body})
			s.mu.Unlock()
			s.writeAck(cmd.Headers.ReqID, 0, "ok")
		}
	}
}

func (s *server) writeAck(reqID string, errcode int, errmsg string) {
	ack := map[string]any{
		"headers": map[string]string{"req_id": reqID},
		"errcode": errcode,
		"errmsg":  errmsg,
	}
	data, _ := json.Marshal(ack)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.WriteMessage(websocket.TextMessage, data)
	}
}

func (s *server) handlePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req pushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ChatType == "" {
		req.ChatType = "single"
	}
	chatType := req.ChatType
	if chatType != "group" {
		chatType = "single"
	}
	envelope := map[string]any{
		"cmd":     "aibot_msg_callback",
		"headers": map[string]string{"req_id": randomID()},
		"body": map[string]any{
			"msgid":    "msg_" + randomID(),
			"aibotid":  *botID,
			"chatid":   req.ChatID,
			"chattype": chatType,
			"from":     map[string]string{"userid": req.UserID},
			"msgtype":  "text",
			"text":     map[string]string{"content": req.Text},
		},
	}
	data, _ := json.Marshal(envelope)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || !s.ready {
		http.Error(w, "no subscribed client", http.StatusServiceUnavailable)
		return
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"pushed":true}`))
}

func (s *server) handleSent(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"sent": s.sent})
}

func (s *server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.sent = nil
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"reset":true}`))
}

func randomID() string {
	buf := make([]byte, 6)
	if _, err := crand.Read(buf); err != nil {
		for i := range buf {
			buf[i] = 0
		}
	}
	return hex.EncodeToString(buf)
}
