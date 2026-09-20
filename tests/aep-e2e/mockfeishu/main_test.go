package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// TestRealSDKHandshakeAndEventDelivery proves the mock speaks the actual
// lark SDK websocket protocol: the SDK bootstraps, connects, and receives
// an im.message.receive_v1 event pushed through the mock's pbbp2 frames.
func TestRealSDKHandshakeAndEventDelivery(t *testing.T) {
	s := &server{}
	wsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.wsPort = wsLn.Addr().(*net.TCPAddr).Port
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/ws", s.handleWS)
	go func() { _ = http.Serve(wsLn, wsMux) }()
	defer wsLn.Close()

	ts := httptest.NewServer(s)
	defer ts.Close()

	received := make(chan *larkim.P2MessageReceiveV1, 1)
	dispatcher := larkdispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, event *larkim.P2MessageReceiveV1) error {
			received <- event
			return nil
		})
	client := larkws.NewClient(*appID, *appSecret,
		larkws.WithEventHandler(dispatcher),
		larkws.WithDomain(ts.URL),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- client.Start(ctx) }()

	// Push one event and wait for the SDK dispatcher to receive it.
	deadline := time.Now().Add(5 * time.Second)
	pushed := false
	for time.Now().Before(deadline) {
		body, _ := json.Marshal(pushRequest{
			ChatID: "oc_test_1", SenderOpenID: "ou_alice", Text: "hello mock", ChatType: "p2p",
		})
		resp, err := http.Post(ts.URL+"/_test/push", "application/json", bytes.NewReader(body))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				pushed = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !pushed {
		t.Fatal("mock never accepted a push (ws client not connected)")
	}

	select {
	case err := <-startErr:
		t.Fatalf("sdk ws start returned: %v", err)
	case event := <-received:
		if event == nil || event.Event == nil || event.Event.Message == nil {
			t.Fatalf("event = %+v", event)
		}
		if got := *event.Event.Message.ChatId; got != "oc_test_1" {
			t.Fatalf("chat id = %q", got)
		}
		if got := *event.Event.Sender.SenderId.OpenId; got != "ou_alice" {
			t.Fatalf("sender = %q", got)
		}
		var content struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(*event.Event.Message.Content), &content); err != nil {
			t.Fatalf("content parse: %v", err)
		}
		if content.Text != "hello mock" {
			t.Fatalf("text = %q", content.Text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sdk dispatcher never received the pushed event")
	}

	// REST send path records into the mock.
	sendBody := `{"receive_id":"oc_test_1","msg_type":"text","content":"{\"text\":\"reply\"}"}`
	resp, err := http.Post(ts.URL+"/open-apis/im/v1/messages?receive_id_type=chat_id", "application/json", bytes.NewReader([]byte(sendBody)))
	if err != nil {
		t.Fatalf("rest send: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(raw, []byte(`"code":0`)) {
		t.Fatalf("rest send response = %s", raw)
	}
	out, err := http.Get(ts.URL + "/_test/sent")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	var listing struct {
		Sent []sendRecord `json:"sent"`
	}
	_ = json.NewDecoder(out.Body).Decode(&listing)
	if len(listing.Sent) != 1 || listing.Sent[0].ReceiveID != "oc_test_1" ||
		listing.Sent[0].MsgType != "text" || !bytes.Contains([]byte(listing.Sent[0].Content), []byte("reply")) {
		t.Fatalf("recorded sends = %+v", listing.Sent)
	}
}
