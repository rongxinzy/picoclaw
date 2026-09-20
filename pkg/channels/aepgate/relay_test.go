package aepgate

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestRelayTurnJSONTags pins the wire names of RelayTurn: the fork-side
// handler decodes turnId by name, so a tag typo would silently disable
// idempotency rather than fail loudly.
func TestRelayTurnJSONTags(t *testing.T) {
	raw, err := json.Marshal(RelayTurn{
		ChatID:               "feishu:oc_1",
		ChatType:             "direct",
		RequesterUserID:      "user-sub",
		RequesterDisplayName: "Subordinate",
		Text:                 "dept report",
		SourceChannel:        "feishu",
		TurnID:               "turn-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"chatID":"feishu:oc_1"`,
		`"requesterUserID":"user-sub"`,
		`"requesterDisplayName":"Subordinate"`,
		`"text":"dept report"`,
		`"sourceChannel":"feishu"`,
		`"turnId":"turn-1"`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("encoded turn %s missing %s", raw, want)
		}
	}

	// An unset TurnID is omitted, matching the optional contract field.
	raw, err = json.Marshal(RelayTurn{ChatID: "c", Text: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "turnId") {
		t.Fatalf("empty TurnID should be omitted: %s", raw)
	}
}
