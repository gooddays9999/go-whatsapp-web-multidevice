package whatsmeow

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestBuildNewsletterPollVoteNodeMatchesWebStanza(t *testing.T) {
	jid := types.NewJID("120363408885202478", types.NewsletterServer)
	node, err := buildNewsletterPollVoteNode(jid, 102, []string{"A"}, "3EB0TESTMSGID")
	if err != nil {
		t.Fatalf("buildNewsletterPollVoteNode returned error: %v", err)
	}
	if node.Tag != "message" {
		t.Fatalf("node tag = %q, want message", node.Tag)
	}
	if got := node.AttrGetter().String("type"); got != "poll" {
		t.Fatalf("message type = %q, want poll", got)
	}
	if got := node.AttrGetter().String("id"); got != "3EB0TESTMSGID" {
		t.Fatalf("message id = %q, want 3EB0TESTMSGID", got)
	}
	if got := node.AttrGetter().Int("server_id"); got != 102 {
		t.Fatalf("server_id = %d, want 102", got)
	}
	content := node.GetChildren()
	if len(content) != 2 {
		t.Fatalf("content length = %d, want 2", len(content))
	}
	if content[0].Tag != "meta" || content[0].AttrGetter().String("polltype") != "vote" {
		t.Fatalf("first child = %#v, want meta polltype=vote", content[0])
	}
	if content[1].Tag != "votes" {
		t.Fatalf("second child tag = %q, want votes", content[1].Tag)
	}
	votes := content[1].GetChildren()
	if len(votes) != 1 || votes[0].Tag != "vote" {
		t.Fatalf("votes = %#v, want one vote node", votes)
	}
	hash := sha256.Sum256([]byte("A"))
	voteHash, ok := votes[0].Content.([]byte)
	if !ok {
		t.Fatalf("vote content type = %T, want []byte", votes[0].Content)
	}
	if !bytes.Equal(voteHash, hash[:]) {
		t.Fatalf("vote hash = %x, want %x", voteHash, hash[:])
	}
}
