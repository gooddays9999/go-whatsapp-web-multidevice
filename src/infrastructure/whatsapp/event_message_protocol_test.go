package whatsapp

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestIsProtocolOnlyMessage(t *testing.T) {
	skdm := &waE2E.SenderKeyDistributionMessage{GroupID: proto.String("123@g.us")}
	tests := []struct {
		name string
		msg  *waE2E.Message
		want bool
	}{
		{name: "nil", msg: nil, want: false},
		{name: "empty", msg: &waE2E.Message{}, want: false},
		{
			name: "sender key distribution only",
			msg:  &waE2E.Message{SenderKeyDistributionMessage: skdm},
			want: true,
		},
		{
			name: "sender key distribution with context info",
			msg: &waE2E.Message{
				SenderKeyDistributionMessage: skdm,
				MessageContextInfo:           &waE2E.MessageContextInfo{},
			},
			want: true,
		},
		{
			name: "sender key distribution alongside real content",
			msg: &waE2E.Message{
				SenderKeyDistributionMessage: skdm,
				Conversation:                 proto.String("hi"),
			},
			want: false,
		},
		{
			name: "unhandled content type",
			msg:  &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isProtocolOnlyMessage(tt.msg); got != tt.want {
				t.Fatalf("isProtocolOnlyMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}
