package usecase

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waVnameCert"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

type countingContactStore struct {
	store.NoopStore
	contacts map[types.JID]types.ContactInfo
	reads    int
}

func (s *countingContactStore) GetContact(_ context.Context, jid types.JID) (types.ContactInfo, error) {
	s.reads++
	return s.contacts[jid], nil
}

func TestResolveParticipantDisplayNamePreservesSourceOrdering(t *testing.T) {
	participantJID := types.NewJID("628123456789", types.DefaultUserServer)
	verifiedName := "Verified Business"
	userInfo := map[types.JID]types.UserInfo{
		participantJID: {
			VerifiedName: &types.VerifiedName{
				Details: &waVnameCert.VerifiedNameCertificate_Details{
					VerifiedName: &verifiedName,
				},
			},
		},
	}

	tests := []struct {
		name      string
		seed      string
		contact   types.ContactInfo
		want      string
		wantReads int
	}{
		{
			name:      "seed wins before contact and verified name",
			seed:      "Participant Seed",
			contact:   types.ContactInfo{Found: true, FullName: "Saved Contact"},
			want:      "Participant Seed",
			wantReads: 0,
		},
		{
			name:      "contact wins before verified name",
			contact:   types.ContactInfo{Found: true, FullName: "Saved Contact"},
			want:      "Saved Contact",
			wantReads: 1,
		},
		{
			name:      "verified name follows an unavailable contact",
			want:      "Verified Business",
			wantReads: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contacts := &countingContactStore{
				contacts: map[types.JID]types.ContactInfo{
					participantJID: tt.contact,
				},
			}
			client := &whatsmeow.Client{
				Store: &store.Device{Contacts: contacts},
			}

			got := resolveParticipantDisplayName(
				context.Background(),
				client,
				tt.seed,
				participantJID,
				participantJID,
				userInfo,
			)
			if got != tt.want {
				t.Fatalf("resolveParticipantDisplayName() = %q, want %q", got, tt.want)
			}
			if contacts.reads != tt.wantReads {
				t.Fatalf("contact reads = %d, want %d", contacts.reads, tt.wantReads)
			}
		})
	}
}

func TestManageParticipantMapsEveryFailedResultAsError(t *testing.T) {
	tests := []struct {
		name        string
		participant types.GroupParticipant
		wantStatus  string
		wantCode    int
		wantID      string
	}{
		{
			name: "rate limited participant",
			participant: types.GroupParticipant{
				JID:   types.NewJID("15550000001", types.DefaultUserServer),
				Error: 429,
			},
			wantStatus: "error",
			wantCode:   429,
			wantID:     "15550000001@s.whatsapp.net",
		},
		{
			name: "approval request is not a completed add",
			participant: types.GroupParticipant{
				JID:        types.NewJID("15550000002", types.DefaultUserServer),
				Error:      403,
				AddRequest: &types.GroupParticipantAddRequest{Code: "invite-code"},
			},
			wantStatus: "error",
			wantCode:   403,
			wantID:     "15550000002@s.whatsapp.net",
		},
		{
			name: "completed participant operation",
			participant: types.GroupParticipant{
				JID: types.NewJID("15550000003", types.DefaultUserServer),
			},
			wantStatus: "success",
			wantCode:   0,
			wantID:     "15550000003@s.whatsapp.net",
		},
		{
			name: "lid result prefers provided phone number",
			participant: types.GroupParticipant{
				JID:         types.NewJID("987654321", types.HiddenUserServer),
				PhoneNumber: types.NewJID("15550000004", types.DefaultUserServer),
			},
			wantStatus: "success",
			wantCode:   0,
			wantID:     "15550000004@s.whatsapp.net",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := participantStatusFromResult(context.Background(), nil, tt.participant)
			if got.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got.Status, tt.wantStatus)
			}
			if got.ErrorCode != tt.wantCode {
				t.Fatalf("error code = %d, want %d", got.ErrorCode, tt.wantCode)
			}
			if got.Participant != tt.wantID {
				t.Fatalf("participant = %q, want %q", got.Participant, tt.wantID)
			}
		})
	}
}
