package bridge

import (
	"context"
	"testing"

	domainChat "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chat"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"google.golang.org/protobuf/proto"
)

type clearChatsBridgeStubUsecase struct {
	domainChat.IChatUsecase
	receivedRequest domainChat.ClearChatsRequest
	called          bool
}

func (s *clearChatsBridgeStubUsecase) ClearChats(_ context.Context, request domainChat.ClearChatsRequest) (domainChat.ClearChatsResponse, error) {
	s.called = true
	s.receivedRequest = request
	return domainChat.ClearChatsResponse{
		Status:  "success",
		Message: "Cleared 2/2 chats",
		Total:   2,
		Success: 2,
		Results: []domainChat.ClearChatResult{
			{ChatJID: "628111111111@s.whatsapp.net", Status: "success"},
			{ChatJID: "628222222222@s.whatsapp.net", Status: "success"},
		},
	}, nil
}

func TestClearChatsRPCForwardsAccountAndDeleteMedia(t *testing.T) {
	stub := &clearChatsBridgeStubUsecase{}
	var scopedAccountID string
	svc := &Service{
		deps: Dependencies{ChatUsecase: stub},
		accountContextForTest: func(ctx context.Context, accountID string) (context.Context, error) {
			scopedAccountID = accountID
			return ctx, nil
		},
	}

	resp, err := svc.ClearChats(context.Background(), &bridgepb.ClearChatsRequest{
		AccountId:   "acc-1",
		DeleteMedia: proto.Bool(false),
	})
	if err != nil {
		t.Fatalf("ClearChats RPC: %v", err)
	}

	if scopedAccountID != "acc-1" {
		t.Fatalf("scoped account id = %q, want acc-1", scopedAccountID)
	}
	if !stub.called {
		t.Fatal("chat usecase ClearChats was not called")
	}
	if stub.receivedRequest.DeleteMedia == nil || *stub.receivedRequest.DeleteMedia {
		t.Fatalf("DeleteMedia = %v, want explicit false", stub.receivedRequest.DeleteMedia)
	}
	if !resp.GetSuccess() || resp.GetTotal() != 2 || resp.GetSuccessCount() != 2 || resp.GetFailedCount() != 0 {
		t.Fatalf("response = %+v, want success counts 2/2/0", resp)
	}
	if len(resp.GetResults()) != 2 || resp.GetResults()[0].GetChatId() != "628111111111@s.whatsapp.net" {
		t.Fatalf("results = %+v, want mapped chat results", resp.GetResults())
	}
}
