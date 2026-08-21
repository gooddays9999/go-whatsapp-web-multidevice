package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domainChat "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chat"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/ui/rest/middleware"
	"github.com/gofiber/fiber/v2"
)

type clearChatsStubUsecase struct {
	domainChat.IChatUsecase
	receivedRequest domainChat.ClearChatsRequest
	called          bool
}

func (s *clearChatsStubUsecase) ClearChats(_ context.Context, request domainChat.ClearChatsRequest) (domainChat.ClearChatsResponse, error) {
	s.called = true
	s.receivedRequest = request
	return domainChat.ClearChatsResponse{
		Status:  "success",
		Message: "Cleared 1 chats",
		Total:   1,
		Success: 1,
	}, nil
}

func TestClearChatsRouteForwardsDeleteMedia(t *testing.T) {
	stub := &clearChatsStubUsecase{}
	app := fiber.New()
	app.Use(middleware.Recovery())
	controller := Chat{Service: stub}
	app.Post("/chats/clear", controller.ClearChats)

	req := httptest.NewRequest(http.MethodPost, "/chats/clear", strings.NewReader(`{"delete_media":false}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusOK)
	}
	if !stub.called {
		t.Fatal("ClearChats usecase was not called")
	}
	if stub.receivedRequest.DeleteMedia == nil {
		t.Fatal("DeleteMedia = nil, want explicit false from request body")
	}
	if *stub.receivedRequest.DeleteMedia {
		t.Fatal("DeleteMedia = true, want false from request body")
	}
}
