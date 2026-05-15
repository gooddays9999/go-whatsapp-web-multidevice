package bridge

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "required"), strings.Contains(msg, "invalid"), strings.Contains(msg, "must be"):
		return status.Error(codes.InvalidArgument, err.Error())
	case strings.Contains(msg, "not found"):
		return status.Error(codes.NotFound, err.Error())
	case strings.Contains(msg, "not connected"), strings.Contains(msg, "not connect"), strings.Contains(msg, "not logged in"), strings.Contains(msg, "wa cli"):
		return status.Error(codes.NotFound, err.Error())
	case strings.Contains(msg, "timeout"):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case strings.Contains(msg, "already"), strings.Contains(msg, "exists"):
		return status.Error(codes.AlreadyExists, err.Error())
	case strings.Contains(msg, "permission"):
		return status.Error(codes.PermissionDenied, err.Error())
	case strings.Contains(msg, "unavailable"), strings.Contains(msg, "no capacity"):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
