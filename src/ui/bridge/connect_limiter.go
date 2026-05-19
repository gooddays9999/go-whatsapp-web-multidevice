package bridge

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
)

var errConnectQueueFull = errors.New("connect queue full")

func (s *Service) acquireConnectSlot(ctx context.Context, accountID, operation string) (func(), error) {
	if s == nil || s.reconnectSlots == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queueTimeout := s.cfg.ReconnectQueueTimeout
	if queueTimeout <= 0 {
		queueTimeout = 3 * time.Second
	}
	queueCtx, cancel := context.WithTimeout(ctx, queueTimeout)
	select {
	case s.reconnectSlots <- struct{}{}:
		return func() {
			<-s.reconnectSlots
			cancel()
		}, nil
	case <-queueCtx.Done():
		err := queueCtx.Err()
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w after %s", errConnectQueueFull, queueTimeout)
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"account_id": accountID,
			"operation":  operation,
			"timeout":    queueTimeout.String(),
		}).Warn("WhatsApp connect skipped: connect queue full")
		return nil, err
	}
}

func (s *Service) connectWithSlot(ctx context.Context, inst *whatsapp.DeviceInstance, accountID, operation string, timeout time.Duration) error {
	if inst == nil {
		return fmt.Errorf("account client is nil")
	}
	release, err := s.acquireConnectSlot(ctx, accountID, operation)
	if err != nil {
		return err
	}
	defer release()
	return inst.ConnectWithTimeout(ctx, timeout, operation)
}
