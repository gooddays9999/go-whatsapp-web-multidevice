package bridge

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type NATSPublisher struct {
	conn      *nats.Conn
	published atomic.Uint64
}

func NewNATSPublisher(url string) *NATSPublisher {
	if url == "" {
		return &NATSPublisher{}
	}
	conn, err := nats.Connect(
		url,
		nats.Name("go-whatsapp-web-ims-bridge"),
		nats.Timeout(2*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logrus.WithError(err).Warn("bridge NATS disconnected")
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			logrus.Infof("bridge NATS reconnected to %s", conn.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			logrus.Warn("bridge NATS connection closed")
		}),
	)
	if err != nil {
		logrus.WithError(err).Warn("bridge NATS disabled")
		return &NATSPublisher{}
	}
	return &NATSPublisher{conn: conn}
}

func (p *NATSPublisher) Close() {
	if p != nil && p.conn != nil {
		p.conn.Close()
	}
}

func (p *NATSPublisher) IsConnected() bool {
	return p != nil && p.conn != nil && p.conn.IsConnected()
}

func (p *NATSPublisher) Count() uint64 {
	if p == nil {
		return 0
	}
	return p.published.Load()
}

func (p *NATSPublisher) Publish(ctx context.Context, eventType string, payload map[string]any) {
	if p == nil || p.conn == nil || eventType == "" {
		return
	}
	if _, ok := payload["type"]; !ok {
		payload["type"] = eventType
	}
	if _, ok := payload["timestamp"]; !ok {
		payload["timestamp"] = time.Now().UnixMilli()
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logrus.WithError(err).Warnf("failed to marshal bridge event %s", eventType)
		return
	}
	done := make(chan error, 1)
	go func() {
		done <- p.conn.Publish(eventType, data)
	}()
	select {
	case <-ctx.Done():
		return
	case err := <-done:
		if err != nil {
			logrus.WithError(err).Warnf("failed to publish bridge event %s", eventType)
			return
		}
		p.published.Add(1)
	}
}
