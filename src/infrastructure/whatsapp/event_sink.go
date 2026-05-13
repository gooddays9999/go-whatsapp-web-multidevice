package whatsapp

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type EventSink interface {
	HandleWhatsAppEvent(ctx context.Context, instance *DeviceInstance, rawEvt any)
}

var eventSinks sync.Map

func RegisterEventSink(name string, sink EventSink) {
	if name == "" || sink == nil {
		return
	}
	eventSinks.Store(name, sink)
}

func UnregisterEventSink(name string) {
	if name == "" {
		return
	}
	eventSinks.Delete(name)
}

func notifyEventSinks(ctx context.Context, instance *DeviceInstance, rawEvt any) {
	eventSinks.Range(func(_, value any) bool {
		sink, ok := value.(EventSink)
		if !ok || sink == nil {
			return true
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logrus.Warnf("event sink panic: %v", r)
				}
			}()
			sink.HandleWhatsAppEvent(ctx, instance, rawEvt)
		}()
		return true
	})
}
