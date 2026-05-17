package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	domainMessage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/message"
	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	domainUser "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/user"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Dependencies struct {
	DB              *sql.DB
	DeviceManager   *whatsapp.DeviceManager
	ChatStorageRepo domainChatStorage.IChatStorageRepository
	SendUsecase     domainSend.ISendUsecase
	UserUsecase     domainUser.IUserUsecase
	MessageUsecase  domainMessage.IMessageUsecase
	GroupUsecase    domainGroup.IGroupUsecase
}

type Service struct {
	bridgepb.UnimplementedWhatsAppBridgeServer

	cfg               Config
	deps              Dependencies
	envStore          *EnvironmentStore
	uaPool            *UAPool
	publisher         *NATSPublisher
	accountProxyStore *AccountProxyStore
	grpcServer        *grpc.Server
	httpServer        *http.Server
	startedAt         time.Time
	workerID          string

	mu              sync.RWMutex
	connected       map[string]time.Time
	statuses        map[string]string
	reconnecting    map[string]time.Time
	statusSendSlots chan struct{}
	statusSendMu    sync.Mutex
	lastStatusSend  time.Time
}

func NewService(cfg Config, deps Dependencies) (*Service, error) {
	if deps.DB == nil {
		return nil, fmt.Errorf("database is required")
	}
	if deps.DeviceManager == nil {
		return nil, fmt.Errorf("device manager is required")
	}
	uaPool := LoadUAPool(cfg.UAFilePath)
	envStore := NewEnvironmentStore(deps.DB, uaPool, cfg)
	if err := envStore.Init(context.Background()); err != nil {
		return nil, err
	}
	if cfg.StatusSendConcurrency <= 0 {
		cfg.StatusSendConcurrency = 1
	}
	var accountProxyStore *AccountProxyStore
	if cfg.AccountDBDSN != "" {
		store, err := NewAccountProxyStore(cfg.AccountDBDSN)
		if err != nil {
			return nil, err
		}
		accountProxyStore = store
		logrus.Info("bridge account database proxy resolver enabled")
	}
	workerID := fmt.Sprintf("%s-%d", cfg.InstanceID, os.Getpid())
	service := &Service{
		cfg:               cfg,
		deps:              deps,
		envStore:          envStore,
		uaPool:            uaPool,
		publisher:         NewNATSPublisher(cfg.NATSURL),
		accountProxyStore: accountProxyStore,
		startedAt:         time.Now(),
		workerID:          workerID,
		connected:         make(map[string]time.Time),
		statuses:          make(map[string]string),
		reconnecting:      make(map[string]time.Time),
		statusSendSlots:   make(chan struct{}, cfg.StatusSendConcurrency),
	}
	return service, nil
}

func (s *Service) Start(ctx context.Context) error {
	whatsapp.RegisterEventSink("ims-bridge", s)
	s.publish("bridge.started", "", map[string]any{"instanceId": s.cfg.InstanceID})
	s.publish("worker.ready", "", map[string]any{"workerId": s.workerID, "pid": os.Getpid()})
	go s.heartbeatLoop(ctx)
	go s.restorePersistedAccounts(ctx)

	if err := s.startHTTP(ctx); err != nil {
		return err
	}
	return s.startGRPC(ctx)
}

func (s *Service) Shutdown(ctx context.Context) {
	whatsapp.UnregisterEventSink("ims-bridge")
	if s.publisher != nil {
		s.publisher.Close()
	}
	if s.accountProxyStore != nil {
		_ = s.accountProxyStore.Close()
	}
	if s.grpcServer != nil {
		done := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.grpcServer.Stop()
		}
	}
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(ctx)
	}
}

func (s *Service) startGRPC(ctx context.Context) error {
	addr := fmt.Sprintf("0.0.0.0:%d", s.cfg.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(50*1024*1024),
		grpc.MaxSendMsgSize(50*1024*1024),
		grpc.UnaryInterceptor(unaryPanicRecoveryInterceptor),
	)
	bridgepb.RegisterWhatsAppBridgeServer(s.grpcServer, s)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Shutdown(shutdownCtx)
	}()
	logrus.Infof("ims-compatible gRPC bridge listening on %s", addr)
	return s.grpcServer.Serve(lis)
}

func unaryPanicRecoveryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logrus.WithFields(logrus.Fields{
				"method": info.FullMethod,
				"panic":  fmt.Sprint(recovered),
				"stack":  string(debug.Stack()),
			}).Error("recovered bridge gRPC panic")
			if recoveredErr, ok := recovered.(error); ok {
				err = grpcError(recoveredErr)
				return
			}
			err = grpcError(fmt.Errorf("%v", recovered))
		}
	}()
	return handler(ctx, req)
}

func (s *Service) publish(eventType, accountID string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["type"] = eventType
	data["accountId"] = accountID
	if accountID != "" {
		if _, ok := data["tenantId"]; !ok {
			if tenantID := s.eventTenantID(accountID); tenantID != "" {
				data["tenantId"] = tenantID
			}
		}
	}
	data["timestamp"] = time.Now().UnixMilli()
	s.publisher.Publish(context.Background(), eventType, data)
}

func (s *Service) eventTenantID(accountID string) string {
	if s == nil || s.envStore == nil || accountID == "" {
		return ""
	}
	env, err := s.envStore.Get(context.Background(), accountID)
	if err != nil || env == nil {
		if err != nil {
			logrus.WithError(err).WithField("account_id", accountID).Debug("failed to load bridge environment tenant")
		}
		return ""
	}
	return env.TenantID
}

func (s *Service) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.publishHeartbeat()
		}
	}
}

func (s *Service) publishHeartbeat() {
	accounts := s.usableAccountIDs()
	s.publish("account.heartbeat_batch", "", map[string]any{
		"accountIds": accounts,
		"instanceId": s.cfg.InstanceID,
	})
}
