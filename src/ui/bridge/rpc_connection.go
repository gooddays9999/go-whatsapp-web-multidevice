package bridge

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow"
)

func (s *Service) ensureClient(ctx context.Context, accountID, tenantID string, proxy *bridgepb.ProxyConfig, allowRequestProxy bool) (*whatsapp.DeviceInstance, *whatsmeow.Client, *BridgeEnvironment, error) {
	env, _, err := s.envStore.GetOrCreate(ctx, accountID, tenantID, proxy, allowRequestProxy)
	if err != nil {
		return nil, nil, nil, err
	}
	proxyURL, err := env.ProxyURL()
	if err != nil {
		return nil, nil, nil, err
	}
	inst, err := s.deps.DeviceManager.EnsureClientWithEnvironment(ctx, accountID, whatsapp.ClientEnvironment{
		ProxyAddress:  proxyURL,
		UserAgent:     env.UserAgent,
		BrowserFamily: env.BrowserFamily,
		OSName:        env.OSName,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	client := inst.GetClient()
	if client == nil {
		return nil, nil, nil, fmt.Errorf("account not connected")
	}
	return inst, client, env, nil
}

func (s *Service) Connect(ctx context.Context, req *bridgepb.ConnectRequest) (*bridgepb.ConnectResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	inst, client, _, err := s.ensureClient(ctx, req.GetAccountId(), req.GetTenantId(), req.GetProxy(), true)
	if err != nil {
		return nil, grpcError(err)
	}
	if client.Store != nil && client.Store.ID != nil && !client.IsConnected() {
		inst.SetState("connecting")
		if err := client.Connect(); err != nil {
			return &bridgepb.ConnectResponse{Success: false, Status: "failed", Message: err.Error()}, nil
		}
	}
	if client.IsConnected() && client.IsLoggedIn() {
		s.markConnected(req.GetAccountId())
		s.publish("account.connected", req.GetAccountId(), map[string]any{
			"phoneNumber": inst.PhoneNumber(),
			"workerId":    s.workerID,
			"connectedAt": time.Now().UnixMilli(),
			"verified":    true,
		})
		return &bridgepb.ConnectResponse{Success: true, Status: "connected", Message: "Connected to worker " + s.workerID}, nil
	}
	if client.Store == nil || client.Store.ID == nil {
		return &bridgepb.ConnectResponse{Success: true, Status: "qr_pending", Message: "QR login required"}, nil
	}
	return &bridgepb.ConnectResponse{Success: true, Status: "connecting", Message: "Connection started"}, nil
}

func (s *Service) Disconnect(ctx context.Context, req *bridgepb.DisconnectRequest) (*bridgepb.DisconnectResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	if req.GetClearSession() {
		_ = s.envStore.Delete(ctx, req.GetAccountId())
		if err := s.deps.DeviceManager.PurgeDevice(ctx, req.GetAccountId()); err != nil {
			return nil, grpcError(err)
		}
		s.markDisconnected(req.GetAccountId())
		return &bridgepb.DisconnectResponse{Success: true}, nil
	}
	if inst, ok := s.deps.DeviceManager.GetDevice(req.GetAccountId()); ok && inst != nil {
		if client := inst.GetClient(); client != nil {
			client.Disconnect()
		}
		inst.SetState("disconnected")
	}
	s.markDisconnected(req.GetAccountId())
	s.publish("account.disconnected", req.GetAccountId(), map[string]any{"reason": "client_disconnect"})
	return &bridgepb.DisconnectResponse{Success: true}, nil
}

func (s *Service) GetQRCode(req *bridgepb.QRCodeRequest, stream bridgepb.WhatsAppBridge_GetQRCodeServer) error {
	if req.GetAccountId() == "" {
		return grpcError(fmt.Errorf("account_id is required"))
	}
	ctx := stream.Context()
	inst, client, _, err := s.ensureClient(ctx, req.GetAccountId(), "", nil, false)
	if err != nil {
		return grpcError(err)
	}
	if client.IsLoggedIn() {
		return stream.Send(&bridgepb.QRCodeResponse{
			AccountId: req.GetAccountId(),
			Stage:     "authenticated",
			Message:   "Already authenticated",
		})
	}
	client.Disconnect()
	qrCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	ch, err := client.GetQRChannel(qrCtx)
	if err != nil {
		return grpcError(err)
	}
	if err := client.Connect(); err != nil {
		return grpcError(err)
	}
	inst.SetState("connecting")
	for evt := range ch {
		switch evt.Event {
		case "code":
			resp := &bridgepb.QRCodeResponse{
				AccountId: req.GetAccountId(),
				QrCode:    evt.Code,
				Stage:     "qr_generated",
				Message:   "Scan QR code",
			}
			s.publish("account.qrcode", req.GetAccountId(), map[string]any{
				"qrCode":    evt.Code,
				"stage":     "qr_generated",
				"expiresAt": time.Now().Add(evt.Timeout).UnixMilli(),
			})
			if err := stream.Send(resp); err != nil {
				return err
			}
		case "success":
			s.markConnected(req.GetAccountId())
			s.publish("account.authenticated", req.GetAccountId(), map[string]any{"phoneNumber": inst.PhoneNumber()})
			return stream.Send(&bridgepb.QRCodeResponse{AccountId: req.GetAccountId(), Stage: "authenticated", Message: "Authenticated"})
		default:
			if evt.Error != nil {
				_ = stream.Send(&bridgepb.QRCodeResponse{AccountId: req.GetAccountId(), Stage: "failed", Message: evt.Error.Error()})
				return nil
			}
		}
	}
	return nil
}

func (s *Service) GetLinkCode(ctx context.Context, req *bridgepb.LinkCodeRequest) (*bridgepb.LinkCodeResponse, error) {
	if req.GetAccountId() == "" || req.GetPhoneNumber() == "" {
		return nil, grpcError(fmt.Errorf("account_id and phone_number are required"))
	}
	_, client, _, err := s.ensureClient(ctx, req.GetAccountId(), "", nil, false)
	if err != nil {
		return nil, grpcError(err)
	}
	if client.IsLoggedIn() {
		return nil, grpcError(fmt.Errorf("account already logged in"))
	}
	if !client.IsConnected() {
		if err := client.Connect(); err != nil {
			return nil, grpcError(err)
		}
	}
	code, err := client.PairPhone(ctx, req.GetPhoneNumber(), true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		return nil, grpcError(err)
	}
	expiresAt := time.Now().Add(120 * time.Second).UnixMilli()
	s.publish("account.linkcode", req.GetAccountId(), map[string]any{"linkCode": code, "expiresAt": expiresAt})
	return &bridgepb.LinkCodeResponse{AccountId: req.GetAccountId(), LinkCode: code, ExpiresAt: expiresAt}, nil
}

func (s *Service) GetAccountStatus(ctx context.Context, req *bridgepb.AccountStatusRequest) (*bridgepb.AccountStatusResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	inst, ok := s.deps.DeviceManager.GetDevice(req.GetAccountId())
	if !ok || inst == nil {
		return &bridgepb.AccountStatusResponse{AccountId: req.GetAccountId(), Status: "offline", StatusDetail: "Account not connected", IsUsable: false}, nil
	}
	inst.UpdateStateFromClient()
	status := "offline"
	detail := "Account offline"
	usable := false
	if inst.IsConnected() && inst.IsLoggedIn() {
		status = "online"
		detail = "Worker connected, client verified"
		usable = true
	} else if inst.IsConnected() {
		status = "qr_pending"
		detail = "Connected but not authenticated"
	}
	return &bridgepb.AccountStatusResponse{
		AccountId:    req.GetAccountId(),
		Status:       status,
		PhoneNumber:  inst.PhoneNumber(),
		Name:         inst.DisplayName(),
		PushName:     inst.DisplayName(),
		LastSeen:     0,
		IsUsable:     usable,
		StatusDetail: detail,
		Windows:      []*bridgepb.BrowserWindow{},
	}, nil
}

func (s *Service) GetConnectionState(ctx context.Context, req *bridgepb.ConnectionStateRequest) (*bridgepb.ConnectionStateResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	state := "DISCONNECTED"
	if inst, ok := s.deps.DeviceManager.GetDevice(req.GetAccountId()); ok && inst != nil {
		if inst.IsConnected() && inst.IsLoggedIn() {
			state = "CONNECTED"
		} else if inst.IsConnected() {
			state = "QR_PENDING"
		}
	}
	return &bridgepb.ConnectionStateResponse{AccountId: req.GetAccountId(), State: state, WorkerId: s.workerID}, nil
}

func (s *Service) GetAccountStats(ctx context.Context, req *bridgepb.GetAccountStatsRequest) (*bridgepb.GetAccountStatsResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	s.mu.RLock()
	connectedAt := s.connected[req.GetAccountId()]
	s.mu.RUnlock()
	isOnline := !connectedAt.IsZero()
	current := int64(0)
	lastConnected := int64(0)
	if isOnline {
		current = int64(time.Since(connectedAt).Seconds())
		lastConnected = connectedAt.UnixMilli()
	}
	return &bridgepb.GetAccountStatsResponse{
		AccountId:             req.GetAccountId(),
		TotalOnlineSeconds:    current,
		TotalSessions:         boolToInt32(isOnline),
		CurrentSessionSeconds: current,
		LastConnectedAt:       lastConnected,
		LastDisconnectedAt:    0,
		IsOnline:              isOnline,
	}, nil
}

func (s *Service) GetBridgeStats(ctx context.Context, req *bridgepb.GetBridgeStatsRequest) (*bridgepb.GetBridgeStatsResponse, error) {
	accounts := s.accountIDs()
	resp := &bridgepb.GetBridgeStatsResponse{
		InstanceId:    s.cfg.InstanceID,
		TotalWorkers:  1,
		ReadyWorkers:  1,
		TotalAccounts: int32(len(accounts)),
	}
	if req.GetIncludeWorkers() {
		resp.Workers = []*bridgepb.BridgeWorkerInfo{{
			Id:            s.workerID,
			Pid:           int32(os.Getpid()),
			Status:        "ready",
			AccountCount:  int32(len(accounts)),
			Accounts:      accounts,
			StartedAt:     s.startedAt.UnixMilli(),
			LastHeartbeat: time.Now().UnixMilli(),
			MemoryUsage:   0,
		}}
	}
	return resp, nil
}

func (s *Service) GetWebServerStats(ctx context.Context, req *bridgepb.GetWebServerStatsRequest) (*bridgepb.WebServerStats, error) {
	if req.GetServer() == nil {
		return nil, grpcError(fmt.Errorf("server is required"))
	}
	return s.webServerStats(req.GetServer()), nil
}

func (s *Service) BatchGetWebServerStats(ctx context.Context, req *bridgepb.BatchGetWebServerStatsRequest) (*bridgepb.BatchGetWebServerStatsResponse, error) {
	stats := make([]*bridgepb.WebServerStats, 0, len(req.GetServers()))
	for _, spec := range req.GetServers() {
		stats = append(stats, s.webServerStats(spec))
	}
	return &bridgepb.BatchGetWebServerStatsResponse{Stats: stats}, nil
}

func (s *Service) CloseAllTabs(ctx context.Context, req *bridgepb.CloseAllTabsRequest) (*bridgepb.CloseAllTabsResponse, error) {
	if req.GetAccountId() == "" {
		return nil, grpcError(fmt.Errorf("account_id is required"))
	}
	return &bridgepb.CloseAllTabsResponse{Success: true}, nil
}

func (s *Service) webServerStats(spec *bridgepb.WebServerStatSpec) *bridgepb.WebServerStats {
	capacity := spec.GetMaxConcurrentEnvironments()
	if capacity <= 0 {
		capacity = 100
	}
	count := int32(len(s.accountIDs()))
	return &bridgepb.WebServerStats{
		WebServerId:      spec.GetWebServerId(),
		BitbrowserApiUrl: spec.GetBitbrowserApiUrl(),
		Capacity:         capacity,
		OpenedEstimate:   count,
		ReservedOpening:  0,
		OpeningInflight:  0,
		ActualOpenCount:  count,
		LastReconciledAt: time.Now().UnixMilli(),
		Stale:            false,
	}
}

func (s *Service) markConnected(accountID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.connected[accountID]; !ok {
		s.connected[accountID] = time.Now()
	}
	s.statuses[accountID] = "connected"
}

func (s *Service) markDisconnected(accountID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connected, accountID)
	s.statuses[accountID] = "disconnected"
}

func boolToInt32(value bool) int32 {
	if value {
		return 1
	}
	return 0
}
