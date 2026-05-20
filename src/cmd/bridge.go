package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/ui/bridge"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	bridgeGRPCPort    int
	bridgeMetricsPort int
	bridgeUAFile      string
)

var bridgeCmd = &cobra.Command{
	Use:   "bridge",
	Short: "Start ims-bridge compatible gRPC/NATS bridge",
	Run:   bridgeServer,
}

func init() {
	rootCmd.AddCommand(bridgeCmd)
	bridgeCmd.Flags().IntVar(&bridgeGRPCPort, "grpc-port", 0, "gRPC port for ims-compatible bridge")
	bridgeCmd.Flags().IntVar(&bridgeMetricsPort, "metrics-port", 0, "health/metrics port for ims-compatible bridge")
	bridgeCmd.Flags().StringVar(&bridgeUAFile, "ua-file", "", "path to one-user-agent-per-line UA pool")
}

func bridgeServer(_ *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(context.Background())
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalCh)
	defer cancel()
	go func() {
		sig := <-signalCh
		logrus.WithField("signal", sig.String()).Warn("bridge received shutdown signal")
		cancel()
	}()

	cfg := bridge.LoadConfig()
	if bridgeGRPCPort > 0 {
		cfg.GRPCPort = bridgeGRPCPort
	}
	if bridgeMetricsPort > 0 {
		cfg.MetricsPort = bridgeMetricsPort
	}
	if bridgeUAFile != "" {
		cfg.UAFilePath = bridgeUAFile
	}

	service, err := bridge.NewService(cfg, bridge.Dependencies{
		DB:              chatStorageDB,
		DeviceManager:   whatsapp.GetDeviceManager(),
		ChatStorageRepo: chatStorageRepo,
		SendUsecase:     sendUsecase,
		UserUsecase:     userUsecase,
		MessageUsecase:  messageUsecase,
		GroupUsecase:    groupUsecase,
	})
	if err != nil {
		logrus.Fatalf("failed to initialize bridge: %v", err)
	}
	if err := service.Start(ctx); err != nil {
		logrus.Fatalf("bridge server stopped: %v", err)
	}
	logrus.Info("bridge server stopped")
}
