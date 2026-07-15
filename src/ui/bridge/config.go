package bridge

import (
	"os"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GRPCPort    int
	MetricsPort int
	// EnablePprof exposes net/http/pprof on the metrics port when true (default off).
	// Toggle with BRIDGE_ENABLE_PPROF=true to capture a live goroutine/CPU profile
	// during a send-latency incident, e.g. curl :<metrics>/debug/pprof/goroutine?debug=2.
	EnablePprof bool
	InstanceID  string
	// WebServerID identifies this bridge's row in the ims web_servers table
	// (its server_id, e.g. "api01"). It is published in bridge.started so
	// ims-api can scope its restart handling to this bridge's accounts. Empty
	// keeps the legacy behaviour (ims-api leaves web_online untouched).
	WebServerID                     string
	NATSURL                         string
	UAFilePath                      string
	HeartbeatInterval               time.Duration
	ConnectTimeout                  time.Duration
	StatusSendConcurrency           int
	StatusSendMinInterval           time.Duration
	StatusSendQueueTimeout          time.Duration
	StatusAccountContextTimeout     time.Duration
	StatusRecipientTimeout          time.Duration
	StatusBuildTimeout              time.Duration
	StatusMessageTimeout            time.Duration
	MessageSendTimeout              time.Duration
	MessageReactionTimeout          time.Duration
	HistorySyncOnConnect            bool
	HistorySyncMaxChats             int
	HistorySyncMessageCount         int
	HistorySyncExactOutgoingPerChat int
	HistorySyncTimeout              time.Duration
	HistorySyncMinInterval          time.Duration
	HistorySyncConcurrency          int
	StartupRestoreConcurrency       int
	ReconnectConcurrency            int
	ReconnectQueueTimeout           time.Duration
	MediaDownloadPath               string
	UploadMediaURL                  string
	UploadAPIKey                    string
	AccountDBDSN                    string
	DefaultProxy                    ProxySpec
}

type configFile struct {
	Server struct {
		GRPCPort    int    `yaml:"grpc_port"`
		MetricsPort int    `yaml:"metrics_port"`
		InstanceID  string `yaml:"instance_id"`
		WebServerID string `yaml:"web_server_id"`
	} `yaml:"server"`
	Worker struct {
		HeartbeatInterval             int `yaml:"heartbeat_interval"`
		ConnectTimeout                int `yaml:"connect_timeout"`
		StatusSendConcurrency         int `yaml:"status_send_concurrency"`
		StatusSendMinIntervalMS       int `yaml:"status_send_min_interval_ms"`
		StatusSendQueueTimeoutMS      int `yaml:"status_send_queue_timeout_ms"`
		StatusAccountContextTimeoutMS int `yaml:"status_account_context_timeout_ms"`
		StatusRecipientTimeoutMS      int `yaml:"status_recipient_timeout_ms"`
		StatusBuildTimeoutMS          int `yaml:"status_build_timeout_ms"`
		StatusMessageTimeoutMS        int `yaml:"status_message_timeout_ms"`
		MessageSendTimeoutMS          int `yaml:"message_send_timeout_ms"`
		MessageReactionTimeoutMS      int `yaml:"message_reaction_timeout_ms"`
		StartupRestoreConcurrency     int `yaml:"startup_restore_concurrency"`
		ReconnectConcurrency          int `yaml:"reconnect_concurrency"`
		ReconnectQueueTimeoutMS       int `yaml:"reconnect_queue_timeout_ms"`
	} `yaml:"worker"`
	NATS struct {
		URL string `yaml:"url"`
	} `yaml:"nats"`
	Media struct {
		DownloadPath   string `yaml:"download_path"`
		UploadMediaURL string `yaml:"upload_media_url"`
		UploadAPIKey   string `yaml:"upload_api_key"`
	} `yaml:"media"`
	UA struct {
		FilePath string `yaml:"file_path"`
	} `yaml:"ua"`
	AccountDB struct {
		DSN string `yaml:"dsn"`
	} `yaml:"account_db"`
	Proxy ProxySpec `yaml:"proxy"`
}

type ProxySpec struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     int32  `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func LoadConfig() Config {
	cfg := Config{
		GRPCPort:                        9091,
		MetricsPort:                     9191,
		InstanceID:                      "ims-bridge-go",
		NATSURL:                         "nats://localhost:4222",
		UAFilePath:                      "/Users/eric/Downloads/ua_US.txt",
		HeartbeatInterval:               30 * time.Second,
		ConnectTimeout:                  45 * time.Second,
		StatusSendConcurrency:           1,
		StatusSendMinInterval:           1500 * time.Millisecond,
		StatusSendQueueTimeout:          5 * time.Second,
		StatusAccountContextTimeout:     12 * time.Second,
		StatusRecipientTimeout:          8 * time.Second,
		StatusBuildTimeout:              30 * time.Second,
		StatusMessageTimeout:            25 * time.Second,
		MessageSendTimeout:              25 * time.Second,
		MessageReactionTimeout:          15 * time.Second,
		HistorySyncOnConnect:            true,
		HistorySyncMaxChats:             20,
		HistorySyncMessageCount:         50,
		HistorySyncExactOutgoingPerChat: 2,
		HistorySyncTimeout:              30 * time.Second,
		HistorySyncMinInterval:          5 * time.Minute,
		HistorySyncConcurrency:          5,
		StartupRestoreConcurrency:       20,
		ReconnectConcurrency:            10,
		ReconnectQueueTimeout:           15 * time.Second,
		MediaDownloadPath:               "/tmp/media",
		UploadMediaURL:                  "http://localhost:8080/internal/media/upload",
		UploadAPIKey:                    "",
	}

	if path := os.Getenv("CONFIG_PATH"); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var fileCfg configFile
			if err := yaml.Unmarshal([]byte(expandEnvDefaults(string(data))), &fileCfg); err == nil {
				mergeFileConfig(&cfg, fileCfg)
			}
		}
	}

	applyEnvOverrides(&cfg)
	return cfg
}

func mergeFileConfig(cfg *Config, fileCfg configFile) {
	if fileCfg.Server.GRPCPort > 0 {
		cfg.GRPCPort = fileCfg.Server.GRPCPort
	}
	if fileCfg.Server.MetricsPort > 0 {
		cfg.MetricsPort = fileCfg.Server.MetricsPort
	}
	if fileCfg.Server.InstanceID != "" {
		cfg.InstanceID = fileCfg.Server.InstanceID
	}
	if fileCfg.Server.WebServerID != "" {
		cfg.WebServerID = fileCfg.Server.WebServerID
	}
	if fileCfg.Worker.HeartbeatInterval > 0 {
		cfg.HeartbeatInterval = time.Duration(fileCfg.Worker.HeartbeatInterval) * time.Second
	}
	if fileCfg.Worker.ConnectTimeout > 0 {
		cfg.ConnectTimeout = time.Duration(fileCfg.Worker.ConnectTimeout) * time.Second
	}
	if fileCfg.Worker.StatusSendConcurrency > 0 {
		cfg.StatusSendConcurrency = fileCfg.Worker.StatusSendConcurrency
	}
	if fileCfg.Worker.StatusSendMinIntervalMS > 0 {
		cfg.StatusSendMinInterval = time.Duration(fileCfg.Worker.StatusSendMinIntervalMS) * time.Millisecond
	}
	if fileCfg.Worker.StatusSendQueueTimeoutMS > 0 {
		cfg.StatusSendQueueTimeout = time.Duration(fileCfg.Worker.StatusSendQueueTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.StatusAccountContextTimeoutMS > 0 {
		cfg.StatusAccountContextTimeout = time.Duration(fileCfg.Worker.StatusAccountContextTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.StatusRecipientTimeoutMS > 0 {
		cfg.StatusRecipientTimeout = time.Duration(fileCfg.Worker.StatusRecipientTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.StatusBuildTimeoutMS > 0 {
		cfg.StatusBuildTimeout = time.Duration(fileCfg.Worker.StatusBuildTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.StatusMessageTimeoutMS > 0 {
		cfg.StatusMessageTimeout = time.Duration(fileCfg.Worker.StatusMessageTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.MessageSendTimeoutMS > 0 {
		cfg.MessageSendTimeout = time.Duration(fileCfg.Worker.MessageSendTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.MessageReactionTimeoutMS > 0 {
		cfg.MessageReactionTimeout = time.Duration(fileCfg.Worker.MessageReactionTimeoutMS) * time.Millisecond
	}
	if fileCfg.Worker.StartupRestoreConcurrency > 0 {
		cfg.StartupRestoreConcurrency = fileCfg.Worker.StartupRestoreConcurrency
	}
	if fileCfg.Worker.ReconnectConcurrency > 0 {
		cfg.ReconnectConcurrency = fileCfg.Worker.ReconnectConcurrency
	}
	if fileCfg.Worker.ReconnectQueueTimeoutMS > 0 {
		cfg.ReconnectQueueTimeout = time.Duration(fileCfg.Worker.ReconnectQueueTimeoutMS) * time.Millisecond
	}
	if fileCfg.NATS.URL != "" {
		cfg.NATSURL = fileCfg.NATS.URL
	}
	if fileCfg.Media.DownloadPath != "" {
		cfg.MediaDownloadPath = fileCfg.Media.DownloadPath
	}
	if fileCfg.Media.UploadMediaURL != "" {
		cfg.UploadMediaURL = fileCfg.Media.UploadMediaURL
	}
	if fileCfg.Media.UploadAPIKey != "" {
		cfg.UploadAPIKey = fileCfg.Media.UploadAPIKey
	}
	if fileCfg.UA.FilePath != "" {
		cfg.UAFilePath = fileCfg.UA.FilePath
	}
	if fileCfg.AccountDB.DSN != "" {
		cfg.AccountDBDSN = fileCfg.AccountDB.DSN
	}
	if fileCfg.Proxy.Type != "" || fileCfg.Proxy.Host != "" {
		cfg.DefaultProxy = fileCfg.Proxy
	}
}

func applyEnvOverrides(cfg *Config) {
	if value := os.Getenv("BRIDGE_GRPC_PORT"); value != "" {
		if port, err := strconv.Atoi(value); err == nil && port > 0 {
			cfg.GRPCPort = port
		}
	}
	if value := os.Getenv("BRIDGE_METRICS_PORT"); value != "" {
		if port, err := strconv.Atoi(value); err == nil && port > 0 {
			cfg.MetricsPort = port
		}
	}
	if value := os.Getenv("BRIDGE_ENABLE_PPROF"); value != "" {
		if enabled, err := strconv.ParseBool(value); err == nil {
			cfg.EnablePprof = enabled
		}
	}
	if value := os.Getenv("INSTANCE_ID"); value != "" {
		cfg.InstanceID = value
	}
	if value := os.Getenv("WEB_SERVER_ID"); value != "" {
		cfg.WebServerID = value
	}
	if value := os.Getenv("NATS_URL"); value != "" {
		cfg.NATSURL = value
	}
	if value := os.Getenv("BRIDGE_UA_FILE"); value != "" {
		cfg.UAFilePath = value
	}
	if value := os.Getenv("BRIDGE_CONNECT_TIMEOUT"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
			cfg.ConnectTimeout = time.Duration(seconds) * time.Second
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_SEND_CONCURRENCY"); value != "" {
		if concurrency, err := strconv.Atoi(value); err == nil && concurrency > 0 {
			cfg.StatusSendConcurrency = concurrency
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_SEND_MIN_INTERVAL_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds >= 0 {
			cfg.StatusSendMinInterval = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_SEND_QUEUE_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.StatusSendQueueTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_ACCOUNT_CONTEXT_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.StatusAccountContextTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_RECIPIENT_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.StatusRecipientTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_BUILD_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.StatusBuildTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_STATUS_MESSAGE_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.StatusMessageTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_MESSAGE_SEND_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.MessageSendTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_MESSAGE_REACTION_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.MessageReactionTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_ON_CONNECT"); value != "" {
		cfg.HistorySyncOnConnect = value != "false" && value != "0"
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_MAX_CHATS"); value != "" {
		if maxChats, err := strconv.Atoi(value); err == nil && maxChats >= 0 {
			cfg.HistorySyncMaxChats = maxChats
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_MESSAGE_COUNT"); value != "" {
		if count, err := strconv.Atoi(value); err == nil && count >= 0 {
			cfg.HistorySyncMessageCount = count
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_EXACT_OUTGOING_PER_CHAT"); value != "" {
		if count, err := strconv.Atoi(value); err == nil && count >= 0 {
			cfg.HistorySyncExactOutgoingPerChat = count
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.HistorySyncTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_MIN_INTERVAL_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds >= 0 {
			cfg.HistorySyncMinInterval = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("BRIDGE_HISTORY_SYNC_CONCURRENCY"); value != "" {
		if concurrency, err := strconv.Atoi(value); err == nil && concurrency > 0 {
			cfg.HistorySyncConcurrency = concurrency
		}
	}
	if value := os.Getenv("BRIDGE_STARTUP_RESTORE_CONCURRENCY"); value != "" {
		if concurrency, err := strconv.Atoi(value); err == nil && concurrency > 0 {
			cfg.StartupRestoreConcurrency = concurrency
		}
	}
	if value := os.Getenv("BRIDGE_RECONNECT_CONCURRENCY"); value != "" {
		if concurrency, err := strconv.Atoi(value); err == nil && concurrency > 0 {
			cfg.ReconnectConcurrency = concurrency
		}
	}
	if value := os.Getenv("BRIDGE_RECONNECT_QUEUE_TIMEOUT_MS"); value != "" {
		if milliseconds, err := strconv.Atoi(value); err == nil && milliseconds > 0 {
			cfg.ReconnectQueueTimeout = time.Duration(milliseconds) * time.Millisecond
		}
	}
	if value := os.Getenv("UPLOAD_MEDIA_URL"); value != "" {
		cfg.UploadMediaURL = value
	}
	if value := os.Getenv("UPLOAD_API_KEY"); value != "" {
		cfg.UploadAPIKey = value
	}
	if value := os.Getenv("BRIDGE_ACCOUNT_DB_DSN"); value != "" {
		cfg.AccountDBDSN = value
	}
	if value := os.Getenv("ACCOUNT_DB_DSN"); value != "" {
		cfg.AccountDBDSN = value
	}
}

var envDefaultPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?}`)

func expandEnvDefaults(input string) string {
	return envDefaultPattern.ReplaceAllStringFunc(input, func(match string) string {
		parts := envDefaultPattern.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		if value := os.Getenv(parts[1]); value != "" {
			return value
		}
		if len(parts) >= 3 {
			return parts[2]
		}
		return ""
	})
}
