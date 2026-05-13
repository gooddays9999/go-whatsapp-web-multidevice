package bridge

import (
	"os"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GRPCPort          int
	MetricsPort       int
	InstanceID        string
	NATSURL           string
	UAFilePath        string
	HeartbeatInterval time.Duration
	MediaDownloadPath string
	UploadMediaURL    string
	UploadAPIKey      string
	DefaultProxy      ProxySpec
}

type configFile struct {
	Server struct {
		GRPCPort    int    `yaml:"grpc_port"`
		MetricsPort int    `yaml:"metrics_port"`
		InstanceID  string `yaml:"instance_id"`
	} `yaml:"server"`
	Worker struct {
		HeartbeatInterval int `yaml:"heartbeat_interval"`
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
		GRPCPort:          9091,
		MetricsPort:       9191,
		InstanceID:        "ims-bridge-go",
		NATSURL:           "nats://localhost:4222",
		UAFilePath:        "/Users/eric/Downloads/ua_US.txt",
		HeartbeatInterval: 30 * time.Second,
		MediaDownloadPath: "/tmp/media",
		UploadMediaURL:    "http://localhost:8080/internal/media/upload",
		UploadAPIKey:      "",
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
	if fileCfg.Worker.HeartbeatInterval > 0 {
		cfg.HeartbeatInterval = time.Duration(fileCfg.Worker.HeartbeatInterval) * time.Second
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
	if value := os.Getenv("INSTANCE_ID"); value != "" {
		cfg.InstanceID = value
	}
	if value := os.Getenv("NATS_URL"); value != "" {
		cfg.NATSURL = value
	}
	if value := os.Getenv("BRIDGE_UA_FILE"); value != "" {
		cfg.UAFilePath = value
	}
	if value := os.Getenv("UPLOAD_MEDIA_URL"); value != "" {
		cfg.UploadMediaURL = value
	}
	if value := os.Getenv("UPLOAD_API_KEY"); value != "" {
		cfg.UploadAPIKey = value
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
