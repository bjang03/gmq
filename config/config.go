// Package config 提供全局配置管理
package config

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

// 默认配置常量（问题14修复：集中管理默认值）
const (
	// 服务器默认配置
	DefaultServerAddress = ":1688"
	DefaultServerName    = "gmq"
	// WebSocket 默认配置
	DefaultWSReadBufferSize  = 1024
	DefaultWSWriteBufferSize = 1024
	DefaultWSPingInterval    = 30
	DefaultWSReadTimeout     = 60
)

// GlobalConfig 全局配置
var GlobalConfig *Config

// Config 应用配置
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	NATS      NATSConfig      `yaml:"nats"`
	RocketMQ  RocketMQConfig  `yaml:"rocketmq"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	WebSocket WebSocketConfig `yaml:"websocket"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Address string `yaml:"address"`
	Name    string `yaml:"name"`
}

// NATSConfig NATS配置
type NATSConfig struct {
	URL            string `yaml:"url"`
	Timeout        int    `yaml:"timeout"`        // 连接超时(秒)
	ReconnectWait  int    `yaml:"reconnectWait"`  // 重连等待(秒)
	MaxReconnects  int    `yaml:"maxReconnects"`  // 最大重连次数(-1为无限)
	MessageTimeout int    `yaml:"messageTimeout"` // 消息处理超时(秒)
}

// WebSocketConfig WebSocket配置
type WebSocketConfig struct {
	ReadBufferSize  int `yaml:"readBufferSize"`
	WriteBufferSize int `yaml:"writeBufferSize"`
	PingInterval    int `yaml:"pingInterval"`
	ReadTimeout     int `yaml:"readTimeout"`
}

// RocketMQConfig RocketMQ配置
type RocketMQConfig struct {
	NameServers    string `yaml:"nameServers"`    // NameServer地址
	Timeout        int    `yaml:"timeout"`        // 连接超时(秒)
	ReconnectWait  int    `yaml:"reconnectWait"`  // 重连等待(秒)
	MaxReconnects  int    `yaml:"maxReconnects"`  // 最大重连次数(-1为无限)
	MessageTimeout int    `yaml:"messageTimeout"` // 消息处理超时(秒)
	ConsumerGroup  string `yaml:"consumerGroup"`  // 消费者组名
	InstanceName   string `yaml:"instanceName"`   // 实例名称
}

// KafkaConfig Kafka配置
type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`        // Broker地址列表
	Timeout        int      `yaml:"timeout"`        // 连接超时(秒)
	ReconnectWait  int      `yaml:"reconnectWait"`  // 重连等待(秒)
	MaxReconnects  int      `yaml:"maxReconnects"`  // 最大重连次数(-1为无限)
	MessageTimeout int      `yaml:"messageTimeout"` // 消息处理超时(秒)
	ConsumerGroup  string   `yaml:"consumerGroup"`  // 消费者组名
	ClientID       string   `yaml:"clientId"`       // 客户端ID
}

// Validate 验证WebSocket配置
func (c *WebSocketConfig) Validate() error {
	if c.ReadBufferSize < 0 || c.ReadBufferSize > 65535 {
		return fmt.Errorf("readBufferSize must be between 0 and 65535")
	}
	if c.WriteBufferSize < 0 || c.WriteBufferSize > 65535 {
		return fmt.Errorf("writeBufferSize must be between 0 and 65535")
	}
	if c.PingInterval < 1 || c.PingInterval > 3600 {
		return fmt.Errorf("pingInterval must be between 1 and 3600 seconds")
	}
	if c.ReadTimeout < 1 || c.ReadTimeout > 3600 {
		return fmt.Errorf("readTimeout must be between 1 and 3600 seconds")
	}
	return nil
}

// Validate 验证RocketMQ配置
func (c *RocketMQConfig) Validate() error {
	if c.NameServers == "" {
		return fmt.Errorf("RocketMQ NameServers cannot be empty")
	}
	if c.Timeout < 1 || c.Timeout > 300 {
		return fmt.Errorf("timeout must be between 1 and 300 seconds")
	}
	if c.ReconnectWait < 1 || c.ReconnectWait > 3600 {
		return fmt.Errorf("reconnectWait must be between 1 and 3600 seconds")
	}
	if c.MaxReconnects < -1 {
		return fmt.Errorf("maxReconnects must be -1 or positive")
	}
	if c.MessageTimeout < 1 || c.MessageTimeout > 3600 {
		return fmt.Errorf("messageTimeout must be between 1 and 3600 seconds")
	}
	return nil
}

// Validate 验证Kafka配置
func (c *KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("Kafka brokers cannot be empty")
	}
	if c.Timeout < 1 || c.Timeout > 300 {
		return fmt.Errorf("timeout must be between 1 and 300 seconds")
	}
	if c.ReconnectWait < 1 || c.ReconnectWait > 3600 {
		return fmt.Errorf("reconnectWait must be between 1 and 3600 seconds")
	}
	if c.MaxReconnects < -1 {
		return fmt.Errorf("maxReconnects must be -1 or positive")
	}
	if c.MessageTimeout < 1 || c.MessageTimeout > 3600 {
		return fmt.Errorf("messageTimeout must be between 1 and 3600 seconds")
	}
	return nil
}

// LoadConfig 加载配置文件
func LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// 设置默认值（使用常量）
	if cfg.Server.Address == "" {
		cfg.Server.Address = DefaultServerAddress
	}
	if cfg.Server.Name == "" {
		cfg.Server.Name = DefaultServerName
	}
	// WebSocket 默认配置
	if cfg.WebSocket.ReadBufferSize == 0 {
		cfg.WebSocket.ReadBufferSize = DefaultWSReadBufferSize
	}
	if cfg.WebSocket.WriteBufferSize == 0 {
		cfg.WebSocket.WriteBufferSize = DefaultWSWriteBufferSize
	}
	if cfg.WebSocket.PingInterval == 0 {
		cfg.WebSocket.PingInterval = DefaultWSPingInterval
	}
	if cfg.WebSocket.ReadTimeout == 0 {
		cfg.WebSocket.ReadTimeout = DefaultWSReadTimeout
	}
	GlobalConfig = &cfg
	return nil
}

// GetServerAddress 获取服务器地址
func GetServerAddress() string {
	if GlobalConfig == nil {
		return DefaultServerAddress
	}
	return GlobalConfig.Server.Address
}

// GetWebSocketConfig 获取WebSocket配置
func GetWebSocketConfig() WebSocketConfig {
	if GlobalConfig == nil {
		return WebSocketConfig{
			ReadBufferSize:  DefaultWSReadBufferSize,
			WriteBufferSize: DefaultWSWriteBufferSize,
			PingInterval:    DefaultWSPingInterval,
			ReadTimeout:     DefaultWSReadTimeout,
		}
	}
	return GlobalConfig.WebSocket
}
