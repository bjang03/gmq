// Package config 提供全局配置管理
package config

import (
	"fmt"
	"os"
	"sync"

	"github.com/goccy/go-yaml"
)

// configMu 保护全局配置访问
var configMu sync.RWMutex

// GlobalConfig 全局配置
var GlobalConfig *Config

// Config 应用配置
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	NATS      NATSConfig      `yaml:"nats"`
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

// Validate 验证NATS配置
func (c *NATSConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("NATS URL cannot be empty")
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

// Validate 验证整个配置
func (c *Config) Validate() error {
	if err := c.NATS.Validate(); err != nil {
		return fmt.Errorf("NATS config error: %w", err)
	}
	if err := c.WebSocket.Validate(); err != nil {
		return fmt.Errorf("WebSocket config error: %w", err)
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

	// 设置默认值
	if cfg.Server.Address == "" {
		cfg.Server.Address = ":1688"
	}
	if cfg.Server.Name == "" {
		cfg.Server.Name = "gmq"
	}
	if cfg.NATS.URL == "" {
		cfg.NATS.URL = "nats://localhost:4222"
	}
	if cfg.NATS.Timeout == 0 {
		cfg.NATS.Timeout = 10
	}
	if cfg.NATS.ReconnectWait == 0 {
		cfg.NATS.ReconnectWait = 5
	}
	if cfg.NATS.MaxReconnects == 0 {
		cfg.NATS.MaxReconnects = -1
	}
	if cfg.NATS.MessageTimeout == 0 {
		cfg.NATS.MessageTimeout = 30
	}
	// WebSocket 默认配置
	if cfg.WebSocket.ReadBufferSize == 0 {
		cfg.WebSocket.ReadBufferSize = 1024
	}
	if cfg.WebSocket.WriteBufferSize == 0 {
		cfg.WebSocket.WriteBufferSize = 1024
	}
	if cfg.WebSocket.PingInterval == 0 {
		cfg.WebSocket.PingInterval = 30
	}
	if cfg.WebSocket.ReadTimeout == 0 {
		cfg.WebSocket.ReadTimeout = 60
	}

	// 验证配置参数
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	configMu.Lock()
	GlobalConfig = &cfg
	configMu.Unlock()
	return nil
}

// GetServerAddress 获取服务器地址
func GetServerAddress() string {
	configMu.RLock()
	defer configMu.RUnlock()
	if GlobalConfig == nil {
		return ":1688"
	}
	return GlobalConfig.Server.Address
}

// GetNATSURL 获取NATS连接地址
func GetNATSURL() string {
	configMu.RLock()
	defer configMu.RUnlock()
	if GlobalConfig == nil {
		return "nats://localhost:4222"
	}
	return GlobalConfig.NATS.URL
}

// GetWebSocketConfig 获取WebSocket配置
func GetWebSocketConfig() WebSocketConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	if GlobalConfig == nil {
		return WebSocketConfig{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			PingInterval:    30,
			ReadTimeout:     60,
		}
	}
	return GlobalConfig.WebSocket
}

// GetNATSConfig 获取NATS配置
func GetNATSConfig() NATSConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	if GlobalConfig == nil {
		return NATSConfig{
			URL:            "nats://localhost:4222",
			Timeout:        10,
			ReconnectWait:  5,
			MaxReconnects:  -1,
			MessageTimeout: 30,
		}
	}
	return GlobalConfig.NATS
}
