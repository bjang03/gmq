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
