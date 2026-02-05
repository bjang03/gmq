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
	Server ServerConfig `yaml:"server"`
	NATS   NATSConfig   `yaml:"nats"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Address string `yaml:"address"`
	Name    string `yaml:"name"`
}

// NATSConfig NATS配置
type NATSConfig struct {
	URL string `yaml:"url"`
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
