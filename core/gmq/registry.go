// Package core provides the core functionality for the GMQ message queue system.
// It includes the unified Gmq interface, proxy wrapper, and plugin registry.
// The proxy layer adds monitoring, retry logic, and connection management.
//
// Plugin Registry:
//   - GmqRegisterPlugins: Register a new message queue plugin instance
//   - GetGmq: Retrieve a specific plugin by name
//   - GetAllGmq: Get all registered plugins
//
// Lifecycle Management:
//   - Init: Initialize the GMQ system with configuration from config.yml
//   - Shutdown: Gracefully close all connections and stop background goroutines
//
// Connection Management:
//   - Automatic reconnection with exponential backoff (5s to 60s max)
//   - Connection health monitoring via periodic ping checks
//   - Subscription restoration after reconnection
package core

import (
	"context"
	"time"

	"github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/spf13/cast"
)

// GmqPlugins stores all registered message queue plugins (name -> GmqProxy)
// globalShutdown is a signal channel for graceful shutdown of all plugins
// pluginCancelFuncs stores cancellation functions for plugin connection goroutines (name -> cancelFunc)
var (
	GmqPlugins        = make(map[string]*GmqProxy)
	globalShutdown    = make(chan struct{})
	pluginCancelFuncs = make(map[string]context.CancelFunc)
)

// GmqRegisterPlugins registers a message queue plugin with a given name.
// If the name is empty or already registered, this function logs a warning and returns without error.
// The plugin is wrapped in a GmqProxy for unified monitoring and retry logic.
// Parameters:
//   - name: unique identifier for the plugin (used for logging and plugin retrieval)
//   - plugin: message queue implementation instance (must implement Gmq interface)
func GmqRegisterPlugins(name string, plugin Gmq) {
	utils.LogInfo("registering plugin", "name", name, "plugin", name)
	if name == "" {
		utils.LogWarn("plugin name cannot be empty", "plugin", "registry")
		return
	}

	if _, exists := GmqPlugins[name]; exists {
		utils.LogWarn("plugin already registered, skipping", "name", name, "plugin", name)
		return
	}
	proxy := newGmqProxy(name, plugin)
	GmqPlugins[name] = proxy
}

// Init initializes the GMQ system by loading configuration and registering plugins.
// It reads the config.yml file, creates plugin instances based on configuration,
// and starts background goroutines for connection management.
// This function should be called once at application startup.
// Example:
//
//	gmq.Init()  // Load config and register plugins
//	defer gmq.Shutdown(context.Background())  // Clean shutdown
func Init(configPath string) {
	config, err := utils.LoadGMQConfig(configPath)
	if err != nil {
		utils.LogError("failed to load config", "error", err, "plugin", "registry")
		return
	}
	for secondLevel, thirdLevelData := range config.GMQ {
		for thirdLevel, configItems := range thirdLevelData {
			configMap := cast.ToStringMap(configItems)
			if secondLevel == "nats" {
				GmqRegisterPlugins(thirdLevel, &mq.NatsConn{})
			}
			if secondLevel == "redis" {
				GmqRegisterPlugins(thirdLevel, &mq.RedisConn{})
			}
			if secondLevel == "rabbitmq" {
				GmqRegisterPlugins(thirdLevel, &mq.RabbitMQConn{})
			}
			connectPlugins(thirdLevel, configMap)
		}
	}
}

// connectPlugins starts background goroutine to automatically maintain connection status and reconnect on disconnect.
// It implements exponential backoff retry logic with configurable delay limits.
// Parameters:
//   - name: plugin name
//   - cfg: plugin configuration parameters
func connectPlugins(name string, cfg map[string]any) {
	proxy, exists := GmqPlugins[name]

	if !exists {
		utils.LogWarn("plugin not registered, skip connection create", "name", name, "plugin", name)
		return
	}

	// Check if already has a running connection goroutine
	if _, exists := pluginCancelFuncs[name]; exists {
		utils.LogWarn("plugin connection goroutine already exists, skipping", "name", name, "plugin", name)
		return
	}
	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	pluginCancelFuncs[name] = mgrCancel

	logger := utils.LogWithPlugin(name)

	go func(name string, cfg map[string]any, p *GmqProxy, mgrCtx context.Context) {
		reconnectDelay := types.BaseReconnectDelay
		for {
			select {
			case <-globalShutdown:
				p.clearSubscriptions()
				return
			case <-mgrCtx.Done():
				p.clearSubscriptions()
				return
			default:
				pingCtx, pingCancel := context.WithTimeout(mgrCtx, 5*time.Second)
				isConnected := p.GmqPing(pingCtx)
				pingCancel()

				if isConnected {
					reconnectDelay = types.BaseReconnectDelay
					select {
					case <-time.After(10 * time.Second):
					case <-mgrCtx.Done():
						return
					}
					continue
				}

				connCtx, connCancel := context.WithTimeout(mgrCtx, types.ConnectTimeout)
				err := p.GmqConnect(connCtx, cfg)
				connCancel()

				if err != nil {
					logger.Error("connect failed, wait for retry", "error", err, "delay", reconnectDelay)
					select {
					case <-time.After(reconnectDelay):
					case <-mgrCtx.Done():
						return
					}
					reconnectDelay *= 2
					if reconnectDelay > types.MaxReconnectDelay {
						reconnectDelay = types.MaxReconnectDelay
					}
					continue
				}

				logger.Info("reconnected successfully", "name", name)
			}
		}
	}(name, cfg, proxy, mgrCtx)
}

// Shutdown gracefully shuts down all message queue connections and stops background goroutines.
// Parameters:
//   - ctx: context for timeout/cancellation control
//
// Returns the last error encountered during shutdown (may be nil)
func Shutdown(ctx context.Context) error {
	select {
	case <-globalShutdown:
	default:
		close(globalShutdown)
	}

	// Cancel all plugin connection goroutines
	for name, cancel := range pluginCancelFuncs {
		cancel()
		delete(pluginCancelFuncs, name)
	}

	proxies := make([]*GmqProxy, 0, len(GmqPlugins))
	for _, p := range GmqPlugins {
		proxies = append(proxies, p)
	}

	var lastErr error
	for _, p := range proxies {
		if err := p.GmqClose(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GetGmq retrieves a registered message queue proxy by name.
// Parameters:
//   - name: plugin name
//
// Returns the GmqProxy if found, nil otherwise
func GetGmq(name string) *GmqProxy {
	proxy, ok := GmqPlugins[name]
	if !ok {
		return nil
	}
	return proxy
}

// GetAllGmq returns a copy of all registered message queue proxies.
// Returns a map of plugin names to their corresponding GmqProxy instances
func GetAllGmq() map[string]*GmqProxy {
	result := make(map[string]*GmqProxy, len(GmqPlugins))
	for k, v := range GmqPlugins {
		result[k] = v
	}
	return result
}
