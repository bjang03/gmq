package core

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// GmqPlugins 已注册的消息队列插件集合
var (
	GmqPlugins = make(map[string]*GmqPipeline)
	pluginsMu  sync.RWMutex
)

// globalShutdown 用于优雅关闭信号
var (
	globalShutdown     = make(chan struct{})
	globalShutdownOnce sync.Once
)

// pluginCancelFuncs 插件取消函数集合，支持单个插件的注销
var (
	pluginCancelFuncs = make(map[string]context.CancelFunc)
	pluginCancelMu    sync.RWMutex
)

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，断线自动重连
func GmqRegister(name string, plugin Gmq) error {
	if name == "" {
		return fmt.Errorf("plugin name cannot be empty")
	}

	pluginsMu.Lock()
	if _, exists := GmqPlugins[name]; exists {
		pluginsMu.Unlock()
		return fmt.Errorf("plugin %s already registered", name)
	}

	pipeline := newGmqPipeline(name, plugin)
	GmqPlugins[name] = pipeline
	pluginsMu.Unlock()

	mgrCtx, mgrCancel := context.WithCancel(context.Background())

	pluginCancelMu.Lock()
	pluginCancelFuncs[name] = mgrCancel
	pluginCancelMu.Unlock()

	go func(name string, p *GmqPipeline, mgrCtx context.Context) {
		const (
			baseReconnectDelay = 5 * time.Second  // 基础重连延迟
			maxReconnectDelay  = 60 * time.Second // 最大重连延迟
			connectTimeout     = 30 * time.Second // 连接超时
		)
		reconnectDelay := baseReconnectDelay

		for {
			select {
			case <-globalShutdown:
				p.mu.Lock()
				p.clearSubscriptions()
				p.mu.Unlock()
				return
			case <-mgrCtx.Done():
				p.mu.Lock()
				p.clearSubscriptions()
				p.mu.Unlock()
				return
			default:
				pingCtx, pingCancel := context.WithTimeout(mgrCtx, 5*time.Second)
				isConnected := p.GmqPing(pingCtx)
				pingCancel()

				if isConnected {
					reconnectDelay = baseReconnectDelay
					select {
					case <-time.After(10 * time.Second):
					case <-mgrCtx.Done():
						return
					}
					continue
				}

				connCtx, connCancel := context.WithTimeout(mgrCtx, connectTimeout)
				err := p.GmqConnect(connCtx)
				connCancel()

				if err != nil {
					select {
					case <-time.After(reconnectDelay):
					case <-mgrCtx.Done():
						return
					}
					reconnectDelay *= 2
					if reconnectDelay > maxReconnectDelay {
						reconnectDelay = maxReconnectDelay
					}
					continue
				}

				log.Printf("[GMQ] %s reconnected successfully", name)
				reconnectDelay = baseReconnectDelay
				p.restoreSubscriptions()
			}
		}
	}(name, pipeline, mgrCtx)

	return nil
}

// Shutdown 优雅关闭所有消息队列连接
func Shutdown(ctx context.Context) error {
	globalShutdownOnce.Do(func() {
		select {
		case <-globalShutdown:
		default:
			close(globalShutdown)
		}
	})

	pluginsMu.RLock()
	pipelines := make([]*GmqPipeline, 0, len(GmqPlugins))
	for _, p := range GmqPlugins {
		pipelines = append(pipelines, p)
	}
	pluginsMu.RUnlock()

	var lastErr error
	for _, p := range pipelines {
		if err := p.GmqClose(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GmqUnregister 注销消息队列插件
// 停止后台 goroutine 并清理资源
func GmqUnregister(name string) error {
	if name == "" {
		return fmt.Errorf("plugin name cannot be empty")
	}

	pluginCancelMu.Lock()
	cancelFunc, exists := pluginCancelFuncs[name]
	if exists {
		cancelFunc()
		delete(pluginCancelFuncs, name)
	}
	pluginCancelMu.Unlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	pluginsMu.Lock()
	pipeline, exists := GmqPlugins[name]
	if exists {
		delete(GmqPlugins, name)
	}
	pluginsMu.Unlock()

	if pipeline != nil {
		if err := pipeline.GmqClose(context.Background()); err != nil {
			return fmt.Errorf("failed to close plugin %s: %w", name, err)
		}
	}

	return nil
}

// GetGmq 获取已注册的消息队列管道
func GetGmq(name string) (*GmqPipeline, bool) {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	pipeline, ok := GmqPlugins[name]
	return pipeline, ok
}

// GetAllGmq 获取所有已注册的消息队列管道的副本
func GetAllGmq() map[string]*GmqPipeline {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()

	result := make(map[string]*GmqPipeline, len(GmqPlugins))
	for k, v := range GmqPlugins {
		result[k] = v
	}
	return result
}
