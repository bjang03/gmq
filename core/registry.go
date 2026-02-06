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

// 问题18修复：添加插件取消通道，支持单个插件的注销
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
	// 检查是否已注册，防止重复注册导致内存泄漏
	if _, exists := GmqPlugins[name]; exists {
		pluginsMu.Unlock()
		return fmt.Errorf("plugin %s already registered", name)
	}

	// 创建管道包装器
	pipeline := newGmqPipeline(name, plugin)
	GmqPlugins[name] = pipeline
	pluginsMu.Unlock()

	// 问题18修复：创建独立的取消 context，支持 Unregister
	mgrCtx, mgrCancel := context.WithCancel(context.Background())

	// 保存取消函数，用于 Unregister 时关闭 goroutine
	pluginCancelMu.Lock()
	pluginCancelFuncs[name] = mgrCancel
	pluginCancelMu.Unlock()

	go func(name string, p *GmqPipeline, mgrCtx context.Context) {
		// 重连退避配置
		const (
			baseReconnectDelay = 5 * time.Second
			maxReconnectDelay  = 60 * time.Second
			connectTimeout     = 30 * time.Second // 连接超时
		)
		reconnectDelay := baseReconnectDelay

		for {
			select {
			case <-globalShutdown:
				// Shutdown 重复关闭问题修复：只清理订阅，不关闭连接
				// 连接关闭由 Shutdown 函数统一处理
				p.mu.Lock()
				p.clearSubscriptions()
				p.mu.Unlock()
				return
			case <-mgrCtx.Done():
				// 问题18修复：Unregister 时清理订阅并退出
				p.mu.Lock()
				p.clearSubscriptions()
				p.mu.Unlock()
				return
			default:
				// 使用带超时的 context 进行 ping 检查
				pingCtx, pingCancel := context.WithTimeout(mgrCtx, 5*time.Second)
				isConnected := p.GmqPing(pingCtx)
				pingCancel()

				if isConnected {
					// 连接正常，重置退避时间
					reconnectDelay = baseReconnectDelay
					select {
					case <-time.After(10 * time.Second):
					case <-mgrCtx.Done():
						return
					}
					continue
				}

				// 连接断开，尝试重连（使用带超时的 context）
				connCtx, connCancel := context.WithTimeout(mgrCtx, connectTimeout)
				err := p.GmqConnect(connCtx)
				connCancel()

				if err != nil {
					// 重连失败，增加退避时间
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

				// 重连成功，重置退避时间，恢复所有订阅
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
			// channel 已关闭，跳过
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

// GmqUnregister 注销消息队列插件（问题18修复）
// 停止后台 goroutine 并清理资源
func GmqUnregister(name string) error {
	if name == "" {
		return fmt.Errorf("plugin name cannot be empty")
	}

	// 取消后台 goroutine
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

	// 关闭连接
	pluginsMu.Lock()
	pipeline, exists := GmqPlugins[name]
	if exists {
		delete(GmqPlugins, name)
	}
	pluginsMu.Unlock()

	if pipeline != nil {
		// 使用 background context 关闭
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

	// 返回副本，避免外部修改
	result := make(map[string]*GmqPipeline, len(GmqPlugins))
	for k, v := range GmqPlugins {
		result[k] = v
	}
	return result
}
