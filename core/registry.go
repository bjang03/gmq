package core

import (
	"context"
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

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，断线自动重连
func GmqRegister(name string, plugin Gmq) {
	// 创建管道包装器
	pipeline := newGmqPipeline(name, plugin)

	pluginsMu.Lock()
	GmqPlugins[name] = pipeline
	pluginsMu.Unlock()

	go func(name string, p *GmqPipeline) {
		// 重连退避配置
		const (
			baseReconnectDelay = 5 * time.Second
			maxReconnectDelay  = 60 * time.Second
		)
		reconnectDelay := baseReconnectDelay

		for {
			select {
			case <-globalShutdown:
				// 收到关闭信号，关闭连接并退出
				_ = p.GmqClose(context.Background())
				return
			default:
				if p.GmqPing(context.Background()) {
					// 连接正常，重置退避时间
					reconnectDelay = baseReconnectDelay
					time.Sleep(10 * time.Second)
					continue
				}

				// 连接断开，尝试重连
				if err := p.GmqConnect(context.Background()); err != nil {
					// 重连失败，增加退避时间
					time.Sleep(reconnectDelay)
					reconnectDelay *= 2
					if reconnectDelay > maxReconnectDelay {
						reconnectDelay = maxReconnectDelay
					}
					continue
				}

				// 重连成功，重置退避时间
				reconnectDelay = baseReconnectDelay
			}
		}
	}(name, pipeline)
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
