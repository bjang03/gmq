package core

import (
	"context"
	"log"
	"sync"
	"time"
)

// globalShutdown 用于优雅关闭信号
var (
	GmqPlugins         = make(map[string]*GmqProxy)
	globalShutdown     = make(chan struct{})
	globalShutdownOnce sync.Once
	pluginCancelFuncs  = make(map[string]context.CancelFunc)
)

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，断线自动重连
func GmqRegister(name string, plugin Gmq) {
	log.Printf("[GMQ] Registering plugin: %s\n", name)
	if name == "" {
		log.Printf("[GMQ] Plugin name cannot be empty\n")
		return
	}
	if _, exists := GmqPlugins[name]; exists {
		return
	}

	proxy := newGmqProxy(name, plugin)
	GmqPlugins[name] = proxy

	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	pluginCancelFuncs[name] = mgrCancel

	go func(name string, p *GmqProxy, mgrCtx context.Context) {
		const (
			baseReconnectDelay = 5 * time.Second  // 基础重连延迟
			maxReconnectDelay  = 60 * time.Second // 最大重连延迟
			connectTimeout     = 30 * time.Second // 连接超时
		)
		reconnectDelay := baseReconnectDelay

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
	}(name, proxy, mgrCtx)
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

// GetGmq 获取已注册的消息队列代理
func GetGmq(name string) *GmqProxy {
	proxy, ok := GmqPlugins[name]
	if !ok {
		return nil
	}
	return proxy
}

// GetAllGmq 获取所有已注册的消息队列代理的副本
func GetAllGmq() map[string]*GmqProxy {
	result := make(map[string]*GmqProxy, len(GmqPlugins))
	for k, v := range GmqPlugins {
		result[k] = v
	}
	return result
}
