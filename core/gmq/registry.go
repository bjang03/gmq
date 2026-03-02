package core

import (
	"context"
	"github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/spf13/cast"
	"log"
	"time"
)

// globalShutdown 用于优雅关闭信号
var (
	GmqPlugins        = make(map[string]*GmqProxy)
	globalShutdown    = make(chan struct{})
	pluginCancelFuncs = make(map[string]context.CancelFunc)
)

// GmqRegisterPlugins 注册消息队列插件
func GmqRegisterPlugins(name string, plugin Gmq) {
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
}

// Init 启动所有消息队列插件
func Init() {
	config, err := utils.LoadGMQConfig()
	if err != nil {
		log.Fatalf("[GMQ]加载配置失败: %v", err)
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

// connectPlugins 启动后台协程自动维护连接状态，断线自动重连
func connectPlugins(name string, cfg map[string]any) {
	// 先获取已注册的proxy
	proxy, exists := GmqPlugins[name]
	if !exists {
		log.Printf("[GMQ] Plugin %s not registered, skip connection create", name)
		return
	}
	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	pluginCancelFuncs[name] = mgrCancel

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

				log.Printf("[GMQ] %s reconnected successfully", name)
				reconnectDelay = types.BaseReconnectDelay
				p.restoreSubscriptions()
			}
		}
	}(name, cfg, proxy, mgrCtx)
}

// Shutdown 优雅关闭所有消息队列连接
func Shutdown(ctx context.Context) error {
	select {
	case <-globalShutdown:
	default:
		close(globalShutdown)
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
