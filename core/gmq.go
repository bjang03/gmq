// Package gmq 提供统一的消息队列抽象接口，支持多种消息中间件(NATS、Redis-Stream、RabbitMQ等)
package core

import (
	"context"
	"time"
)

// PubMessage 发布消息基础结构
type PubMessage struct {
	QueueName string // 队列名称
	Data      any    // 消息数据
}

// SubMessage 订阅消息基础结构
type SubMessage[T any] struct {
	QueueName  string                                     // 队列名称
	AutoAck    bool                                       // 是否自动确认
	FetchCount int                                        // 每次拉取消息数量
	HandleFunc func(ctx context.Context, message T) error // 消息处理函数
}

// Publish 发布消息接口
type Publish interface {
	GetGmqPublishMsgType()
}

// Subscribe 订阅消息接口
type Subscribe interface {
	GetGmqSubscribeMsgType()
}

// Parser 数据解析器接口
type Parser interface {
	GmqParseData(data any) (dt any, err error)
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	// GmqPublish 发布消息
	GmqPublish(ctx context.Context, msg Publish) error
	// GmqSubscribe 订阅消息
	GmqSubscribe(ctx context.Context, msg Subscribe) error
	// GmqPing 检测连接状态
	GmqPing(ctx context.Context) bool
	// GmqConnect 重连
	GmqConnect(ctx context.Context) error
	// GmqClose 关闭连接
	GmqClose(ctx context.Context) error
}

// GmqPlugins 已注册的消息队列插件集合
var GmqPlugins map[string]Gmq

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，每10秒检测一次，断线自动重连
func GmqRegister(name string, plugin Gmq) {
	//todo 检查gmq对象中是否有连接配置，如果没有直接返回
	GmqPlugins[name] = plugin
	ctx := context.Background()
	go func(name string, plugin Gmq) {
		for {
			select {
			case <-ctx.Done():
				_ = plugin.GmqClose(ctx)
				return
			default:
				if plugin.GmqPing(ctx) {
					time.Sleep(10 * time.Second)
					continue
				}
				if err := plugin.GmqConnect(ctx); err != nil {
					time.Sleep(10 * time.Second)
					continue
				}

			}
		}
	}(name, plugin)
}
