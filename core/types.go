package core

import (
	"context"
	"time"
)

// 默认重试配置
const (
	DefaultRetryAttempts = 3
	DefaultRetryDelay    = 500 * time.Millisecond
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

// SubscriptionValidator 订阅对象验证接口
type SubscriptionValidator interface {
	IsValid() bool
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	// GmqConnect 连接消息队列
	GmqConnect(ctx context.Context) error
	// GmqPublish 发布消息
	GmqPublish(ctx context.Context, msg Publish) error
	// GmqSubscribe 订阅消息，返回订阅对象
	GmqSubscribe(ctx context.Context, msg any) (interface{}, error)
	// GmqPing 检测连接状态
	GmqPing(ctx context.Context) bool
	// GmqClose 关闭连接
	GmqClose(ctx context.Context) error
	// GetMetrics 获取监控指标
	GetMetrics(ctx context.Context) *Metrics
}
