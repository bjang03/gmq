package core

import (
	"context"
	"time"
)

// 默认重试配置
const (
	DefaultRetryAttempts = 3                      // 默认重试次数
	DefaultRetryDelay    = 500 * time.Millisecond // 默认重试间隔
)

// SubMessage 订阅消息基础结构
type SubMessage[T any] struct {
	QueueName    string                                     // 队列名称
	ConsumerName string                                     // 消费者名称（用于群组消费）
	AutoAck      bool                                       // 是否自动确认
	FetchCount   int                                        // 每次拉取消息数量
	HandleFunc   func(ctx context.Context, message T) error // 消息处理函数
}

// GetQueueName 获取队列名称（实现 QueueNameProvider 接口）
func (m *SubMessage[T]) GetQueueName() string {
	return m.QueueName
}

// GetConsumerName 获取消费者名称（实现 ConsumerNameProvider 接口）
func (m *SubMessage[T]) GetConsumerName() string {
	return m.ConsumerName
}

// PubMessage 发布消息基础结构
type PubMessage struct {
	QueueName string // 队列名称
	Data      any    // 消息数据
}

// Publish 发布消息接口
type Publish interface {
	GetGmqPublishMsgType()
}

// Subscribe 订阅消息接口
type Subscribe interface {
	GetGmqSubscribeMsgType()
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	GmqConnect(ctx context.Context) error                           // 连接消息队列
	GmqPublish(ctx context.Context, msg Publish) error              // 发布消息
	GmqSubscribe(ctx context.Context, msg any) (interface{}, error) // 订阅消息，返回订阅对象
	GmqPing(ctx context.Context) bool                               // 检测连接状态
	GmqClose(ctx context.Context) error                             // 关闭连接
	GetMetrics(ctx context.Context) *Metrics                        // 获取监控指标
}
