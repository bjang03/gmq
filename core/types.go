package core

import (
	"context"
	"time"
)

// 默认重试配置
const (
	MsgRetryDeliver = 3               // 消息的最大重试次数，达到此值后进入死信队列(默认3次)
	MsgRetryDelay   = 3 * time.Second // 消息的重试延迟时间(秒，默认3m)
)

// PubMessage 发布消息基础结构
type PubMessage struct {
	QueueName string // 队列名称
	Data      any    // 消息数据
}

// GetQueueName 获取队列名称
func (m *PubMessage) GetQueueName() string {
	return m.QueueName
}

// GetData 获取消息数据
func (m *PubMessage) GetData() any {
	return m.Data
}

type PubDelayMessage struct {
	PubMessage
	DelaySeconds int
}

func (m *PubDelayMessage) GetDelaySeconds() int {
	return m.DelaySeconds
}

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

// GetAutoAck 获取是否自动确认
func (m *SubMessage[T]) GetAutoAck() bool {
	return m.AutoAck
}

// GetFetchCount 获取每次拉取消息数量
func (m *SubMessage[T]) GetFetchCount() int {
	return m.FetchCount
}

func (m *SubMessage[T]) GetHandleFunc() func(ctx context.Context, message T) error {
	return m.HandleFunc
}

func (m *SubMessage[T]) SetHandleFunc(handleFunc func(ctx context.Context, message T) error) {
	m.HandleFunc = handleFunc
}

type AckMessage struct {
	MessageData     any
	AckRequiredAttr map[string]any
}

// Publish 发布消息接口（用于类型约束）
type Publish interface {
	GetQueueName() string
	GetData() any
}

// PublishDelay 发布延迟消息接口（用于类型约束）
type PublishDelay interface {
	GetQueueName() string
	GetData() any
	GetDelaySeconds() int
}

// Subscribe 订阅消息接口（用于类型约束）
type Subscribe interface {
	GetQueueName() string
	GetConsumerName() string
	GetAutoAck() bool
	GetFetchCount() int
	GetHandleFunc() func(ctx context.Context, message any) error
	SetHandleFunc(handleFunc func(ctx context.Context, message any) error)
}

// DeadLetterMsgDTO 死信消息DTO（给前端返回的结构化数据）
type DeadLetterMsgDTO struct {
	MessageID   string                 `json:"message_id"`   // 消息ID
	Body        string                 `json:"body"`         // 消息体
	Headers     map[string]interface{} `json:"headers"`      // 消息头（包含死信原因等信息）
	Timestamp   string                 `json:"timestamp"`    // 消息发布时间
	Exchange    string                 `json:"exchange"`     // 原交换机
	RoutingKey  string                 `json:"routing_key"`  // 原路由键
	DeadReason  string                 `json:"dead_reason"`  // 死信原因（解析自headers）
	QueueName   string                 `json:"queue_name"`   // 死信队列名称
	DeliveryTag uint64                 `json:"delivery_tag"` // 投递标签（用于手动操作）
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	GmqConnect(ctx context.Context) error              // 连接消息队列
	GmqPublish(ctx context.Context, msg Publish) error // 发布消息
	GmqPublishDelay(ctx context.Context, msg PublishDelay) error
	GmqSubscribe(ctx context.Context, msg any) error                                               // 订阅消息，返回订阅对象
	GmqGetDeadLetter(ctx context.Context, queueName string, limit int) ([]DeadLetterMsgDTO, error) // 获取死信队列消息
	GmqPing(ctx context.Context) bool                                                              // 检测连接状态
	GmqClose(ctx context.Context) error                                                            // 关闭连接
	GetMetrics(ctx context.Context) *Metrics                                                       // 获取监控指标
	Ack(msg *AckMessage) error                                                                     // 确认消息
	Nak(msg *AckMessage) error                                                                     // 拒绝消息（可重新入队，直到 MaxDeliver）
	Term(msg *AckMessage) error                                                                    // 终止消息（直接丢弃，不进入死信队列）
}
