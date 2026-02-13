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

// SubMessage 订阅消息基础结构（泛型版本）
type SubMessage struct {
	QueueName    string                                       // 队列名称
	ConsumerName string                                       // 消费者名称（用于群组消费）
	AutoAck      bool                                         // 是否自动确认
	FetchCount   int                                          // 每次拉取消息数量
	HandleFunc   func(ctx context.Context, message any) error // 消息处理函数
}

func (m *SubMessage) GetSubMsg() any {
	return m
}

func (m *SubMessage) GetAckHandleFunc() func(ctx context.Context, message *AckMessage) error {
	return nil
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
	GetSubMsg() any
	GetAckHandleFunc() func(ctx context.Context, message *AckMessage) error
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	GmqConnect(ctx context.Context) error              // 连接消息队列
	GmqPublish(ctx context.Context, msg Publish) error // 发布消息
	GmqPublishDelay(ctx context.Context, msg PublishDelay) error
	GmqSubscribe(ctx context.Context, msg Subscribe) error                                         // 订阅消息，返回订阅对象
	GmqGetDeadLetter(ctx context.Context, queueName string, limit int) ([]DeadLetterMsgDTO, error) // 获取死信队列消息
	GmqPing(ctx context.Context) bool                                                              // 检测连接状态
	GmqClose(ctx context.Context) error                                                            // 关闭连接
	GmqGetMetrics(ctx context.Context) *Metrics                                                    // 获取监控指标
	GmqAck(ctx context.Context, msg *AckMessage) error                                             // 确认消息
	GmqNak(ctx context.Context, msg *AckMessage) error                                             // 拒绝消息（可重新入队，直到 MaxDeliver）
}
