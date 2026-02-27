package types

import (
	"context"
	"time"
)

// 默认重试配置
const (
	MsgRetryDeliver = 3               // 消息的最大重试次数，达到此值后进入死信队列(默认3次)
	MsgRetryDelay   = 3 * time.Second // 消息的重试延迟时间(秒，默认3s)
)

// PubMessage 发布消息基础结构
type PubMessage struct {
	Topic string // 主题名称
	Data  any    // 消息数据
}

// GetTopic 获取主题名称
func (m *PubMessage) GetTopic() string {
	return m.Topic
}

// GetData 获取消息数据
func (m *PubMessage) GetData() any {
	return m.Data
}

// PubDelayMessage 延迟发布消息基础结构
type PubDelayMessage struct {
	PubMessage
	DelaySeconds int // 延迟秒数
}

func (m *PubDelayMessage) GetDelaySeconds() int {
	return m.DelaySeconds
}

// SubMessage 订阅消息基础结构
type SubMessage struct {
	Topic        string                                       // 主题名称
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

// AckMessage 确认消息结构
type AckMessage struct {
	MessageData     any // 消息数据
	AckRequiredAttr any // 确认所需属性（如 nats.Msg、amqp.Delivery 等）
}

// Publish 发布消息接口（用于类型约束）
type Publish interface {
	GetTopic() string
	GetData() any
}

// PublishDelay 发布延迟消息接口（用于类型约束）
type PublishDelay interface {
	GetTopic() string
	GetData() any
	GetDelaySeconds() int
}

// Subscribe 订阅消息接口（用于类型约束）
type Subscribe interface {
	GetSubMsg() any
	GetAckHandleFunc() func(ctx context.Context, message *AckMessage) error
}
