// Package types provides unified type definitions for the GMQ message queue system.
// It includes configuration structures, message types, constants, and error definitions
// used across all message queue implementations (NATS, Redis, RabbitMQ).
package types

import (
	"context"
)

// PubMessage publish message base structure
type PubMessage struct {
	Topic string // topic name
	Data  any    // message data
}

// GetTopic gets topic name
func (m *PubMessage) GetTopic() string {
	return m.Topic
}

// GetData gets message data
func (m *PubMessage) GetData() any {
	return m.Data
}

// PubDelayMessage delayed publish message base structure
type PubDelayMessage struct {
	PubMessage
	DelaySeconds int // delay seconds
}

// GetDelaySeconds gets delay seconds
func (m *PubDelayMessage) GetDelaySeconds() int {
	return m.DelaySeconds
}

// SubMessage subscription message base structure
type SubMessage struct {
	Topic        string                                       // topic name to subscribe to
	ConsumerName string                                       // consumer name for group consumption (required)
	AutoAck      bool                                         // when true: acknowledge before processing; when false: acknowledge after successful processing (default: false)
	FetchCount   int                                          // number of messages to fetch each time (QoS prefetch count, must be > 0)
	HandleFunc   func(ctx context.Context, message any) error // message handler function (not used directly in wrapped implementation)
}

// GetSubMsg gets subscription message
func (m *SubMessage) GetSubMsg() any {
	return m
}

// GetAckHandleFunc gets acknowledgment handler function
func (m *SubMessage) GetAckHandleFunc() func(ctx context.Context, message *AckMessage) error {
	return nil
}

// AckMessage acknowledgment message structure used for message processing feedback.
// This structure wraps the original message and provides attributes required for acknowledgment operations.
type AckMessage struct {
	MessageData     any // message data payload received from the message queue
	AckRequiredAttr any // attributes required for acknowledgment (e.g., *nats.Msg, amqp.Delivery, or map for Redis)
}

// Publish publish message interface (type constraint for publish operations)
type Publish interface {
	GetTopic() string // Get the message topic/subject
	GetData() any     // Get the message payload data
}

// PublishDelay publish delayed message interface (type constraint for delayed publish operations)
type PublishDelay interface {
	GetTopic() string     // Get the message topic/subject
	GetData() any         // Get the message payload data
	GetDelaySeconds() int // Get the delay time in seconds
}

// Subscribe subscribe message interface (type constraint for subscription operations)
type Subscribe interface {
	GetSubMsg() any                                                         // Get the subscription message configuration
	GetAckHandleFunc() func(ctx context.Context, message *AckMessage) error // Get the acknowledgment handler function
}
