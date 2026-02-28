package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	"github.com/bjang03/gmq/types"
	"testing"
)

var rabbitMQRegisterName = "rabbitmq-test"

// RabbitMQ 注册
func rabbitMQRegister(ctx context.Context) {
	gmq.GmqRegisterPlugins(rabbitMQRegisterName, &RabbitMQConn{})
	gmq.GmqStartPlugins()
	defer gmq.Shutdown(ctx)
}

// ============ 消息发布测试 ============

// TestRabbitMQPublish RabbitMQ发布消息
func TestRabbitMQPublish(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.publish.topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &RabbitMQPubMessage{
		PubMessage: types.PubMessage{
			Topic: topic,
			Data:  testData,
		},
		Durable: true,
	}

	if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestRabbitMQPublishWithDifferentDataTypes RabbitMQ发布不同类型的数据
func TestRabbitMQPublishWithDifferentDataTypes(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.datatypes.topic"

	testCases := []struct {
		name string
		data any
	}{
		{"string", "Test string"},
		{"int", 12345},
		{"float", 123.456},
		{"bool", true},
		{"map", map[string]interface{}{"key": "value", "number": 42}},
		{"slice", []string{"item1", "item2", "item3"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pubMsg := &RabbitMQPubMessage{
				PubMessage: types.PubMessage{
					Topic: topic,
					Data:  tc.data,
				},
				Durable: true,
			}

			if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish %s data: %v", tc.name, err)
			}
		})
	}
}

// TestRabbitMQPublishNonDurable RabbitMQ发布非持久化消息
func TestRabbitMQPublishNonDurable(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.nondurable.topic"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &RabbitMQPubMessage{
		PubMessage: types.PubMessage{
			Topic: topic,
			Data:  testData,
		},
		Durable: false,
	}

	if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish non-durable message: %v", err)
	}
}

// ============ 延迟消息测试 ============

// TestRabbitMQPublishDelay RabbitMQ发布延迟消息
func TestRabbitMQPublishDelay(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.delay.topic"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &RabbitMQPubDelayMessage{
		PubDelayMessage: types.PubDelayMessage{
			DelaySeconds: 2,
			PubMessage: types.PubMessage{
				Topic: topic,
				Data:  testData,
			},
		},
		Durable: true,
	}

	if err := getGmq.GmqPublishDelay(ctx, delayMsg); err != nil {
		t.Fatalf("Failed to publish delay message: %v", err)
	}
}

// ============ 消息订阅测试 ============

// TestRabbitMQSubscribe RabbitMQ订阅消息
func TestRabbitMQSubscribe(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &RabbitMQSubMessage{
		SubMessage: types.SubMessage{
			Topic:        topic,
			ConsumerName: "test-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				t.Logf("Received message: %s", message)
				return nil
			},
		},
	}

	if err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}
