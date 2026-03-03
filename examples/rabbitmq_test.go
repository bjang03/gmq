package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	mq2 "github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/types"
	"testing"
)

var rabbitMQRegisterName = "rabbitmq-test"

// RabbitMQ register
func rabbitMQRegister(ctx context.Context) {
	gmq.Init("config.yml")
	defer gmq.Shutdown(ctx)
}

// ============ Message Publish Tests ============

// TestRabbitMQPublish tests RabbitMQ publish message
func TestRabbitMQPublish(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.publish.topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &mq2.RabbitMQPubMessage{
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

// TestRabbitMQPublishWithDifferentDataTypes tests RabbitMQ publish with different data types
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
			pubMsg := &mq2.RabbitMQPubMessage{
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

// TestRabbitMQPublishNonDurable tests RabbitMQ publish non-durable message
func TestRabbitMQPublishNonDurable(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.nondurable.topic"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &mq2.RabbitMQPubMessage{
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

// ============ Delay Message Tests ============

// TestRabbitMQPublishDelay tests RabbitMQ publish delay message
func TestRabbitMQPublishDelay(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.delay.topic"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &mq2.RabbitMQPubDelayMessage{
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

// ============ Message Subscribe Tests ============

// TestRabbitMQSubscribe tests RabbitMQ subscribe message
func TestRabbitMQSubscribe(t *testing.T) {
	ctx := context.Background()
	rabbitMQRegister(ctx)

	getGmq := gmq.GetGmq(rabbitMQRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &mq2.RabbitMQSubMessage{
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
