package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	mq2 "github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/types"
	"testing"
)

var natsRegisterName = "nats-test"

// NATS register
func natsRegister(ctx context.Context) {
	gmq.Init("config.yml")
	defer gmq.Shutdown(ctx)
}

// ============ Message Publish Tests ============

// TestNatsPublish tests NATS publish message
func TestNatsPublish(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-publish-topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}
	pubMsg := &mq2.NatsPubMessage{
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

// TestNatsPublishWithDifferentDataTypes tests NATS publish with different data types
func TestNatsPublishWithDifferentDataTypes(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

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
			pubMsg := &mq2.NatsPubMessage{
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

// TestNatsPublishNonDurable tests NATS publish non-durable message
func TestNatsPublishNonDurable(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-nondurable-topic"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &mq2.NatsPubMessage{
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

// TestNatsPublishDelay tests NATS publish delay message
func TestNatsPublishDelay(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-delay-topic"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &mq2.NatsPubDelayMessage{
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

// TestNatsSubscribe tests NATS subscribe message
func TestNatsSubscribe(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &mq2.NatsSubMessage{
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
		Durable:    true,
		IsDelayMsg: false,
	}
	if err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}

// TestNatsSubscribeDelay tests NATS subscribe delay message
func TestNatsSubscribeDelay(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test.subscribe.delay.topic"

	subMsg := &mq2.NatsSubMessage{
		SubMessage: types.SubMessage{
			Topic:        topic,
			ConsumerName: "test-delay-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				t.Logf("Received delay message: %s", message)
				return nil
			},
		},
		Durable:    true,
		IsDelayMsg: true,
	}
	if err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}
