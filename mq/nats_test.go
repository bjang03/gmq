package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	"github.com/bjang03/gmq/types"
	"testing"
)

var natsRegisterName = "nats-test"

// NATS 注册
func natsRegister(ctx context.Context) {
	conn := &NatsConn{
		NatsConfig: types.NatsConfig{
			Url:  "localhost",
			Port: "4222",
			//Username: "nats",// 选填
			//Password: "<PASSWORD>",// 选填
		},
	}
	gmq.GmqRegister(natsRegisterName, conn)
	defer gmq.Shutdown(ctx)
}

// ============ 消息发布测试 ============

// TestNatsPublish NATS发布消息
func TestNatsPublish(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-publish-topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}
	pubMsg := &NatsPubMessage{
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

// TestNatsPublishWithDifferentDataTypes NATS发布不同类型的数据
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
			pubMsg := &NatsPubMessage{
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

// TestNatsPublishNonDurable NATS发布非持久化消息
func TestNatsPublishNonDurable(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-nondurable-topic"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &NatsPubMessage{
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

// TestNatsPublishDelay NATS发布延迟消息
func TestNatsPublishDelay(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test-delay-topic"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &NatsPubDelayMessage{
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

// TestNatsSubscribe NATS订阅消息
func TestNatsSubscribe(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &NatsSubMessage{
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

// TestNatsSubscribeDelay NATS订阅延迟消息
func TestNatsSubscribeDelay(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)

	topic := "test.subscribe.delay.topic"

	subMsg := &NatsSubMessage{
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
