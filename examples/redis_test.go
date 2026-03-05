package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	mq2 "github.com/bjang03/gmq/mq"
	"testing"

	"github.com/bjang03/gmq/types"
)

var redisRegisterName = "redis-test"

// Redis register
func redisRegister(ctx context.Context) {
	gmq.Init("config.yml")
	defer gmq.Shutdown(ctx)
}

// ============ Message Publish Tests ============

// TestRedisPublish tests Redis publish message
func TestRedisPublish(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

	topic := "test.publish.topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &mq2.RedisPubMessage{
		PubMessage: types.PubMessage{
			Topic: topic,
			Data:  testData,
		},
	}

	if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestRedisPublishWithDifferentDataTypes tests Redis publish with different data types
func TestRedisPublishWithDifferentDataTypes(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

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
			pubMsg := &mq2.RedisPubMessage{
				PubMessage: types.PubMessage{
					Topic: topic,
					Data:  tc.data,
				},
			}

			if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish %s data: %v", tc.name, err)
			}
		})
	}
}

// ============ Message Subscribe Tests ============

// TestRedisSubscribe tests Redis subscribe message
func TestRedisSubscribe(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &mq2.RedisSubMessage{
		SubMessage: types.SubMessage{
			Topic:        topic,
			ConsumerName: "test-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				t.Logf("Received message: %v", message)
				return nil
			},
		},
	}

	if err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}
