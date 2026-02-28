package mq

import (
	"context"
	gmq "github.com/bjang03/gmq/core/gmq"
	"testing"

	"github.com/bjang03/gmq/types"
)

var redisRegisterName = "redis-test"

// Redis 注册
func redisRegister(ctx context.Context) {
	gmq.GmqRegisterPlugins(redisRegisterName, &RedisConn{})
	gmq.GmqStartPlugins()
	defer gmq.Shutdown(ctx)
}

// ============ 消息发布测试 ============

// TestRedisPublish 测试Redis发布消息
func TestRedisPublish(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

	topic := "test.publish.topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &RedisPubMessage{
		PubMessage: types.PubMessage{
			Topic: topic,
			Data:  testData,
		},
	}

	if err := getGmq.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestRedisPublishWithDifferentDataTypes 测试Redis发布不同类型的数据
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
			pubMsg := &RedisPubMessage{
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

// TestRedisPublishDelay 测试Redis延迟消息（Redis不支持延迟消息）
func TestRedisPublishDelay(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

	err := getGmq.GmqPublishDelay(ctx, &RedisPubDelayMessage{})
	if err == nil {
		t.Error("Expected error for delay message, got nil")
	}
}

// ============ 消息订阅测试 ============

// TestRedisSubscribe 测试Redis订阅消息
func TestRedisSubscribe(t *testing.T) {
	ctx := context.Background()
	redisRegister(ctx)

	getGmq := gmq.GetGmq(redisRegisterName)

	topic := "test.subscribe.topic"

	subMsg := &RedisSubMessage{
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
