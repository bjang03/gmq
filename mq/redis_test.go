package mq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// Redis 测试辅助函数
func setupRedisConn(t *testing.T) *RedisConn {
	conn := &RedisConn{
		Url:  "localhost",
		Port: "6379",
	}

	ctx := context.Background()
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	return conn
}

// ============ 连接管理测试 ============

// TestRedisConnect 测试Redis连接
func TestRedisConnect(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Redis ping failed")
	}
}

// TestRedisPing 测试Redis Ping
func TestRedisPing(t *testing.T) {
	conn := &RedisConn{
		Url:  "localhost",
		Port: "6379",
	}

	ctx := context.Background()

	// 未连接时 Ping 应该返回 false
	if conn.GmqPing(ctx) {
		t.Error("Expected ping to return false when not connected")
	}

	// 连接后 Ping 应该返回 true
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true when connected")
	}
}

// TestRedisClose 测试Redis关闭连接
func TestRedisClose(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true before close")
	}

	if err := conn.GmqClose(ctx); err != nil {
		t.Errorf("Failed to close connection: %v", err)
	}

	// 关闭后 Ping 应该返回 false
	if conn.GmqPing(ctx) {
		t.Error("Expected ping to return false after close")
	}
}

// ============ 消息发布测试 ============

// TestRedisPublish 测试Redis发布消息
func TestRedisPublish(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.publish.queue"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &RedisPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      testData,
		},
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestRedisPublishMultipleMessages 测试Redis发布多条消息
func TestRedisPublishMultipleMessages(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.multiple.queue"
	messageCount := 10

	for i := 0; i < messageCount; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Test message %d", i),
			"index":   i,
		}

		pubMsg := &RedisPubMessage{
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      testData,
			},
		}

		if err := conn.GmqPublish(ctx, pubMsg); err != nil {
			t.Errorf("Failed to publish message %d: %v", i, err)
		}
	}
}

// TestRedisConcurrentPublish 测试Redis并发发布
func TestRedisConcurrentPublish(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.concurrent.queue"
	concurrentCount := 100
	var wg sync.WaitGroup
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func(index int) {
			defer wg.Done()
			testData := map[string]interface{}{
				"message": fmt.Sprintf("Concurrent message %d", index),
				"index":   index,
			}

			pubMsg := &RedisPubMessage{
				PubMessage: core.PubMessage{
					QueueName: queueName,
					Data:      testData,
				},
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish concurrent message %d: %v", index, err)
			}
		}(i)
	}

	wg.Wait()
}

// TestRedisPublishWithDifferentDataTypes 测试Redis发布不同类型的数据
func TestRedisPublishWithDifferentDataTypes(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.datatypes.queue"

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
				PubMessage: core.PubMessage{
					QueueName: queueName,
					Data:      tc.data,
				},
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish %s data: %v", tc.name, err)
			}
		})
	}
}

// TestRedisPublishDelay 测试Redis延迟消息（Redis不支持延迟消息）
func TestRedisPublishDelay(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	err := conn.GmqPublishDelay(ctx, &RedisPubDelayMessage{})
	if err == nil {
		t.Error("Expected error for delay message, got nil")
	}
}

// ============ 消息订阅测试 ============

// TestRedisSubscribe 测试Redis订阅消息
func TestRedisSubscribe(t *testing.T) {
	core.GmqRegister("redis-test", &RedisConn{
		Url:  "localhost",
		Port: "6379",
	})
	client := core.GetGmq("redis-test")

	queueName := "test.subscribe.queue"
	receivedMessages := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	subMsg := &RedisSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "test-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				data := cast.ToStringMapString(message)
				msgStr := data["data"]
				receivedMessages = append(receivedMessages, msgStr)
				t.Logf("Received message: %v", message)
				wg.Done()
				return nil
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	pubMsg := &RedisPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      "Test message for subscribe",
		},
	}

	if err := client.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if len(receivedMessages) == 0 {
			t.Error("Expected to receive at least 1 message")
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

// ============ 消息确认测试 ============

// TestRedisAck 测试Redis确认消息
func TestRedisAck(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.ack.queue"
	testData := map[string]interface{}{
		"message": "Test message for ack",
	}

	// 发布消息
	pubMsg := &RedisPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      testData,
		},
	}
	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// 模拟消息确认
	ackMsg := &core.AckMessage{
		MessageData: testData,
		AckRequiredAttr: map[string]any{
			"MessageId": "test-message-id",
			"QueueName": "gmq:stream:" + queueName,
			"Group":     "test-group",
		},
	}

	if err := conn.GmqAck(ctx, ackMsg); err != nil {
		t.Logf("Ack returned error (expected if message doesn't exist): %v", err)
	}
}

// TestRedisNak 测试Redis否定确认消息（Redis不支持Nak）
func TestRedisNak(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	ackMsg := &core.AckMessage{}
	if err := conn.GmqNak(ctx, ackMsg); err != nil {
		t.Logf("Nak returned error (expected): %v", err)
	}
}

// ============ 监控指标测试 ============

// TestRedisGetMetrics 测试Redis获取监控指标
func TestRedisGetMetrics(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	metrics := conn.GmqGetMetrics(ctx)
	if metrics == nil {
		t.Error("Expected metrics to be non-nil")
	}
	if metrics.Type != "redis" {
		t.Errorf("Expected type 'redis', got '%s'", metrics.Type)
	}
	if metrics.Status != "connected" {
		t.Errorf("Expected status 'connected', got '%s'", metrics.Status)
	}
	t.Logf("Metrics: %+v", metrics)
}

// ============ 死信队列测试 ============

// TestRedisGetDeadLetter 测试Redis获取死信消息
func TestRedisGetDeadLetter(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	messages, err := conn.GmqGetDeadLetter(ctx)
	if err != nil {
		t.Logf("GetDeadLetter returned error: %v", err)
	}
	if messages == nil {
		messages = []core.DeadLetterMsgDTO{}
	}
	t.Logf("Found %d dead letter messages", len(messages))
}

// TestRedisCreateDeadLetter 创建死信消息用于测试
func TestRedisCreateDeadLetter(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.deadletter.demo"
	streamKey := "gmq:stream:" + queueName

	// 清理旧数据
	t.Log("Cleaning old data...")
	conn.conn.Del(ctx, streamKey)

	// 发布测试消息
	t.Log("Publishing test messages...")
	for i := 0; i < 5; i++ {
		testData := map[string]interface{}{
			"message":  fmt.Sprintf("Demo DLQ message %d", i),
			"index":    i,
			"priority": "high",
			"retry":    0,
		}

		pubMsg := &RedisPubMessage{
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      testData,
			},
		}

		if err := conn.GmqPublish(ctx, pubMsg); err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// 创建消费组
	groupName := "demo-consumer:default:group"
	conn.conn.XGroupCreateMkStream(ctx, streamKey, groupName, "0")

	// 消费消息但不确认
	t.Log("Consuming messages (no ack)...")
	msgs, _ := conn.conn.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "demo-consumer",
		Count:    5,
		Streams:  []string{streamKey, ">"},
	}).Result()

	for _, stream := range msgs {
		for _, msg := range stream.Messages {
			t.Logf("Consumed: ID=%s, Data=%v", msg.ID, msg.Values)
		}
	}

	t.Log("\nMessages are now pending (not acked)")
	t.Log("Wait for 180 seconds to see them as dead letters")
}

// TestRedisCheckPending 检查pending消息状态
func TestRedisCheckPending(t *testing.T) {
	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.deadletter.demo"
	streamKey := "gmq:stream:" + queueName

	// 获取所有消费组
	groups, err := conn.getStreamGroups(ctx, streamKey)
	if err != nil {
		t.Logf("Failed to get groups: %v", err)
		return
	}

	t.Logf("Groups for queue '%s': %v", queueName, groups)

	for _, groupName := range groups {
		t.Logf("\n=== Group: %s ===", groupName)
		pendingList, err := conn.getPendingMsgList(ctx, streamKey, groupName)
		if err != nil {
			t.Logf("Failed to get pending: %v", err)
			continue
		}

		t.Logf("Pending count: %d", len(pendingList))
		for i, p := range pendingList {
			t.Logf("  [%d] ID=%s, Idle=%dms (%.1fs), Consumer=%s",
				i, p.ID, p.Idle, float64(p.Idle)/1000, p.Consumer)
		}
	}
}

// TestRedisDeadLetterFull 完整的死信队列测试（耗时约180秒）
// 跳过：go test -short ./mq
func TestRedisDeadLetterFull(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过耗时测试，使用 -short 标志跳过")
	}

	conn := setupRedisConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.deadletter.full"

	// 步骤1: 发布消息
	t.Log("Step 1: Publishing messages...")
	for i := 0; i < 3; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("DLQ full test message %d", i),
			"index":   i,
		}

		pubMsg := &RedisPubMessage{
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      testData,
			},
		}

		if err := conn.GmqPublish(ctx, pubMsg); err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}
	t.Log("Published 3 messages")

	// 步骤2: 订阅消息但不确认
	t.Log("Step 2: Subscribing (will not ack)...")
	core.GmqRegister("redis-dlq-full", &RedisConn{
		Url:  "localhost",
		Port: "6379",
	})
	client := core.GetGmq("redis-dlq-full")

	subMsg := &RedisSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "dlq-full-consumer",
			AutoAck:      false, // 不自动确认
			FetchCount:   3,
			HandleFunc: func(ctx context.Context, message any) error {
				t.Logf("Received message: %v", message)
				// 返回错误，消息不会被确认
				return fmt.Errorf("simulated consumer error")
			},
		},
	}

	// 启动订阅goroutine
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(subCtx, subMsg); err != nil {
			t.Logf("Subscribe ended: %v", err)
		}
	}()

	// 等待消息被消费
	time.Sleep(2 * time.Second)
	t.Log("Messages consumed but not acked")

	// 步骤3: 等待180秒让消息变成死信
	t.Log("Step 3: Waiting 180 seconds for idle timeout...")
	for i := 180; i > 0; i-- {
		if i%30 == 0 || i <= 10 {
			t.Logf("Countdown: %d seconds remaining...", i)
		}
		time.Sleep(1 * time.Second)
	}

	// 步骤4: 检查死信队列
	t.Log("Step 4: Checking dead letter queue...")
	deadLetterMessages, err := conn.GmqGetDeadLetter(ctx)
	if err != nil {
		t.Logf("Failed to get dead letter messages: %v", err)
	}

	t.Logf("Dead letter count: %d", len(deadLetterMessages))

	// 打印死信详情
	for i, msg := range deadLetterMessages {
		t.Logf("\n=== Dead Letter %d ===", i+1)
		t.Logf("  MessageID: %s", msg.MessageID)
		t.Logf("  Queue: %s", msg.QueueName)
		t.Logf("  Timestamp: %s", msg.Timestamp)
		t.Logf("  DeadReason: %s", msg.DeadReason)
		t.Logf("  Body: %v", msg.Body)
	}

	// 验证结果
	if len(deadLetterMessages) == 0 {
		t.Error("Expected at least 1 dead letter message, got 0")
	}
}
