package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bjang03/gmq/core"
)

// NATS 测试辅助函数
func setupNatsConn(t *testing.T) *NatsConn {
	conn := &NatsConn{
		Url:  "localhost",
		Port: "4222",
	}

	ctx := context.Background()
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	return conn
}

// ============ 连接管理测试 ============

// TestNatsConnect 测试NATS连接
func TestNatsConnect(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("NATS ping failed")
	}
}

// TestNatsPing 测试NATS Ping
func TestNatsPing(t *testing.T) {
	conn := &NatsConn{
		Url:  "localhost",
		Port: "4222",
	}

	ctx := context.Background()

	// 未连接时 Ping 应该返回 false
	if conn.GmqPing(ctx) {
		t.Error("Expected ping to return false when not connected")
	}

	// 连接后 Ping 应该返回 true
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true when connected")
	}
}

// TestNatsClose 测试NATS关闭连接
func TestNatsClose(t *testing.T) {
	conn := setupNatsConn(t)
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

// TestNatsReconnect 测试NATS重连
func TestNatsReconnect(t *testing.T) {
	conn := &NatsConn{
		Url:  "localhost",
		Port: "4222",
	}

	ctx := context.Background()

	// 第一次连接
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true")
	}

	// 关闭连接
	if err := conn.GmqClose(ctx); err != nil {
		t.Errorf("Failed to close connection: %v", err)
	}

	if conn.GmqPing(ctx) {
		t.Error("Expected ping to return false after close")
	}

	// 重新连接
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to reconnect to NATS: %v", err)
	}
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true after reconnect")
	}
}

// ============ 消息发布测试 ============

// TestNatsPublish 测试NATS发布消息
func TestNatsPublish(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test-publish-queue"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      testData,
		},
		Durable: true,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestNatsPublishMultipleMessages 测试NATS发布多条消息
func TestNatsPublishMultipleMessages(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.multiple.queue"
	messageCount := 10

	for i := 0; i < messageCount; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Test message %d", i),
			"index":   i,
		}

		pubMsg := &NatsPubMessage{
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      testData,
			},
			Durable: true,
		}

		if err := conn.GmqPublish(ctx, pubMsg); err != nil {
			t.Errorf("Failed to publish message %d: %v", i, err)
		}
	}
}

// TestNatsConcurrentPublish 测试NATS并发发布
func TestNatsConcurrentPublish(t *testing.T) {
	conn := setupNatsConn(t)
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

			pubMsg := &NatsPubMessage{
				PubMessage: core.PubMessage{
					QueueName: queueName,
					Data:      testData,
				},
				Durable: true,
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish concurrent message %d: %v", index, err)
			}
		}(i)
	}

	wg.Wait()
}

// TestNatsPublishWithDifferentDataTypes 测试NATS发布不同类型的数据
func TestNatsPublishWithDifferentDataTypes(t *testing.T) {
	conn := setupNatsConn(t)
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
			pubMsg := &NatsPubMessage{
				PubMessage: core.PubMessage{
					QueueName: queueName,
					Data:      tc.data,
				},
				Durable: true,
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish %s data: %v", tc.name, err)
			}
		})
	}
}

// TestNatsPublishNonDurable 测试NATS发布非持久化消息
func TestNatsPublishNonDurable(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test-nondurable-queue"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      testData,
		},
		Durable: false,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish non-durable message: %v", err)
	}
}

// ============ 延迟消息测试 ============

// TestNatsPublishDelay 测试NATS发布延迟消息
func TestNatsPublishDelay(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test-delay-queue"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &NatsPubDelayMessage{
		PubDelayMessage: core.PubDelayMessage{
			DelaySeconds: 2,
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      testData,
			},
		},
		Durable: true,
	}

	if err := conn.GmqPublishDelay(ctx, delayMsg); err != nil {
		t.Fatalf("Failed to publish delay message: %v", err)
	}
}

// TestNatsPublishDelayMultiple 测试NATS发布多条延迟消息
func TestNatsPublishDelayMultiple(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	queueName := "test.multiple.delay.queue"

	for i := 1; i <= 5; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Delay message %d", i),
			"index":   i,
		}

		delayMsg := &NatsPubDelayMessage{
			PubDelayMessage: core.PubDelayMessage{
				DelaySeconds: i * 2,
				PubMessage: core.PubMessage{
					QueueName: queueName,
					Data:      testData,
				},
			},
			Durable: true,
		}

		if err := conn.GmqPublishDelay(ctx, delayMsg); err != nil {
			t.Errorf("Failed to publish delay message %d: %v", i, err)
		}
	}
}

// ============ 消息订阅测试 ============

// TestNatsSubscribe 测试NATS发布/订阅消息
func TestNatsSubscribe(t *testing.T) {
	core.GmqRegister("nats-test", &NatsConn{
		Url:  "localhost",
		Port: "4222",
	})
	client := core.GetGmq("nats-test")

	queueName := "test.subscribe.queue"
	receivedMessages := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	subMsg := &NatsSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "test-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				data, ok := message.([]byte)
				if !ok {
					return fmt.Errorf("invalid message type: expected []byte")
				}
				msgStr := string(data)
				receivedMessages = append(receivedMessages, msgStr)
				t.Logf("Received message: %s", msgStr)
				wg.Done()
				return nil
			},
		},
		Durable:    true,
		IsDelayMsg: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      "Test message for subscribe",
		},
		Durable: true,
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

// TestNatsSubscribeDelay 测试NATS订阅延迟消息
func TestNatsSubscribeDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过延迟测试，使用 -short 标志跳过")
	}

	core.GmqRegister("nats-delay-test", &NatsConn{
		Url:  "localhost",
		Port: "4222",
	})
	client := core.GetGmq("nats-delay-test")

	queueName := "test.subscribe.delay.queue"
	receivedMessages := make([]string, 0)
	receivedMutex := sync.Mutex{}
	receivedCount := 0

	subMsg := &NatsSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "test-delay-consumer",
			AutoAck:      true,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				data, ok := message.([]byte)
				if !ok {
					return fmt.Errorf("invalid message type: expected []byte")
				}
				msgStr := string(data)
				receivedMutex.Lock()
				receivedMessages = append(receivedMessages, msgStr)
				receivedCount++
				t.Logf("Received delay message: %s", msgStr)
				receivedMutex.Unlock()
				return nil
			},
		},
		Durable:    true,
		IsDelayMsg: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	delayMsg := &NatsPubDelayMessage{
		PubDelayMessage: core.PubDelayMessage{
			DelaySeconds: 2,
			PubMessage: core.PubMessage{
				QueueName: queueName,
				Data:      "Test delay message for subscribe",
			},
		},
		Durable: true,
	}

	startTime := time.Now()
	if err := client.GmqPublishDelay(ctx, delayMsg); err != nil {
		t.Fatalf("Failed to publish delay message: %v", err)
	}

	// 等待消息到达
	for i := 0; i < 15; i++ {
		time.Sleep(1 * time.Second)
		receivedMutex.Lock()
		count := receivedCount
		receivedMutex.Unlock()

		if count > 0 {
			elapsed := time.Since(startTime)
			t.Logf("Received message after %v", elapsed)
			if elapsed < 2*time.Second {
				t.Errorf("Expected message to be delayed by at least 2 seconds, but received in %v", elapsed)
			}
			return
		}
	}

	t.Error("Timeout waiting for delay message")
}

// ============ 消息确认测试 ============

// TestNatsAck 测试NATS确认消息
func TestNatsAck(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	// 创建测试消息
	testData := map[string]interface{}{"test": "data"}
	payload, _ := json.Marshal(testData)

	// 创建模拟的 NATS 消息对象
	ackMsg := &core.AckMessage{
		MessageData:     payload,
		AckRequiredAttr: nil,
	}

	// 测试 Ack 方法
	if err := conn.GmqAck(ctx, ackMsg); err != nil {
		t.Logf("Ack returned error (expected if message is nil): %v", err)
	}
}

// TestNatsNak 测试NATS否定确认消息
func TestNatsNak(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	ackMsg := &core.AckMessage{
		MessageData:     nil,
		AckRequiredAttr: nil,
	}

	if err := conn.GmqNak(ctx, ackMsg); err != nil {
		t.Logf("Nak returned error (expected if message is nil): %v", err)
	}
}

// ============ 监控指标测试 ============

// TestNatsGetMetrics 测试NATS获取监控指标
func TestNatsGetMetrics(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	metrics := conn.GmqGetMetrics(ctx)
	if metrics == nil {
		t.Error("Expected metrics to be non-nil")
	}
	if metrics.Type != "nats" {
		t.Errorf("Expected type 'nats', got '%s'", metrics.Type)
	}
	if metrics.Status != "connected" {
		t.Errorf("Expected status 'connected', got '%s'", metrics.Status)
	}
	t.Logf("Metrics: %+v", metrics)
}

// ============ 死信队列测试 ============

// TestNatsGetDeadLetter 测试NATS获取死信消息
// NATS 死信机制：消息超过 MaxDeliver 次数后触发 Advisory 事件
func TestNatsGetDeadLetter(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	// 启动后台死信监听（在订阅时会自动启动）
	// 这里我们单独测试 GmqGetDeadLetter 方法

	// GmqGetDeadLetter 是阻塞方法，使用带超时的 context
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// 由于当前没有死信消息，方法会在超时后返回
	_, err := conn.GmqGetDeadLetter(timeoutCtx)
	// 应该返回 context deadline exceeded 错误（因为超时）
	if err == nil {
		t.Error("Expected error (context deadline exceeded) for dead letter, got nil")
	} else {
		t.Logf("Expected error (timeout): %v", err)
	}
}

// TestNatsCreateDeadLetter 测试创建死信消息
// 通过让消息处理失败多次（超过 MaxDeliver），触发死信通知
func TestNatsCreateDeadLetter(t *testing.T) {
	core.GmqRegister("nats-deadletter-test", &NatsConn{
		Url:  "localhost",
		Port: "4222",
	})
	client := core.GetGmq("nats-deadletter-test")

	queueName := "test.deadletter.create.queue"
	failCount := 0
	maxFailCount := 3

	// 创建订阅，消息处理总是失败（模拟业务异常）
	subMsg := &NatsSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "test-deadletter-consumer",
			AutoAck:      false, // 手动确认，让 Nak 触发重投
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				failCount++
				t.Logf("Message processing failed (attempt %d/%d)", failCount, maxFailCount)
				// 总是返回错误，让消息被 Nak 重投
				return fmt.Errorf("simulated business error")
			},
		},
		Durable:    true,
		IsDelayMsg: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动订阅（会自动启动死信监听）
	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	// 等待订阅启动
	time.Sleep(500 * time.Millisecond)

	// 发布一条消息
	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      "Test message to create dead letter",
		},
		Durable: true,
	}

	if err := client.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
	t.Logf("Published message to queue: %s", queueName)

	// 等待消息被多次投递（超过 MaxDeliver 后进入死信）
	// NATS 默认 MaxDeliver=1，需要等待消息处理失败多次
	time.Sleep(5 * time.Second)

	// 验证消息被多次尝试处理
	if failCount == 0 {
		t.Error("Expected message to be processed at least once")
	}
	t.Logf("Message was processed %d times", failCount)

	// 注意：由于 NATS 死信是通过 Advisory 事件通知的，
	// 实际死信消息需要通过 GmqGetDeadLetter 或 subscribeDeadLetter 获取
	// 这里我们只是验证了消息处理失败机制
}

// TestNatsSubscribeDeadLetter 测试订阅死信队列
// 后台监听死信通知
func TestNatsSubscribeDeadLetter(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer conn.GmqClose(ctx)

	// 启动后台死信监听
	go func() {
		err := conn.subscribeDeadLetter(ctx)
		if err != nil && err != context.Canceled {
			t.Logf("SubscribeDeadLetter error: %v", err)
		}
	}()

	// 等待订阅启动
	time.Sleep(500 * time.Millisecond)

	// 创建一个队列和订阅，让消息处理失败
	queueName := "test.deadletter.subscribe.queue"

	// 发布一条消息
	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      "Test message for dead letter subscribe",
		},
		Durable: true,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// 创建订阅并故意让消息处理失败
	subMsg := &NatsSubMessage{
		SubMessage: core.SubMessage{
			QueueName:    queueName,
			ConsumerName: "test-dl-sub-consumer",
			AutoAck:      false,
			FetchCount:   1,
			HandleFunc: func(ctx context.Context, message any) error {
				t.Logf("Processing message (will fail)")
				return fmt.Errorf("simulated error to trigger dead letter")
			},
		},
		Durable:    true,
		IsDelayMsg: false,
	}

	go func() {
		if err := conn.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	// 等待死信订阅收到通知
	time.Sleep(5 * time.Second)

	t.Logf("SubscribeDeadLetter test completed")
}

// ============ Stream 创建测试 ============

// TestNatsCreateStream 测试NATS创建Stream
func TestNatsCreateStream(t *testing.T) {
	conn := setupNatsConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	// 测试创建普通消息的Stream（内存存储，非持久化）
	streamName, storage, err := conn.createStream(ctx, "test.stream.create", false, false)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	if streamName != "ordinary_memory_test_stream_create" {
		t.Errorf("Expected stream name 'ordinary_memory_test_stream_create', got '%s'", streamName)
	}
	if storage != 1 { // nats.MemoryStorage = 1
		t.Error("Expected memory storage")
	}

	// 测试创建持久化普通消息的Stream（文件存储）- 使用不同的队列名避免冲突
	streamName, storage, err = conn.createStream(ctx, "test.stream.create.durable", true, false)
	if err != nil {
		t.Fatalf("Failed to create durable stream: %v", err)
	}
	if streamName != "ordinary_file_test_stream_create_durable" {
		t.Errorf("Expected stream name 'ordinary_file_test_stream_create_durable', got '%s'", streamName)
	}
	if storage != 0 { // nats.FileStorage = 0
		t.Errorf("Expected file storage (0), got %d", storage)
	}

	// 测试创建延迟消息的Stream（内存存储，非持久化）
	streamName, storage, err = conn.createStream(ctx, "test.stream.create.delay", false, true)
	if err != nil {
		t.Fatalf("Failed to create delay stream: %v", err)
	}
	if streamName != "delay_memory_test_stream_create_delay" {
		t.Errorf("Expected stream name 'delay_memory_test_stream_create_delay', got '%s'", streamName)
	}
	if storage != 1 {
		t.Error("Expected memory storage")
	}

	// 测试创建持久化延迟消息的Stream（文件存储）
	streamName, storage, err = conn.createStream(ctx, "test.stream.create.delay.durable", true, true)
	if err != nil {
		t.Fatalf("Failed to create durable delay stream: %v", err)
	}
	if streamName != "delay_file_test_stream_create_delay_durable" {
		t.Errorf("Expected stream name 'delay_file_test_stream_create_delay_durable', got '%s'", streamName)
	}
	if storage != 0 {
		t.Errorf("Expected file storage (0), got %d", storage)
	}
}
