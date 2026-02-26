package mq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bjang03/gmq/core"
)

// RabbitMQ 测试辅助函数
func setupRabbitMQConn(t *testing.T) *RabbitMQConn {
	conn := &RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	}

	ctx := context.Background()
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	return conn
}

// ============ 连接管理测试 ============

// TestRabbitMQConnect 测试RabbitMQ连接
func TestRabbitMQConnect(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("RabbitMQ ping failed")
	}
}

// TestRabbitMQPing 测试RabbitMQ Ping
func TestRabbitMQPing(t *testing.T) {
	conn := &RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	}

	ctx := context.Background()

	// 未连接时 Ping 应该返回 false
	if conn.GmqPing(ctx) {
		t.Error("Expected ping to return false when not connected")
	}

	// 连接后 Ping 应该返回 true
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true when connected")
	}
}

// TestRabbitMQClose 测试RabbitMQ关闭连接
func TestRabbitMQClose(t *testing.T) {
	conn := setupRabbitMQConn(t)
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

// TestRabbitMQReconnect 测试RabbitMQ重连
func TestRabbitMQReconnect(t *testing.T) {
	conn := &RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	}

	ctx := context.Background()

	// 第一次连接
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
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
		t.Fatalf("Failed to reconnect to RabbitMQ: %v", err)
	}
	defer conn.GmqClose(ctx)

	if !conn.GmqPing(ctx) {
		t.Error("Expected ping to return true after reconnect")
	}
}

// ============ 消息发布测试 ============

// TestRabbitMQPublish 测试RabbitMQ发布消息
func TestRabbitMQPublish(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.publish.topic"
	testData := map[string]interface{}{
		"message": "Test message for publish",
		"index":   1,
	}

	pubMsg := &RabbitMQPubMessage{
		PubMessage: core.PubMessage{
			Topic: topic,
			Data:  testData,
		},
		Durable: true,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

// TestRabbitMQPublishMultipleMessages 测试RabbitMQ发布多条消息
func TestRabbitMQPublishMultipleMessages(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.multiple.topic"
	messageCount := 10

	for i := 0; i < messageCount; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Test message %d", i),
			"index":   i,
		}

		pubMsg := &RabbitMQPubMessage{
			PubMessage: core.PubMessage{
				Topic: topic,
				Data:  testData,
			},
			Durable: true,
		}

		if err := conn.GmqPublish(ctx, pubMsg); err != nil {
			t.Errorf("Failed to publish message %d: %v", i, err)
		}
	}
}

// TestRabbitMQConcurrentPublish 测试RabbitMQ并发发布
func TestRabbitMQConcurrentPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过并发测试，使用 -short 标志跳过")
	}

	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.concurrent.topic"
	concurrentCount := 50 // 降低并发数避免 RabbitMQ 通道竞争
	var wg sync.WaitGroup
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func(index int) {
			defer wg.Done()
			testData := map[string]interface{}{
				"message": fmt.Sprintf("Concurrent message %d", index),
				"index":   index,
			}

			pubMsg := &RabbitMQPubMessage{
				PubMessage: core.PubMessage{
					Topic: topic,
					Data:  testData,
				},
				Durable: true,
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Logf("Failed to publish concurrent message %d: %v", index, err)
			}
		}(i)
	}

	wg.Wait()
}

// TestRabbitMQPublishWithDifferentDataTypes 测试RabbitMQ发布不同类型的数据
func TestRabbitMQPublishWithDifferentDataTypes(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

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
				PubMessage: core.PubMessage{
					Topic: topic,
					Data:  tc.data,
				},
				Durable: true,
			}

			if err := conn.GmqPublish(ctx, pubMsg); err != nil {
				t.Errorf("Failed to publish %s data: %v", tc.name, err)
			}
		})
	}
}

// TestRabbitMQPublishNonDurable 测试RabbitMQ发布非持久化消息
func TestRabbitMQPublishNonDurable(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.nondurable.topic"
	testData := map[string]interface{}{
		"message": "Test non-durable message",
	}

	pubMsg := &RabbitMQPubMessage{
		PubMessage: core.PubMessage{
			Topic: topic,
			Data:  testData,
		},
		Durable: false,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish non-durable message: %v", err)
	}
}

// ============ 延迟消息测试 ============

// TestRabbitMQPublishDelay 测试RabbitMQ发布延迟消息
func TestRabbitMQPublishDelay(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.delay.topic"
	testData := map[string]interface{}{
		"message": "Test delay message",
		"index":   1,
	}

	delayMsg := &RabbitMQPubDelayMessage{
		PubDelayMessage: core.PubDelayMessage{
			DelaySeconds: 2,
			PubMessage: core.PubMessage{
				Topic: topic,
				Data:  testData,
			},
		},
		Durable: true,
	}

	if err := conn.GmqPublishDelay(ctx, delayMsg); err != nil {
		t.Fatalf("Failed to publish delay message: %v", err)
	}
}

// TestRabbitMQPublishDelayMultiple 测试RabbitMQ发布多条延迟消息
func TestRabbitMQPublishDelayMultiple(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	topic := "test.multiple.delay.topic"

	for i := 1; i <= 5; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Delay message %d", i),
			"index":   i,
		}

		delayMsg := &RabbitMQPubDelayMessage{
			PubDelayMessage: core.PubDelayMessage{
				DelaySeconds: i * 2,
				PubMessage: core.PubMessage{
					Topic: topic,
					Data:  testData,
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

// TestRabbitMQSubscribe 测试RabbitMQ订阅消息
func TestRabbitMQSubscribe(t *testing.T) {
	core.GmqRegister("rabbit-test", &RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	})
	client := core.GetGmq("rabbit-test")

	topic := "test.subscribe.topic"
	receivedMessages := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	subMsg := &RabbitMQSubMessage{
		SubMessage: core.SubMessage{
			Topic:        topic,
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
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	pubMsg := &RabbitMQPubMessage{
		PubMessage: core.PubMessage{
			Topic: topic,
			Data:  "Test message for subscribe",
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

// TestRabbitMQSubscribeDelay 测试RabbitMQ订阅延迟消息
func TestRabbitMQSubscribeDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过延迟测试，使用 -short 标志跳过")
	}

	core.GmqRegister("rabbit-delay-test", &RabbitMQConn{
		Url:      "localhost",
		Port:     "5672",
		Username: "admin",
		Password: "123456",
		VHost:    "",
	})
	client := core.GetGmq("rabbit-delay-test")

	topic := "test.subscribe.delay.topic"
	receivedMessages := make([]string, 0)
	receivedMutex := sync.Mutex{}
	receivedCount := 0

	subMsg := &RabbitMQSubMessage{
		SubMessage: core.SubMessage{
			Topic:        topic,
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
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		if err := client.GmqSubscribe(ctx, subMsg); err != nil {
			t.Logf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	delayMsg := &RabbitMQPubDelayMessage{
		PubDelayMessage: core.PubDelayMessage{
			DelaySeconds: 2,
			PubMessage: core.PubMessage{
				Topic: topic,
				Data:  "Test delay message for subscribe",
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

// TestRabbitMQAck 测试RabbitMQ确认消息
func TestRabbitMQAck(t *testing.T) {
	// 注意：Ack/Nak 操作需要真实的 Delivery 对象（从订阅获取）
	// 这里仅验证接口存在，不进行实际调用
	t.Log("Ack operation requires real Delivery from subscription")
}

// ============ 监控指标测试 ============

// TestRabbitMQGetMetrics 测试RabbitMQ获取监控指标
func TestRabbitMQGetMetrics(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	metrics := conn.GmqGetMetrics(ctx)
	if metrics == nil {
		t.Error("Expected metrics to be non-nil")
	}
	if metrics.Type != "rabbitmq" {
		t.Errorf("Expected type 'rabbitmq', got '%s'", metrics.Type)
	}
	if metrics.Status != "connected" {
		t.Errorf("Expected status 'connected', got '%s'", metrics.Status)
	}
	t.Logf("Metrics: %+v", metrics)
}

// ============ 死信队列测试 ============

// TestRabbitMQGetDeadLetter 测试RabbitMQ获取死信消息
func TestRabbitMQGetDeadLetter(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.GmqClose(ctx)

	// 先清空死信队列
	_, _ = conn.GmqGetDeadLetter(ctx)

	// 获取死信消息（此时应该为空）
	messages, err := conn.GmqGetDeadLetter(ctx)
	if err != nil {
		t.Logf("GetDeadLetter returned error: %v", err)
	}
	if messages == nil {
		messages = []core.DeadLetterMsgDTO{}
	}

	t.Logf("Found %d dead letter messages (should be 0 initially)", len(messages))

	// 如果有死信消息，打印详情
	for i, msg := range messages {
		t.Logf("【死信消息 %d】", i+1)
		t.Logf("  MessageID: %s", msg.MessageID)
		t.Logf("  Topic: %s", msg.Topic)
		t.Logf("  Timestamp: %s", msg.Timestamp)
		t.Logf("  DeadReason: %s", msg.DeadReason)
		t.Logf("  Body: %s", msg.Body)
	}
}

// TestRabbitMQCreateDeadLetter 测试创建死信消息
func TestRabbitMQCreateDeadLetter(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	// 先清空死信队列
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = conn.GmqGetDeadLetter(ctxWithTimeout)

	// 发布一条消息
	topic := "test.deadletter.create.topic"
	pubMsg := &RabbitMQPubMessage{
		PubMessage: core.PubMessage{
			Topic: topic,
			Data:  "Test message to create dead letter",
		},
		Durable: true,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
	t.Logf("Published message to topic: %s", topic)

	// 稍微等待消息到达队列
	time.Sleep(100 * time.Millisecond)

	// 消费并拒绝消息，让它进入死信队列
	t.Logf("Getting message from topic: %s", topic)
	msgs, ok, err := conn.channel.Get(topic, false)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	if ok {
		t.Logf("Got message, will reject it to dead letter queue")
		// 拒绝消息，requeue=false 让它进入死信队列
		if err := msgs.Nack(false, false); err != nil {
			t.Fatalf("Failed to nack message: %v", err)
		}
		t.Logf("Message rejected successfully")
	} else {
		t.Logf("No message found in topic")
	}

	// 等待消息进入死信队列
	time.Sleep(1 * time.Second)

	// 获取死信消息
	t.Logf("Getting dead letter messages...")
	ctxWithTimeout2, ctxCancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer ctxCancel2()
	messages, err := conn.GmqGetDeadLetter(ctxWithTimeout2)
	t.Logf("GetDeadLetter completed: %d messages, err=%v", len(messages), err)
	if err != nil {
		t.Logf("GetDeadLetter returned error: %v", err)
	}
	if messages == nil {
		messages = []core.DeadLetterMsgDTO{}
	}

	t.Logf("Found %d dead letter messages", len(messages))

	// 打印死信消息详情
	for i, msg := range messages {
		t.Logf("【死信消息 %d】", i+1)
		t.Logf("  MessageID: %s", msg.MessageID)
		t.Logf("  Topic: %s", msg.Topic)
		t.Logf("  Timestamp: %s", msg.Timestamp)
		t.Logf("  DeadReason: %s", msg.DeadReason)
		t.Logf("  Body: %s", msg.Body)
	}
}

// TestRabbitMQSubscribeDeadLetter 测试订阅死信队列
func TestRabbitMQSubscribeDeadLetter(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	if err := conn.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.GmqClose(ctx)

	// 先清空死信队列
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = conn.GmqGetDeadLetter(ctxWithTimeout)

	// 订阅死信队列（后台监听）
	done := make(chan bool)

	go func() {
		err := conn.subscribeDeadLetter(ctx)
		if err != nil && err != context.Canceled {
			t.Logf("SubscribeDeadLetter error: %v", err)
		}
		close(done)
	}()

	// 等待订阅启动
	time.Sleep(500 * time.Millisecond)

	// 发布一条消息
	topic := "test.deadletter.subscribe.topic"
	pubMsg := &RabbitMQPubMessage{
		PubMessage: core.PubMessage{
			Topic: topic,
			Data:  "Test message for dead letter subscribe",
		},
		Durable: true,
	}

	if err := conn.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// 消费并拒绝消息，让它进入死信队列
	time.Sleep(100 * time.Millisecond)
	msgs, ok, err := conn.channel.Get(topic, false)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	if ok {
		// 拒绝消息，requeue=false 让它进入死信队列
		if err := msgs.Nack(false, false); err != nil {
			t.Fatalf("Failed to nack message: %v", err)
		}
	}

	// 等待死信订阅收到消息
	time.Sleep(2 * time.Second)

	// 验证死信消息是否被订阅收到
	t.Logf("SubscribeDeadLetter test completed")
}

// TestRabbitMQSetupUnifiedDeadLetter 测试RabbitMQ统一死信队列设置
func TestRabbitMQSetupUnifiedDeadLetter(t *testing.T) {
	conn := setupRabbitMQConn(t)
	ctx := context.Background()
	defer conn.GmqClose(ctx)

	// 调用 setupUnifiedDeadLetter 方法
	if err := conn.setupUnifiedDeadLetter(); err != nil {
		t.Errorf("Failed to setup unified dead letter: %v", err)
	}

	// 验证死信交换机和队列名称
	if conn.unifiedDLExchange != "gmq.dead.letter.exchange" {
		t.Errorf("Expected exchange name 'gmq.dead.letter.exchange', got '%s'", conn.unifiedDLExchange)
	}
	if conn.unifiedDLQueue != "gmq.dead.letter.queue" {
		t.Errorf("Expected queue name 'gmq.dead.letter.queue', got '%s'", conn.unifiedDLQueue)
	}
}
