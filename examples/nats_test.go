package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	gmq "github.com/bjang03/gmq/core/gmq"
	mq2 "github.com/bjang03/gmq/mq"
	"github.com/bjang03/gmq/types"
	"github.com/nats-io/nats.go"
)

var natsRegisterName = "nats-test"

// NATS register - don't use defer Shutdown in test helper, call it explicitly in each test
func natsRegister(ctx context.Context) {
	gmq.GmqRegister(natsRegisterName, &mq2.NatsConn{
		NatsConfig: mq2.NatsConfig{
			Addr: "localhost",
			Port: "4222",
		},
	})

	// Wait for connection to be established
	time.Sleep(2 * time.Second)
}

// TestNatsPublishDelayMultiple tests NATS publish multiple delay messages to verify all messages are consumed
// This test verifies the fix for the issue where only the last message was consumed when sending multiple delayed messages
func TestNatsPublishDelayMultiple(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)
	defer gmq.Shutdown(ctx)

	getGmq := gmq.GetGmq(natsRegisterName)
	if getGmq == nil {
		t.Fatal("无法获取 NATS GMQ 实例，插件可能未正确注册")
	}

	// Check if connection is ready
	if !getGmq.GmqPing(ctx) {
		t.Fatal("NATS 连接未就绪，请检查 NATS 服务器是否运行")
	}

	// Cleanup old stream before test
	connMap := getGmq.GmqGetConn(ctx).(map[string]any)
	js, ok := connMap["js"].(nats.JetStreamContext)
	if ok {
		streamName := "delay_file_test-delay-multiple-topic"
		if err := js.DeleteStream(streamName); err != nil {
			t.Logf("流 %s 不存在或删除失败（这是正常的）: %v", streamName, err)
		} else {
			t.Logf("已删除旧流：%s", streamName)
		}
		// Wait a bit for stream deletion to take effect
		time.Sleep(500 * time.Millisecond)
	}

	topic := "test-delay-multiple-topic"
	consumerName := "test-delay-multiple-consumer"

	// Channel to collect received messages
	receivedMessages := make(chan int, 10)

	// Start consumer first
	go func() {
		subMsg := &mq2.NatsSubMessage{
			SubMessage: types.SubMessage{
				Topic:        topic,
				ConsumerName: consumerName,
				AutoAck:      true,
				FetchCount:   10,
				HandleFunc: func(ctx context.Context, message any) error {
					data := message.([]byte)
					var msgData map[string]interface{}
					if err := json.Unmarshal(data, &msgData); err != nil {
						t.Errorf("消息反序列化失败：%v", err)
						return err
					}

					index := int(msgData["index"].(float64))
					t.Logf("在 %v 收到消息，索引：%d", time.Now(), index)
					receivedMessages <- index
					return nil
				},
			},
			Durable:    true,
			IsDelayMsg: true,
		}
		if _, err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
			t.Errorf("订阅失败：%v", err)
			return
		}
	}()

	// Wait a bit for subscription to be ready
	time.Sleep(1 * time.Second)

	// Send 5 delayed messages with DIFFERENT delay times (2, 3, 4, 5, 6 seconds)
	totalMessages := 5
	baseDelaySeconds := 2

	t.Logf("在 %v 发送 %d 条不同延迟时间的消息", time.Now(), totalMessages)
	for i := 1; i <= totalMessages; i++ {
		testData := map[string]interface{}{
			"message": fmt.Sprintf("Test delay message %d", i),
			"index":   i,
		}

		// Each message has a different delay: 2s, 3s, 4s, 5s, 6s
		delaySeconds := baseDelaySeconds + (i - 1)

		delayMsg := &mq2.NatsPubDelayMessage{
			PubDelayMessage: types.PubDelayMessage{
				DelaySeconds: 10,
				PubMessage: types.PubMessage{
					Topic: topic,
					Data:  testData,
				},
			},
			Durable: true,
		}

		if err := getGmq.GmqPublishDelay(ctx, delayMsg); err != nil {
			t.Fatalf("发送延迟消息 %d 失败：%v", i, err)
		}
		t.Logf("已发送消息 %d，延迟 %d 秒，时间：%v", i, delaySeconds, time.Now())
		time.Sleep(5 * time.Second) // Small delay between sends to simulate real scenario
	}

	// Collect received messages with timeout (use max delay + buffer)
	receivedCount := 0
	timeout := time.After(time.Duration(baseDelaySeconds+totalMessages+3) * time.Second)

	for receivedCount < totalMessages {
		select {
		case idx := <-receivedMessages:
			receivedCount++
			t.Logf("已收集消息 %d/%d（索引：%d）", receivedCount, totalMessages, idx)
			if receivedCount >= totalMessages {
				goto verification
			}
		case <-timeout:
			t.Logf("等待消息超时")
			goto verification
		}
	}

verification:
	// Verify all messages were received
	t.Logf("测试完成：收到 %d/%d 条消息", receivedCount, totalMessages)
	if receivedCount != totalMessages {
		t.Errorf("预期收到 %d 条消息，但实际收到 %d 条。这表明防抖问题尚未修复！", totalMessages, receivedCount)
		t.Fail()
	} else {
		t.Logf("成功：所有 %d 条消息都已正确接收。防抖问题已修复！", totalMessages)
	}
}

// ============ Message Publish Tests ============

// TestNatsPublish tests NATS publish message
func TestNatsPublish(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)
	defer gmq.Shutdown(ctx)

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
	defer gmq.Shutdown(ctx)

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
	defer gmq.Shutdown(ctx)

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
	defer gmq.Shutdown(ctx)

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
	defer gmq.Shutdown(ctx)

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
	if _, err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}

// TestNatsSubscribeDelay tests NATS subscribe delay message
func TestNatsSubscribeDelay(t *testing.T) {
	ctx := context.Background()
	natsRegister(ctx)
	defer gmq.Shutdown(ctx)

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
	if _, err := getGmq.GmqSubscribe(ctx, subMsg); err != nil {
		t.Logf("Subscribe error: %v", err)
	}
}
