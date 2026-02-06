package components

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bjang03/gmq/core"
)

// TestNatsPublish 测试NATS发布单条消息
func TestNatsPublish(t *testing.T) {
	natsClient := &NatsMsg{}

	ctx := context.Background()
	if err := natsClient.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer natsClient.GmqClose(ctx)

	if !natsClient.GmqPing(ctx) {
		t.Error("NATS connection ping failed")
	}

	queueName := "test.queue"
	testMessage := "Hello, NATS!"
	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      testMessage,
		},
	}

	if err := natsClient.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	t.Logf("Successfully published message to queue: %s", queueName)
}

// TestNatsSubscribe 测试NATS订阅消息
func TestNatsSubscribe(t *testing.T) {
	natsClient := &NatsMsg{}

	ctx := context.Background()
	if err := natsClient.GmqConnect(ctx); err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer natsClient.GmqClose(ctx)

	if !natsClient.GmqPing(ctx) {
		t.Error("NATS connection ping failed")
	}

	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	queueName := "test.queue"
	receivedMessages := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	subMsg := &NatsSubMessage{
		SubMessage: core.SubMessage[any]{
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
	}

	sub, err := natsClient.GmqSubscribe(subCtx, subMsg)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer func() {
		if unsub, ok := sub.(interface{ Unsubscribe() error }); ok {
			_ = unsub.Unsubscribe()
		}
	}()

	time.Sleep(100 * time.Millisecond)

	pubMsg := &NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: queueName,
			Data:      "Test message for subscribe",
		},
	}

	if err := natsClient.GmqPublish(ctx, pubMsg); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if len(receivedMessages) != 1 {
			t.Errorf("Expected 1 message, got %d", len(receivedMessages))
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for message")
	}
}
