// Package core provides the core functionality for the GMQ message queue system.
// It includes the unified Gmq interface, proxy wrapper, and plugin registry.
// The proxy layer adds monitoring, retry logic, and connection management.
package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
)

// subMessage wraps subscription message with acknowledgment handler.
// This internal structure is used by the proxy to manage message acknowledgments.
type subMessage struct {
	SubMsg     any                                                        // The original subscription message
	HandleFunc func(ctx context.Context, message *types.AckMessage) error // Wrapped handler function for acknowledgment control
}

// GetSubMsg returns the subscription message
func (m *subMessage) GetSubMsg() any {
	return m.SubMsg
}

// GetAckHandleFunc returns the acknowledgment handler function
func (m *subMessage) GetAckHandleFunc() func(ctx context.Context, message *types.AckMessage) error {
	return m.HandleFunc
}

// GmqProxy message queue proxy wrapper for unified monitoring, retry logic, and connection management.
// It wraps the underlying Gmq implementation to add:
// - Automatic retry with exponential backoff
// - Subscription management and restoration
// - Connection status tracking
// - Structured logging
type GmqProxy struct {
	name         string // proxy/plugin name for identification and logging
	plugin       Gmq    // underlying message queue plugin instance
	pluginUnique GmqUnique
	connected    int32 // connection status: 0=disconnected, 1=connected (atomic access for thread safety)

	subscriptions      sync.Map // active subscription tracking: key=subKey, value=*subscriptionInfo
	subscriptionParams sync.Map // subscription parameter cache: key=subKey, value=*subMessage for restoration
}

// newGmqProxy creates a new proxy wrapper for a given plugin.
// Parameters:
//   - name: plugin name for identification
//   - plugin: underlying message queue implementation
//
// Returns initialized GmqProxy instance
func newGmqProxy(name string, plugin Gmq) *GmqProxy {
	p := &GmqProxy{
		name:   name,
		plugin: plugin,
	}
	return p
}

// validatePublishMsg validates common parameters for publish messages.
// Ensures topic and data are not empty.
// Returns error if validation fails
func validatePublishMsg(msg types.Publish) error {
	if msg.GetTopic() == "" {
		return types.ErrTopicRequired
	}
	if msg.GetData() == nil {
		return types.ErrDataRequired
	}
	return nil
}

// validatePublishDelayMsg validates common parameters for delayed publish messages.
// Ensures topic, data, and delay seconds are valid.
// Returns error if validation fails
func validatePublishDelayMsg(msg types.PublishDelay) error {
	if msg.GetTopic() == "" {
		return types.ErrTopicRequired
	}
	if msg.GetData() == nil {
		return types.ErrDataRequired
	}
	if msg.GetDelaySeconds() <= 0 {
		return types.ErrDelaySecondsRequired
	}
	return nil
}

// executeWithRetry executes an operation with exponential backoff retry logic.
// Returns error if all retry attempts fail
func (p *GmqProxy) executeWithRetry(ctx context.Context, operation string, topic string, op func() error) error {
	logger := utils.LogWithPlugin(p.name)
	var err error

	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		// Wait for connection if not available
		if !p.plugin.GmqPing(ctx) {
			delay := types.MsgRetryDelay * time.Duration(1<<uint(attempt))
			logger.Warn(operation+" waiting for connection", "attempt", attempt, "delay", delay)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Execute operation
		if err = op(); err != nil {
			logger.Error(operation+" failed", "attempt", attempt, "error", err)
			// If connection is still good, don't retry
			if p.plugin.GmqPing(ctx) {
				return err
			}
		} else {
			logger.Info(operation+" success", "attempt", attempt, "topic", topic)
			return nil
		}
	}
	return err
}

// GmqPublish publishes a message with unified monitoring and retry logic.
// Implements exponential backoff retry with configurable attempts and delay.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must implement Publish interface)
//
// Returns error if all retry attempts fail
func (p *GmqProxy) GmqPublish(ctx context.Context, msg types.Publish) error {
	if err := validatePublishMsg(msg); err != nil {
		logger := utils.LogWithPlugin(p.name)
		logger.Error("validate publish message failed", "error", err)
		return err
	}
	return p.executeWithRetry(ctx, "publish", msg.GetTopic(), func() error {
		return p.plugin.GmqPublish(ctx, msg)
	})
}

// GmqPublishDelay publishes a delayed message with unified monitoring and retry logic.
// The message will be delivered after the specified delay period.
// Implements exponential backoff retry with configurable attempts and delay.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: delayed message to publish (must implement PublishDelay interface)
//
// Returns error if all retry attempts fail
func (p *GmqProxy) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error {
	if err := validatePublishDelayMsg(msg); err != nil {
		logger := utils.LogWithPlugin(p.name)
		logger.Error("validate publish delay message failed", "error", err)
		return err
	}
	return p.executeWithRetry(ctx, "publish delay", msg.GetTopic(), func() error {
		return p.pluginUnique.GmqPublishDelay(ctx, msg)
	})
}

// validateSubscribeMsg validates common parameters for subscribe messages.
// Ensures topic, consumer name, fetch count, and handler function are valid.
// Returns error if validation fails
func validateSubscribeMsg(msg *types.SubMessage) error {
	if msg.Topic == "" {
		return types.ErrTopicRequired
	}
	if msg.ConsumerName == "" {
		return types.ErrConsumerNameRequired
	}
	if msg.FetchCount <= 0 {
		return types.ErrFetchCountRequired
	}
	if msg.HandleFunc == nil {
		return types.ErrHandleFuncRequired
	}
	return nil
}

// GmqSubscribe subscribes to messages from a topic with unified monitoring and retry logic.
// Implements exponential backoff retry during subscription establishment.
// Prevents duplicate subscriptions for the same topic and consumer.
// This method is non-blocking - it starts a background goroutine to handle the subscription.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: subscription message containing topic, consumer, and handler
//
// Returns error if subscription fails
func (p *GmqProxy) GmqSubscribe(ctx context.Context, msg types.Subscribe) (err error) {
	message, ok := msg.GetSubMsg().(*types.SubMessage)
	if !ok {
		return fmt.Errorf("%w: expected *types.SubMessage", types.ErrInvalidMessageType)
	}
	if err = validateSubscribeMsg(message); err != nil {
		return err
	}
	sub := new(subMessage)
	sub.SubMsg = msg
	sub.HandleFunc = p.wrapHandleFunc(message.HandleFunc, message.AutoAck)
	message.HandleFunc = nil
	subKey := message.Topic + ":" + message.ConsumerName

	// Check for duplicate subscription
	_, loaded := p.subscriptions.Load(subKey)
	if loaded {
		logger := utils.LogWithPlugin(p.name)
		logger.Warn("already subscribed to topic", "topic", message.Topic, "consumer", message.ConsumerName)
		return fmt.Errorf("%w: topic=%s, consumer=%s", types.ErrSubscriptionAlreadyExists, message.Topic, message.ConsumerName)
	}

	// Create a dedicated context for this subscription
	subCtx, cancel := context.WithCancel(context.Background())

	// Try to store the subscription info first (optimistic locking)
	_, loaded = p.subscriptions.LoadOrStore(subKey, cancel)
	if loaded {
		cancel() // Cancel the context we created
		logger := utils.LogWithPlugin(p.name)
		logger.Warn("already subscribed to topic", "topic", message.Topic, "consumer", message.ConsumerName)
		return fmt.Errorf("%w: topic=%s, consumer=%s", types.ErrSubscriptionAlreadyExists, message.Topic, message.ConsumerName)
	}

	// Store subscription params for potential reconnection
	p.subscriptionParams.Store(subKey, sub)

	// Start subscription in background goroutine
	go p.runSubscription(subCtx, subKey, sub, message)

	logger := utils.LogWithPlugin(p.name)
	logger.Info("subscribe success, started background goroutine", "subKey", subKey)
	return nil
}

// runSubscription runs the actual subscription in a background goroutine.
// It handles retry logic and cleanup when the subscription ends.
func (p *GmqProxy) runSubscription(ctx context.Context, subKey string, sub *subMessage, message *types.SubMessage) {
	logger := utils.LogWithPlugin(p.name)

	err := p.executeWithRetry(ctx, "subscribe", message.Topic, func() error {
		return p.plugin.GmqSubscribe(ctx, sub)
	})

	if err != nil {
		logger.Error("subscription failed after retries", "subKey", subKey, "error", err)
		p.subscriptions.Delete(subKey)
		p.subscriptionParams.Delete(subKey)
		return
	}

	// Wait for context cancellation (subscription ends)
	<-ctx.Done()
	logger.Debug("subscription goroutine exiting", "subKey", subKey, "reason", ctx.Err())
}

// wrapHandleFunc wraps the user's HandleFunc to control ACK at proxy layer.
// It manages message acknowledgment based on the AutoAck setting:
// - When AutoAck=true: acknowledges immediately before processing
// - When AutoAck=false: acknowledges after successful processing, rejects on failure
// Includes panic recovery to prevent a single message handler crash from affecting the entire subscription.
// Parameters:
//   - originalFunc: the user-provided message handler
//   - autoAck: whether to auto-acknowledge before processing
//
// Returns wrapped handler function
func (p *GmqProxy) wrapHandleFunc(originalFunc func(ctx context.Context, message any) error, autoAck bool) func(ctx context.Context, message *types.AckMessage) error {
	return func(ctx context.Context, message *types.AckMessage) (err error) {
		// panic recovery to prevent handler crash from terminating the subscription
		defer func() {
			if r := recover(); r != nil {
				logger := utils.LogWithPlugin(p.name)
				logger.Error("message handler panic recovered", "panic", r)
				err = fmt.Errorf("handler panic: %v", r)
			}
		}()

		if autoAck {
			err = p.plugin.GmqAck(ctx, message)
			if err != nil {
				return err
			}
		}
		err = originalFunc(ctx, message.MessageData)
		if !autoAck {
			if err == nil {
				err = p.plugin.GmqAck(ctx, message)
				if err != nil {
					return err
				}
			} else {
				err = p.pluginUnique.GmqNak(ctx, message)
				if err != nil {
					return err
				}
			}
		}
		return err
	}
}

// clearSubscriptions clears all active subscriptions when shutting down or reconnecting.
// It cancels all subscription goroutines and cleans up the subscription maps.
func (p *GmqProxy) clearSubscriptions() {
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
			cancel() // Cancel the subscription context to stop the goroutine
		}
		p.subscriptions.Delete(subKey)
		p.subscriptionParams.Delete(subKey)
		return true
	})
}

// restoreSubscriptions re-establishes all subscriptions after a successful reconnection.
func (p *GmqProxy) restoreSubscriptions() {
	logger := utils.LogWithPlugin(p.name)

	// Cancel all existing subscription goroutines
	p.subscriptions.Range(func(key, value any) bool {
		if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
			cancel()
		}
		p.subscriptions.Delete(key.(string))
		return true
	})

	// Re-establish all subscriptions in new goroutines
	p.subscriptionParams.Range(func(key, value any) bool {
		subKey := key.(string)
		info, ok := value.(*subMessage)
		if !ok {
			logger.Error("invalid subMessage info", "key", subKey)
			return true
		}

		subMsg, ok := info.SubMsg.(types.Subscribe)
		if !ok {
			logger.Error("invalid SubMsg type", "key", subKey)
			return true
		}
		originalMsg, ok := subMsg.GetSubMsg().(*types.SubMessage)
		if !ok {
			logger.Error("invalid original message type", "key", subKey)
			return true
		}

		subCtx, cancel := context.WithCancel(context.Background())
		p.subscriptions.Store(subKey, cancel)
		go p.runSubscription(subCtx, subKey, info, originalMsg)

		logger.Info("restored subscription", "subKey", subKey)
		return true
	})
}

// GmqPing checks if the connection is alive.
// It first checks the local connection status flag for efficiency,
// then verifies the actual connection with the underlying plugin.
// Returns true if connected and alive, false otherwise
func (p *GmqProxy) GmqPing(ctx context.Context) bool {
	// proxy layer validation: check if connected
	if atomic.LoadInt32(&p.connected) == 0 {
		return false
	}
	// check if connection is valid
	return p.plugin.GmqPing(ctx)
}

// GmqGetConn retrieves the underlying message queue connection object.
// Returns the raw connection object (type varies by implementation)
func (p *GmqProxy) GmqGetConn(ctx context.Context) any {
	return p.plugin.GmqGetConn(ctx)
}

// GmqConnect establishes a connection to the message queue server.
// On successful connection, sets the internal connected flag to 1.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - cfg: connection configuration parameters
//
// Returns error if connection fails
func (p *GmqProxy) GmqConnect(ctx context.Context, cfg map[string]any) error {
	err := p.plugin.GmqConnect(ctx, cfg)
	if err == nil {
		atomic.StoreInt32(&p.connected, 1)
		logger := utils.LogWithPlugin(p.name)
		logger.Info("connection established successfully")
	}
	return err
}

// GmqAck acknowledges successful processing of a message.
// This tells the message queue that the message has been processed successfully
// and can be removed from the queue.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if acknowledgment fails
func (p *GmqProxy) GmqAck(ctx context.Context, msg *types.AckMessage) error {
	return p.plugin.GmqAck(ctx, msg)
}

// GmqNak negatively acknowledges a message, indicating processing failure.
// The message will be re-queued or sent to dead letter queue depending on configuration.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if negative acknowledgment fails
func (p *GmqProxy) GmqNak(ctx context.Context, msg *types.AckMessage) error {
	return p.pluginUnique.GmqNak(ctx, msg)
}

// GmqClose closes the connection and cleans up all subscriptions.
// Sets the internal connected flag to 0 and stops all background goroutines.
// Parameters:
//   - ctx: context for timeout/cancellation control
//
// Returns error if close fails
func (p *GmqProxy) GmqClose(ctx context.Context) error {
	// clear all subscriptions before closing
	p.clearSubscriptions()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}
