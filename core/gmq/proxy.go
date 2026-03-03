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

// anonConsumerCounter atomic counter for generating unique subscription keys for anonymous consumers
var anonConsumerCounter atomic.Int64

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
	name      string // proxy/plugin name for identification and logging
	plugin    Gmq    // underlying message queue plugin instance
	connected int32  // connection status: 0=disconnected, 1=connected (atomic access for thread safety)

	subscriptions      sync.Map // active subscription tracking: key=subKey, value=empty struct for fast lookup
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

// GmqPublish publishes a message with unified monitoring and retry logic.
// Implements exponential backoff retry with configurable attempts and delay.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must implement Publish interface)
//
// Returns error if all retry attempts fail
func (p *GmqProxy) GmqPublish(ctx context.Context, msg types.Publish) error {
	var err error
	if err = validatePublishMsg(msg); err != nil {
		logger := utils.LogWithPlugin(p.name)
		logger.Error("validate publish message failed", "error", err)
		return err
	}
	logger := utils.LogWithPlugin(p.name)
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break
			}
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			logger.Warn("publish attempt ping failed, wait", "attempt", attempt, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				logger.Error("publish attempt context canceled", "attempt", attempt, "error", err)
				return err
			}
		}
		if err = p.plugin.GmqPublish(ctx, msg); err != nil {
			logger.Error("publish attempt failed", "attempt", attempt, "error", err)
			if attempt == types.MsgRetryDeliver-1 {
				logger.Error("all publish attempts failed", "error", err)
			}
		} else {
			logger.Info("publish attempt success", "attempt", attempt, "topic", msg.GetTopic())
			return nil
		}
	}
	return err
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

// GmqPublishDelay publishes a delayed message with unified monitoring and retry logic.
// The message will be delivered after the specified delay period.
// Implements exponential backoff retry with configurable attempts and delay.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: delayed message to publish (must implement PublishDelay interface)
//
// Returns error if all retry attempts fail
func (p *GmqProxy) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error {
	var err error
	if err = validatePublishDelayMsg(msg); err != nil {
		logger := utils.LogWithPlugin(p.name)
		logger.Error("validate publish delay message failed", "error", err)
		return err
	}
	logger := utils.LogWithPlugin(p.name)
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break
			}
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			logger.Warn("publish delay attempt ping failed, wait", "attempt", attempt, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				logger.Error("publish delay attempt context canceled", "attempt", attempt, "error", err)
				return err
			}
		}
		if err = p.plugin.GmqPublishDelay(ctx, msg); err != nil {
			logger.Error("publish delay attempt failed", "attempt", attempt, "error", err)
			if attempt == types.MsgRetryDeliver-1 {
				logger.Error("all publish delay attempts failed", "error", err)
			}
		} else {
			logger.Info("publish delay attempt success", "attempt", attempt, "topic", msg.GetTopic(), "delay_seconds", msg.GetDelaySeconds())
			return nil
		}
	}
	return err
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
				err = p.plugin.GmqNak(ctx, message)
				if err != nil {
					return err
				}
			}
		}
		return err
	}
}

// GmqSubscribe subscribes to messages from a topic with unified monitoring and retry logic.
// Implements exponential backoff retry during subscription establishment.
// Prevents duplicate subscriptions for the same topic and consumer.
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
	subKey := p.getSubKey(message.Topic, message.ConsumerName)
	_, loaded := p.subscriptions.LoadOrStore(subKey, struct{}{})
	if loaded {
		logger := utils.LogWithPlugin(p.name)
		logger.Warn("already subscribed to topic", "topic", message.Topic, "consumer", message.ConsumerName)
		return fmt.Errorf("%w: topic=%s, consumer=%s", types.ErrSubscriptionAlreadyExists, message.Topic, message.ConsumerName)
	}
	// use named return value to ensure defer sees the final error value
	defer func() {
		if err != nil {
			p.subscriptions.Delete(subKey)
			p.subscriptionParams.Delete(subKey)
			logger := utils.LogWithPlugin(p.name)
			logger.Warn("subscribe failed, clean slot", "subKey", subKey, "error", err)
		}
	}()
	logger := utils.LogWithPlugin(p.name)
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break
			}
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			logger.Warn("subscribe attempt ping failed, wait", "attempt", attempt, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				logger.Error("subscribe attempt context canceled", "attempt", attempt, "error", err)
				return err
			}
		}
		err = p.plugin.GmqSubscribe(ctx, sub)
		if err != nil {
			logger.Error("subscribe attempt failed", "attempt", attempt, "error", err)
			if attempt == types.MsgRetryDeliver-1 {
				logger.Error("all subscribe attempts failed", "attempts", types.MsgRetryDeliver, "error", err)
			}
			continue
		}
		logger.Info("subscribe attempt success", "attempt", attempt, "topic", message.Topic)
		break
	}
	if err != nil {
		return err
	}
	p.subscriptionParams.Store(subKey, sub)
	logger.Info("subscribe success, save subKey", "subKey", subKey)
	return nil
}

// clearSubscriptions clears all active subscriptions when shutting down or reconnecting.
// It unsubscribes from all registered topics and cleans up the subscription maps.
func (p *GmqProxy) clearSubscriptions() {
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		subObj := value
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		p.subscriptions.Delete(subKey)
		p.subscriptionParams.Delete(subKey)
		return true
	})
}

// restoreSubscriptions re-establishes all subscriptions after a successful reconnection.
// It iterates through cached subscription parameters and attempts to resubscribe
// with the original configuration, implementing retry logic for each subscription.
func (p *GmqProxy) restoreSubscriptions() {
	logger := utils.LogWithPlugin(p.name)
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		subObj := value
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			if err := closer.Unsubscribe(); err != nil {
				logger.Warn("failed to unsubscribe old subscription", "key", subKey, "error", err)
			}
		}
		p.subscriptions.Delete(subKey)
		return true
	})
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	defer restoreCancel()
	logger = utils.LogWithPlugin(p.name)
	p.subscriptionParams.Range(func(key, value any) bool {
		subKey := key.(string)
		info, ok := value.(*subMessage)
		if !ok {
			logger.Error("invalid subMessage info", "key", subKey)
			return true
		}
		var err error
		for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
			if restoreCtx.Err() != nil {
				logger.Error("restore subscription canceled", "key", subKey, "error", restoreCtx.Err())
				return false
			}
			if attempt > 0 || !p.plugin.GmqPing(restoreCtx) {
				if attempt > 0 && p.plugin.GmqPing(restoreCtx) {
					break
				}
				delay := types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
				logger.Warn("restore subscription ping failed, wait", "attempt", attempt, "delay", delay, "key", subKey)
				select {
				case <-time.After(delay):
				case <-restoreCtx.Done():
					return false
				}
			}
			err = p.plugin.GmqSubscribe(restoreCtx, info)
			if err == nil {
				logger.Info("restore subscription success", "key", subKey, "attempt", attempt)
				break
			}
			logger.Error("restore subscription attempt failed", "attempt", attempt, "key", subKey, "error", err)
			if attempt == types.MsgRetryDeliver-1 {
				logger.Error("restore subscription all attempts failed", "key", subKey)
			}
		}
		if err == nil {
			p.subscriptions.Store(subKey, struct{}{})
		}
		return true
	})
}

// getSubKey generates a unique subscription key for tracking subscriptions.
// For named consumers, uses "topic:consumerName" format.
// For anonymous consumers, uses "topic:anon-{counter}" format with atomic counter.
// Parameters:
//   - topic: the subscription topic
//   - consumerName: the consumer name (empty for anonymous consumers)
//
// Returns a unique subscription key string
func (p *GmqProxy) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	// use atomic counter to generate unique suffix to avoid conflicts
	counter := anonConsumerCounter.Add(1)
	return fmt.Sprintf("%s:anon-%d", topic, counter)
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
	return p.plugin.GmqNak(ctx, msg)
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
