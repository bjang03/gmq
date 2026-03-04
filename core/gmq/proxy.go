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

// ============================================================
// State Constants
// ============================================================

const (
	connectionDisconnected = 0 // Connection state: disconnected
	connectionConnected    = 1 // Connection state: connected

	subscribeDisconnected = 0 // Subscription state: disconnected
	subscribeConnected    = 1 // Subscription state: connected
)

// ============================================================
// Internal Types
// ============================================================

// subMessage wraps subscription message and acknowledgment handler
// The proxy layer uses this structure to manage message acknowledgment
type subMessage struct {
	SubMsg     any                                                        // Original subscription message
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

// ============================================================
// Subscribe Monitor
// ============================================================

// subscribeMonitor manages subscription monitoring and recovery logic
// Responsibilities: Periodically check connection status, re-establish subscriptions after connection recovery
type subscribeMonitor struct {
	name     string        // Monitor name (used for logging)
	proxy    *GmqProxy     // Associated proxy instance
	started  int32         // Start state: 0=not started, 1=started
	stopped  int32         // Stop state: 0=running, 1=stopped
	stopCh   chan struct{} // Stop signal channel
	interval time.Duration // Monitoring check interval
	logger   *utils.Logger // Cached logger instance to avoid repeated creation
}

// newSubscribeMonitor creates a new subscription monitor
func newSubscribeMonitor(name string, proxy *GmqProxy) *subscribeMonitor {
	if proxy == nil {
		panic(fmt.Sprintf("proxy cannot be nil for %s", name))
	}
	return &subscribeMonitor{
		name:     name,
		proxy:    proxy,
		stopCh:   make(chan struct{}),
		interval: 10 * time.Second,                   // Default 10 seconds
		logger:   utils.GetLogger().WithPlugin(name), // Cache logger instance
	}
}

// Start starts monitoring (thread-safe, idempotent)
// Returns true if started successfully, false otherwise
func (m *subscribeMonitor) Start(subscribeCount int) bool {
	// Already stopped, do not allow restart
	if atomic.LoadInt32(&m.stopped) == 1 {
		return false
	}
	// No subscriptions, no need to start
	if subscribeCount == 0 {
		return false
	}
	// Already started, do not start again (use CAS for atomicity)
	if !atomic.CompareAndSwapInt32(&m.started, 0, 1) {
		return false
	}

	// Ensure stop channel is available (stopCh is nil after Stop, needs to be recreated)
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}

	go m.run()
	return true
}

// Stop stops monitoring (thread-safe, prevents duplicate stops)
func (m *subscribeMonitor) Stop() {
	// Use CAS to prevent duplicate stops
	if !atomic.CompareAndSwapInt32(&m.stopped, 0, 1) {
		return
	}
	// If started, close the stop channel
	if atomic.CompareAndSwapInt32(&m.started, 1, 0) {
		close(m.stopCh)
		m.stopCh = nil
	}
}

// Reset resets the stop state to allow restart
func (m *subscribeMonitor) Reset() {
	atomic.StoreInt32(&m.stopped, 0)
	atomic.StoreInt32(&m.started, 0)
	// Recreate stop channel to allow restart
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
}

// run executes the monitoring loop
func (m *subscribeMonitor) run() {
	m.logger.Info("subscribe monitor started")

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.logger.Info("subscribe monitor stopped")
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

// tick performs periodic monitoring checks
func (m *subscribeMonitor) tick() {
	// Stop monitor when there are no subscriptions
	if m.proxy.getSubscribeCount() == 0 {
		m.logger.Info("no subscribe, monitor stopping")
		m.Stop()
		return
	}

	// Attempt to restore subscriptions when connection is disconnected
	if atomic.LoadInt32(&m.proxy.subscribed) == subscribeDisconnected {
		if m.proxy.plugin.GmqPing(context.Background()) {
			m.proxy.restoreSubscribe()
		}
	}
}

// ============================================================
// Proxy Wrapper
// ============================================================

// GmqProxy is a message queue proxy wrapper that provides unified monitoring,
// retry logic, and connection management. Wraps underlying implementation to provide:
// - Automatic retry (exponential backoff)
// - Subscription management and recovery
// - Connection state tracking
// - Structured logging
type GmqProxy struct {
	// Basic fields
	name         string    // Proxy/plugin name
	plugin       Gmq       // Underlying message queue plugin instance
	pluginUnique GmqUnique // Plugin-specific interface (delayed publish, negative acknowledgment)

	// Connection states
	connected  int32 // Connection state: 0=disconnected, 1=connected (atomic operation)
	subscribed int32 // Subscription state: 0=disconnected, 1=subscribed (atomic operation)

	// Subscription management
	subscribe       sync.Map // Active subscription tracking: key=subKey, value=context.CancelFunc
	subscribeParams sync.Map // Subscription parameter cache: key=subKey, value=*subMessage (for recovery)

	// Monitor
	monitor *subscribeMonitor // Subscription monitor
}

// setSubscribedState sets the subscription state (avoids closure escape)
func (p *GmqProxy) setSubscribedState(subscribed bool) {
	if subscribed {
		atomic.StoreInt32(&p.subscribed, subscribeConnected)
	} else {
		atomic.StoreInt32(&p.subscribed, subscribeDisconnected)
	}
}

// newGmqProxy creates a new proxy wrapper
func newGmqProxy(name string, plugin Gmq) *GmqProxy {
	p := &GmqProxy{
		name:   name,
		plugin: plugin,
	}

	// Create monitor
	p.monitor = newSubscribeMonitor(name, p)

	// Set connection state callback (use method reference instead of closure to avoid capturing entire struct)
	if plugin == nil {
		panic(fmt.Sprintf("plugin cannot be nil for %s", name))
	}
	if stateSetter, ok := plugin.(GmqStateSetter); ok {
		stateSetter.SetSubscribedSetter(p.setSubscribedState)
	}

	return p
}

// ============================================================
// Helper Methods
// ============================================================

// getSubscribeCount returns the current number of subscriptions
func (p *GmqProxy) getSubscribeCount() int {
	count := 0
	p.subscribeParams.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// isConnected checks if the connection is active
func (p *GmqProxy) isConnected() bool {
	return atomic.LoadInt32(&p.connected) == connectionConnected
}

// isSubscribed checks if the subscription is active
func (p *GmqProxy) isSubscribed() bool {
	return atomic.LoadInt32(&p.subscribed) == subscribeConnected
}

// setConnected sets the connection state
func (p *GmqProxy) setConnected(connected bool) {
	if connected {
		atomic.StoreInt32(&p.connected, connectionConnected)
	} else {
		atomic.StoreInt32(&p.connected, connectionDisconnected)
	}
}

// ============================================================
// Message Validation
// ============================================================

// validatePublishMsg validates the publish message
func validatePublishMsg(msg types.Publish) error {
	if msg.GetTopic() == "" {
		return types.ErrTopicRequired
	}
	if msg.GetData() == nil {
		return types.ErrDataRequired
	}
	return nil
}

// validatePublishDelayMsg validates the delayed publish message
func validatePublishDelayMsg(msg types.PublishDelay) error {
	if err := validatePublishMsg(msg); err != nil {
		return err
	}
	if msg.GetDelaySeconds() <= 0 {
		return types.ErrDelaySecondsRequired
	}
	return nil
}

// validateSubscribeMsg validates the subscription message
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

// ============================================================
// Retry Executor
// ============================================================

// executeWithRetry executes an operation with exponential backoff retry logic
func (p *GmqProxy) executeWithRetry(ctx context.Context, operation, topic string, op func() error) error {
	logger := utils.GetLogger().WithPlugin(p.name)
	var err error

	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		// Wait for connection to be available
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
			logger.Error(operation+" failed", "attempt", attempt, "topic", topic, "error", err)
			if p.plugin.GmqPing(ctx) {
				return err
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, types.MsgRetryDeliver, err)
}

// ============================================================
// Connection Management
// ============================================================

// GmqConnect establishes a connection with the message queue server
func (p *GmqProxy) GmqConnect(ctx context.Context, cfg map[string]any) error {
	err := p.plugin.GmqConnect(ctx, cfg)
	if err == nil {
		p.setConnected(true)
		logger := utils.GetLogger().WithPlugin(p.name)
		logger.Info("connection established successfully")
	}
	return err
}

// GmqPing checks if the connection is alive
func (p *GmqProxy) GmqPing(ctx context.Context) bool {
	if !p.isConnected() {
		return false
	}
	return p.plugin.GmqPing(ctx)
}

// GmqGetConn returns the underlying message queue connection object
func (p *GmqProxy) GmqGetConn(ctx context.Context) any {
	return p.plugin.GmqGetConn(ctx)
}

// GmqClose closes the connection and cleans up all subscriptions
func (p *GmqProxy) GmqClose(ctx context.Context) error {
	// Stop monitor
	p.monitor.Stop()

	// Clear all subscriptions
	p.clearSubscribe()

	// Close underlying connection
	err := p.plugin.GmqClose(ctx)

	// Reset states
	atomic.StoreInt32(&p.connected, connectionDisconnected)
	p.setSubscribedState(false)

	// Reset monitor stop state to allow subsequent reconnection
	p.monitor.Reset()

	return err
}

// ============================================================
// Message Publishing
// ============================================================

// GmqPublish publishes a message with unified monitoring and retry logic
func (p *GmqProxy) GmqPublish(ctx context.Context, msg types.Publish) error {
	if err := validatePublishMsg(msg); err != nil {
		logger := utils.GetLogger().WithPlugin(p.name)
		logger.Error("validate publish message failed", "error", err)
		return fmt.Errorf("validate publish message failed: %w", err)
	}
	if p.plugin == nil {
		logger := utils.GetLogger().WithPlugin(p.name)
		logger.Error("GmqPublish not supported", "plugin", p.name)
		return fmt.Errorf("GmqPublish not implemented for plugin %s", p.name)
	}
	err := p.executeWithRetry(ctx, "publish", msg.GetTopic(), func() error {
		return p.plugin.GmqPublish(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}
	logger := utils.GetLogger().WithPlugin(p.name)
	logger.Info("publish success")
	return nil
}

// GmqPublishDelay publishes a delayed message
func (p *GmqProxy) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error {
	if err := validatePublishDelayMsg(msg); err != nil {
		logger := utils.GetLogger().WithPlugin(p.name)
		logger.Error("validate publish delay message failed", "error", err)
		return fmt.Errorf("validate publish delay message failed: %w", err)
	}
	if p.pluginUnique == nil {
		logger := utils.GetLogger().WithPlugin(p.name)
		logger.Error("GmqPublishDelay not supported", "plugin", p.name)
		return fmt.Errorf("GmqPublishDelay not implemented for plugin %s", p.name)
	}
	err := p.executeWithRetry(ctx, "publish delay", msg.GetTopic(), func() error {
		return p.pluginUnique.GmqPublishDelay(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}
	logger := utils.GetLogger().WithPlugin(p.name)
	logger.Info("publish delay success")
	return nil
}

// ============================================================
// Message Subscribe
// ============================================================

// GmqSubscribe subscribes to messages with unified monitoring and retry logic
// This method is non-blocking - it starts a background goroutine to handle the subscription
func (p *GmqProxy) GmqSubscribe(ctx context.Context, msg types.Subscribe) error {
	logger := utils.GetLogger().WithPlugin(p.name)

	// Extract and validate subscription message
	message, ok := msg.GetSubMsg().(*types.SubMessage)
	if !ok {
		logger.Error(types.ErrInvalidMessageType.Error(), "expected", "*types.SubMessage", "got", msg.GetSubMsg())
		return fmt.Errorf("%w: expected *types.SubMessage", types.ErrInvalidMessageType)
	}

	if err := validateSubscribeMsg(message); err != nil {
		logger.Error("validate subscribe message failed", "error", err)
		return fmt.Errorf("validate subscribe message failed: %w", err)
	}

	// Wrap handler
	sub := &subMessage{
		SubMsg:     msg,
		HandleFunc: p.wrapHandleFunc(message.HandleFunc, message.AutoAck),
	}
	message.HandleFunc = nil // Avoid duplicate reference

	subKey := message.Topic + ":" + message.ConsumerName

	// Check for duplicate subscription
	if _, loaded := p.subscribe.Load(subKey); loaded {
		logger.Warn("already subscribed to topic", "topic", message.Topic, "consumer", message.ConsumerName)
		return fmt.Errorf("%w: topic=%s, consumer=%s", types.ErrSubscriptionAlreadyExists, message.Topic, message.ConsumerName)
	}

	// Create subscription-specific context
	subCtx, cancel := context.WithCancel(context.Background())

	// Optimistic locking: store subscription info first
	_, loaded := p.subscribe.LoadOrStore(subKey, cancel)
	if loaded {
		cancel() // Cancel created context to prevent leak
		logger.Warn("already subscribed to topic", "topic", message.Topic, "consumer", message.ConsumerName)
		return fmt.Errorf("%w: topic=%s, consumer=%s", types.ErrSubscriptionAlreadyExists, message.Topic, message.ConsumerName)
	}

	// Store subscription parameters for reconnection recovery
	p.subscribeParams.Store(subKey, sub)

	// Start background subscription goroutine
	go p.runSubscribe(subCtx, subKey, sub, message)

	// Start monitor (if needed)
	p.monitor.Start(p.getSubscribeCount())

	return nil
}

// runSubscribe runs the actual subscription in a background goroutine
func (p *GmqProxy) runSubscribe(ctx context.Context, subKey string, sub *subMessage, message *types.SubMessage) {
	logger := utils.GetLogger().WithPlugin(p.name)

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			logger.Error("subscribe goroutine panic recovered", "subKey", subKey, "panic", r)
			p.cleanupSubscribe(subKey)
		}
	}()

	// Check if subscription has been cleaned up (avoid duplicate subscription)
	if _, exists := p.subscribe.Load(subKey); !exists {
		logger.Warn("subscribe already cancelled, exiting", "subKey", subKey)
		return
	}
	// Set subscription flag
	p.setSubscribedState(true)
	// Execute subscription with retry
	err := p.executeWithRetry(ctx, "subscribe", message.Topic, func() error {
		return p.plugin.GmqSubscribe(ctx, sub)
	})

	if err != nil {
		logger.Error("subscribe failed after retries", "subKey", subKey, "error", err)
		p.cleanupSubscribe(subKey)
		return
	}
	// Wait for context cancellation (subscription ends)
	<-ctx.Done()
	logger.Debug("subscribe goroutine exiting", "subKey", subKey, "reason", ctx.Err())
}

// cleanupSubscribe cleans up subscription resources (clears both subscribe and subscribeParams)
func (p *GmqProxy) cleanupSubscribe(subKey string) {
	// Cancel context
	if value, loaded := p.subscribe.Load(subKey); loaded {
		if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
			cancel()
		}
	}

	// Remove from active subscriptions
	p.subscribe.Delete(subKey)

	// Clean up subscription parameter cache (avoid memory leak)
	p.subscribeParams.Delete(subKey)

	// Reset subscription state
	p.setSubscribedState(false)
}

// ============================================================
// Message Acknowledgment
// ============================================================

// GmqAck acknowledges successful message processing
func (p *GmqProxy) GmqAck(ctx context.Context, msg *types.AckMessage) error {
	return p.plugin.GmqAck(ctx, msg)
}

// GmqNak negatively acknowledges a message, indicating processing failure
func (p *GmqProxy) GmqNak(ctx context.Context, msg *types.AckMessage) error {
	return p.pluginUnique.GmqNak(ctx, msg)
}

// ============================================================
// Handler Wrapping
// ============================================================

// wrapHandleFunc wraps the user's handler to control ACK at the proxy layer
func (p *GmqProxy) wrapHandleFunc(originalFunc func(ctx context.Context, message any) error, autoAck bool) func(ctx context.Context, message *types.AckMessage) error {
	// Cache necessary references to avoid closure capturing entire GmqProxy
	plugin := p.plugin
	pluginUnique := p.pluginUnique
	name := p.name

	return func(ctx context.Context, message *types.AckMessage) (err error) {
		// Panic recovery
		defer func() {
			if r := recover(); r != nil {
				logger := utils.GetLogger().WithPlugin(name)
				logger.Error("message handler panic recovered", "panic", r)
				err = fmt.Errorf("handler panic: %v", r)
			}
		}()

		// Auto-ack mode: acknowledge before processing
		if autoAck {
			if err = plugin.GmqAck(ctx, message); err != nil {
				return err
			}
		}

		// Execute user handler
		err = originalFunc(ctx, message.MessageData)

		// Manual ack mode: acknowledge after processing based on result
		if !autoAck {
			if err == nil {
				err = plugin.GmqAck(ctx, message)
			} else {
				err = pluginUnique.GmqNak(ctx, message)
			}
		}

		return err
	}
}

// ============================================================
// Subscription Management
// ============================================================

// clearSubscribe clears all active subscriptions
func (p *GmqProxy) clearSubscribe() {
	var cancels []context.CancelFunc

	// Collect cancel functions
	p.subscribe.Range(func(key, value any) bool {
		subKey, ok := key.(string)
		if !ok {
			logger := utils.GetLogger().WithPlugin(p.name)
			logger.Error("invalid subscribe key type", "key", key)
			return true
		}
		if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
			cancels = append(cancels, cancel)
		}
		p.subscribe.Delete(subKey)
		p.subscribeParams.Delete(subKey)
		return true
	})

	// Call cancel functions outside Range to avoid holding lock
	for _, cancel := range cancels {
		cancel()
	}
}

// restoreSubscribe re-establishes all subscriptions (after reconnection)
func (p *GmqProxy) restoreSubscribe() {
	logger := utils.GetLogger().WithPlugin(p.name)

	// Collect subscription information to restore (process outside Range)
	type restoreInfo struct {
		subKey   string
		sub      *subMessage
		original *types.SubMessage
	}

	var toRestore []restoreInfo
	var toCancel []context.CancelFunc

	// Collect existing subscriptions and subscriptions to restore
	p.subscribe.Range(func(key, value any) bool {
		if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
			toCancel = append(toCancel, cancel)
		}
		p.subscribe.Delete(key.(string))
		return true
	})

	p.subscribeParams.Range(func(key, value any) bool {
		subKey, ok := key.(string)
		if !ok {
			logger.Error("invalid subscribe key type", "key", key)
			return true
		}
		info, ok := value.(*subMessage)
		if !ok {
			logger.Error("invalid subMessage info", "key", subKey)
			p.subscribeParams.Delete(subKey)
			return true
		}

		subMsg, ok := info.SubMsg.(types.Subscribe)
		if !ok {
			logger.Error("invalid SubMsg type", "key", subKey)
			p.subscribeParams.Delete(subKey)
			return true
		}

		originalMsg, ok := subMsg.GetSubMsg().(*types.SubMessage)
		if !ok {
			logger.Error("invalid original message type", "key", subKey)
			p.subscribeParams.Delete(subKey)
			return true
		}

		toRestore = append(toRestore, restoreInfo{
			subKey:   subKey,
			sub:      info,
			original: originalMsg,
		})
		return true
	})

	// Cancel existing subscription goroutines (outside Range)
	for _, cancel := range toCancel {
		cancel()
	}

	// Concurrency limit: use semaphore to control concurrency
	sem := make(chan struct{}, 5) // Restore up to 5 subscriptions concurrently
	var wg sync.WaitGroup
	var failedKeys []string

	for _, info := range toRestore {
		wg.Add(1)
		go func(info restoreInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			subCtx, cancel := context.WithCancel(context.Background())
			if _, loaded := p.subscribe.LoadOrStore(info.subKey, cancel); !loaded {
				go p.runSubscribe(subCtx, info.subKey, info.sub, info.original)
			} else {
				cancel() // If already exists, cancel the newly created context
				logger.Warn("subscribe already exists during restore", "subKey", info.subKey)
			}
		}(info)
	}

	wg.Wait()

	// Clean up subscription parameters for failed restorations (avoid memory leak)
	for _, subKey := range failedKeys {
		p.subscribeParams.Delete(subKey)
		logger.Info("cleaned up failed subscribe", "subKey", subKey)
	}
}
