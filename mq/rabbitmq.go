// Package mq provides message queue implementations for the GMQ system.
//
// RabbitMQ Implementation Notes:
//   - This implementation requires the x-delayed-message plugin to be installed on RabbitMQ server
//     for delayed message support. Without this plugin, delayed messages will fail.
//   - Plugin installation: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
//   - After installation, enable the plugin: rabbitmq-plugins enable rabbitmq_delayed_message_exchange
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ plugin name constant
const rabbitmqPluginName = "rabbitmq"

// Package-level logger instance to avoid repeated heap allocation from utils.LogWithPlugin
// This eliminates the "rabbitmq string escapes to heap" issue in every log call
var rabbitmqLogger = utils.GetLogger().WithPlugin(rabbitmqPluginName)

// RabbitMQPubMessage represents a RabbitMQ publish message.
// Embeds PubMessage for basic message fields.
type RabbitMQPubMessage struct {
	types.PubMessage
}

// RabbitMQPubDelayMessage represents a RabbitMQ delayed publish message.
// Requires x-delayed-message plugin to be installed on RabbitMQ server.
// Embeds PubDelayMessage for delayed message fields.
type RabbitMQPubDelayMessage struct {
	types.PubDelayMessage
}

// RabbitMQSubMessage represents a RabbitMQ subscription configuration.
// Embeds SubMessage for basic subscription fields.
type RabbitMQSubMessage struct {
	types.SubMessage
}

// RabbitMQConn is the RabbitMQ message queue implementation.
// Provides publish, subscribe, delayed message (with plugin), and acknowledgment capabilities.
// Uses a unified dead letter exchange for failed messages.
//
// RabbitMQ Features:
//   - Supports durable exchanges and queues (messages survive broker restart)
//   - Manual acknowledgment mode for reliable message processing
//   - Unified dead letter exchange for collecting all failed messages
//   - Delayed message support via x-delayed-message plugin
//
// Dead Letter Configuration:
//   - Dead letter exchange: "gmq.dead.letter.exchange" (fanout type)
//   - Dead letter queue: "gmq.dead.letter.queue"
//   - Failed messages are automatically routed to the dead letter exchange
type RabbitMQConn struct {
	conn              *amqp.Connection   // RabbitMQ connection object
	channel           *amqp.Channel      // RabbitMQ channel for operations (channels are lightweight)
	unifiedDLExchange string             // unified dead letter exchange name for all failed messages
	unifiedDLQueue    string             // unified dead letter queue name for collecting failed messages
	setSubscribed     func(bool)         // setter function to report connection state changes to proxy
	activeConsumers   map[string]string  // track active consumer tags for each topic (topic -> consumer tag)
	monitorCtx        context.Context    // monitor goroutine context for cleanup
	monitorCancel     context.CancelFunc // monitor goroutine cancel function
}

// rabbitMQConfig holds RabbitMQ connection configuration parameters.
// Used with MapToStruct to convert config map to struct.
type rabbitMQConfig struct {
	Addr     string // RabbitMQ server address
	Port     string // RabbitMQ server port
	Username string // authentication username
	Password string // authentication password
	VHost    string // virtual host name
}

// GmqPing checks if RabbitMQ connection is alive.
// Returns true if both connection and channel are initialized and not closed
func (c *RabbitMQConn) GmqPing(_ context.Context) bool {
	if c.conn == nil || c.channel == nil {
		return false
	}
	if c.conn.IsClosed() || c.channel.IsClosed() {
		return false
	}
	return true
}

// GmqGetConn retrieves the RabbitMQ connection objects.
// Returns a map containing the connection and channel
func (c *RabbitMQConn) GmqGetConn(_ context.Context) any {
	m := map[string]any{
		"conn":    c.conn,
		"channel": c.channel,
	}
	return m
}

// GmqConnect establishes connection to RabbitMQ server.
// Closes existing connections if any, then creates new connection and channel.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - cfg: connection configuration parameters
//
// Returns error if connection or channel creation fails
func (c *RabbitMQConn) GmqConnect(_ context.Context, cfg map[string]any) (err error) {
	config := new(rabbitMQConfig)
	if err = utils.MapToStruct(config, cfg); err != nil {
		rabbitmqLogger.Error("config parse failed", "error", err)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, err)
	}
	if config.Addr == "" {
		rabbitmqLogger.Error("config validation failed", "error", types.ErrConfigAddrRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigAddrRequired)
	}
	if config.Port == "" {
		rabbitmqLogger.Error("config validation failed", "error", types.ErrConfigPortRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigPortRequired)
	}
	if config.Username == "" {
		rabbitmqLogger.Error("config validation failed", "error", types.ErrConfigUsernameRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigUsernameRequired)
	}
	if config.Password == "" {
		rabbitmqLogger.Error("config validation failed", "error", types.ErrConfigPasswordRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigPasswordRequired)
	}

	// Stop existing monitor goroutine before creating new connection
	// This prevents goroutine leak on reconnection
	if c.monitorCancel != nil {
		c.monitorCancel()
		// Wait a short time to ensure the goroutine exits
		time.Sleep(10 * time.Millisecond)
		c.monitorCancel = nil
		c.monitorCtx = nil
	}

	// Cancel all active consumers explicitly before closing the channel
	// This ensures consumer tags are released before we try to reuse them
	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}
	if c.channel != nil && !c.channel.IsClosed() {
		c.channel.Close()
	}

	// build connection URL
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", config.Username, config.Password, config.Addr, config.Port, config.VHost)

	// create connection
	newConn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 30 * time.Second, // heartbeat detection to proactively detect silent disconnection
	})
	if err != nil {
		rabbitmqLogger.Error("connect failed", "addr", config.Addr, "port", config.Port, "error", err)
		return fmt.Errorf("%s: connect: %w", rabbitmqPluginName, err)
	}

	// create Channel
	newChannel, err := newConn.Channel()
	if err != nil {
		newConn.Close()
		rabbitmqLogger.Error("create channel failed", "error", err)
		return fmt.Errorf("%s: create_channel: %w", rabbitmqPluginName, err)
	}

	// Create monitor context and cancel function
	monitorCtx, monitorCancel := context.WithCancel(context.Background())

	// Register connection/channel close listeners
	connCloseChan := newConn.NotifyClose(make(chan *amqp.Error, 1))
	channelCloseChan := newChannel.NotifyClose(make(chan *amqp.Error, 1))

	// Reset active consumers map on reconnection
	c.activeConsumers = make(map[string]string)

	// Atomic assignment to avoid race conditions
	c.conn = newConn
	c.channel = newChannel
	c.monitorCtx = monitorCtx
	c.monitorCancel = monitorCancel

	// Start monitoring goroutine to detect disconnections
	go c.monitorDisconnect(monitorCtx, connCloseChan, channelCloseChan)

	return nil
}

// monitorDisconnect monitors connection/channel disconnection events
// Only handles abnormal disconnections, ignores normal close events (err == nil or err.Code == 200)
// Receives context to allow clean shutdown
func (c *RabbitMQConn) monitorDisconnect(ctx context.Context, connCloseChan, channelCloseChan chan *amqp.Error) {
	defer func() {
		// Panic recovery to prevent goroutine leak on unexpected errors
		if r := recover(); r != nil {
			rabbitmqLogger.Error("monitorDisconnect panic recovered", "panic", r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, exit monitoring
			return
		case err, ok := <-connCloseChan:
			if !ok {
				// Channel closed, exit goroutine
				return
			}
			if err != nil && err.Code == amqp.ConnectionForced {
				// 清空 consumer tag 记录
				c.activeConsumers = make(map[string]string)
				if c.setSubscribed != nil {
					c.setSubscribed(false)
				}
			}
		case err, ok := <-channelCloseChan:
			if !ok {
				// Channel closed, exit goroutine
				return
			}
			if err != nil && err.Code == amqp.ConnectionForced {
				// 清空 consumer tag 记录
				c.activeConsumers = make(map[string]string)
				if c.setSubscribed != nil {
					c.setSubscribed(false)
				}
			}
		}
	}
}

// SetSubscribedSetter sets the callback function to update subscription status.
// The setter function is called when connection or channel disconnects.
func (c *RabbitMQConn) SetSubscribedSetter(setter func(bool)) {
	c.setSubscribed = setter
}

// GmqClose closes the RabbitMQ connection and channel.
// Safe to call multiple times
func (c *RabbitMQConn) GmqClose(_ context.Context) (err error) {

	// Stop monitor goroutine to prevent goroutine leak
	if c.monitorCancel != nil {
		c.monitorCancel()
		c.monitorCancel = nil
		c.monitorCtx = nil
	}

	// Clear external callback reference to avoid memory leak
	c.setSubscribed = nil

	// Clear active consumers map
	c.activeConsumers = nil

	// Get and clear references, then close
	// amqp's Close() method is idempotent, can be safely called multiple times
	if c.channel != nil {
		c.channel.Close()
		c.channel = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	rabbitmqLogger.Info("connection closed")
	return nil
}

// setupQueue creates or verifies RabbitMQ queue, exchange, and binding configuration.
// Parameters:
//   - topic: business topic name (used as exchange name, routing key, and queue name)
//   - delayMsg: whether this is a delayed message queue
//
// Returns exchange name, routing key, and error if any operation fails
func (c *RabbitMQConn) setupQueue(topic string, delayMsg bool) (string, string, error) {

	// 0. ensure unified dead letter exchange and queue are created (idempotent)
	if err := c.setupUnifiedDeadLetter(); err != nil {
		return "", "", err
	}

	// 1. basic configuration
	exchangeType := "fanout"
	exchangeName := topic
	routingKey := topic
	args := amqp.Table{}
	if delayMsg {
		exchangeType = "x-delayed-message"
		exchangeName = topic + ".delayed"
		args["x-delayed-type"] = "fanout"
	}

	// 2. declare business queue (associate with dead letter configuration)
	// directly point dead letter to unified dead letter exchange
	queueArgs := amqp.Table{
		// core: specify current queue's dead letter exchange as unified dead letter exchange
		"x-dead-letter-exchange": c.unifiedDLExchange,
		// fanout type exchange does not need routing key
		"x-dead-letter-routing-key": "",
	}

	// 3. declare business Exchange
	if err := c.channel.ExchangeDeclare(
		exchangeName, // business exchange name
		exchangeType, // exchange type (normal/fanout or delayed/x-delayed-message)
		true,         // durable - always persist
		false,        // autoDelete
		false,        // internal
		false,        // noWait
		args,         // exchange arguments (delayed exchange needs x-delayed-type)
	); err != nil {
		rabbitmqLogger.Error("declare exchange failed", "exchange", exchangeName, "type", exchangeType, "error", err)
		return "", "", fmt.Errorf("%s: declare_exchange: %w", rabbitmqPluginName, err)
	}

	// 4. declare business queue
	if _, err := c.channel.QueueDeclare(
		topic,     // business queue name
		true,      // durable - always persist
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		queueArgs, // queue arguments (including dead letter configuration)
	); err != nil {
		rabbitmqLogger.Error("declare queue failed", "queue", topic, "error", err)
		return "", "", fmt.Errorf("%s: declare_queue: %w", rabbitmqPluginName, err)
	}

	// 5. bind business queue to business exchange
	if err := c.channel.QueueBind(
		topic,        // business queue name
		routingKey,   // routing key
		exchangeName, // business exchange name
		false,        // noWait
		nil,          // args
	); err != nil {
		rabbitmqLogger.Error("bind queue failed", "queue", topic, "exchange", exchangeName, "error", err)
		return "", "", fmt.Errorf("%s: bind_queue: %w", rabbitmqPluginName, err)
	}
	return exchangeName, routingKey, nil
}

// setupUnifiedDeadLetter creates or verifies unified dead letter exchange and queue (idempotent operation).
// This is called automatically during publish operations to ensure dead letter infrastructure exists.
// Returns error if declaration or binding fails
func (c *RabbitMQConn) setupUnifiedDeadLetter() error {

	// declare unified dead letter exchange (fanout type)
	// if exchange already exists with same config, RabbitMQ will ignore this operation (idempotent)
	if err := c.channel.ExchangeDeclare(
		"gmq.dead.letter.exchange", // exchange name
		"fanout",                   // fanout type, broadcast to all bound queues
		true,                       // durable
		false,                      // autoDelete
		false,                      // internal
		false,                      // noWait
		nil,                        // args
	); err != nil {
		rabbitmqLogger.Error("declare dead letter exchange failed", "exchange", "gmq.dead.letter.exchange", "error", err)
		return fmt.Errorf("%s: declare_dl_exchange: %w", rabbitmqPluginName, err)
	}

	// declare unified dead letter queue
	// if queue already exists with same config, RabbitMQ will ignore this operation (idempotent)
	if _, err := c.channel.QueueDeclare(
		"gmq.dead.letter.queue", // unified dead letter queue name
		true,                    // durable
		false,                   // autoDelete
		false,                   // exclusive
		false,                   // noWait
		nil,                     // args
	); err != nil {
		rabbitmqLogger.Error("declare dead letter queue failed", "queue", "gmq.dead.letter.queue", "error", err)
		return fmt.Errorf("%s: declare_dl_queue: %w", rabbitmqPluginName, err)
	}

	// bind unified dead letter queue to unified dead letter exchange
	// if binding already exists, RabbitMQ will ignore this operation (idempotent)
	if err := c.channel.QueueBind(
		"gmq.dead.letter.queue",    // unified dead letter queue
		"",                         // fanout type does not need routing key
		"gmq.dead.letter.exchange", // unified dead letter exchange
		false,                      // noWait
		nil,                        // args
	); err != nil {
		rabbitmqLogger.Error("bind dead letter queue failed", "queue", "gmq.dead.letter.queue", "exchange", "gmq.dead.letter.exchange", "error", err)
		return fmt.Errorf("%s: bind_dl_queue: %w", rabbitmqPluginName, err)
	}

	// Set field values only after all operations succeed
	c.unifiedDLExchange = "gmq.dead.letter.exchange"
	c.unifiedDLQueue = "gmq.dead.letter.queue"

	rabbitmqLogger.Debug("dead letter infrastructure ready", "exchange", c.unifiedDLExchange, "queue", c.unifiedDLQueue)
	return nil
}

// GmqPublish publishes a message to RabbitMQ.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must be *RabbitMQPubMessage)
//
// Returns error if publish fails
func (c *RabbitMQConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	cfg, ok := msg.(*RabbitMQPubMessage)
	if !ok {
		rabbitmqLogger.Error("publish:invalid message type", "expected", "*RabbitMQPubMessage", "plugin", rabbitmqPluginName)
		return fmt.Errorf("%s: publish: %w: expected *RabbitMQPubMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}
	if err = c.createPublish(ctx, cfg.Topic, 0, cfg.Data); err != nil {
		rabbitmqLogger.Error("publish failed", "topic", cfg.Topic, "error", err)
		return err
	}
	return nil
}

// GmqPublishDelay publishes a delayed message to RabbitMQ.
// Uses x-delayed-message plugin for delayed delivery.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: delayed message to publish (must be *RabbitMQPubDelayMessage)
//
// Returns error if publish fails
func (c *RabbitMQConn) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) (err error) {
	cfg, ok := msg.(*RabbitMQPubDelayMessage)
	if !ok {
		rabbitmqLogger.Error("publish_delay:invalid message type", "expected", "*RabbitMQPubDelayMessage", "plugin", rabbitmqPluginName)
		return fmt.Errorf("%s: publish_delay: %w: expected *RabbitMQPubDelayMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}
	if err = c.createPublish(ctx, cfg.Topic, cfg.DelaySeconds, cfg.Data); err != nil {
		rabbitmqLogger.Error("publish delay failed", "topic", cfg.Topic, "delay", cfg.DelaySeconds, "error", err)
		return err
	}
	return nil
}

// createPublish publishes a message to RabbitMQ with optional delay.
// Creates exchange, queue, and binding if they don't exist.
// All failed messages are routed to a unified dead letter exchange.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - topic: business topic name (used as exchange name, routing key, and queue name)
//   - delayTime: delay time in seconds (0 for immediate delivery)
//   - data: message payload
//
// Returns error if any operation fails
func (c *RabbitMQConn) createPublish(ctx context.Context, topic string, delayTime int, data any) error {

	delayMsg := delayTime > 0
	exchangeName, routingKey, err := c.setupQueue(topic, delayMsg)
	if err != nil {
		return err
	}
	// serialize message data
	body, err := json.Marshal(data)
	if err != nil {
		rabbitmqLogger.Error("marshal data failed", "error", err)
		return fmt.Errorf("%s: marshal: %w", rabbitmqPluginName, err)
	}

	// build publish message
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent, // always persistent
		Timestamp:    time.Now(),
	}

	// set delayed message header (if delay needed)
	if delayMsg {
		duration := delayTime * 1000 // milliseconds
		publishing.Headers = amqp.Table{
			"x-delay": duration,
		}
	}

	// publish message
	if err = c.channel.PublishWithContext(
		ctx,
		exchangeName, // business exchange name
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		publishing,
	); err != nil {
		rabbitmqLogger.Error("publish message failed", "exchange", exchangeName, "routingKey", routingKey, "error", err)
		return fmt.Errorf("%s: publish: %w", rabbitmqPluginName, err)
	}

	return nil
}

// GmqSubscribe subscribes to RabbitMQ messages from a queue.
// Uses manual acknowledgment mode for reliable message processing.
// Automatically creates the queue if it doesn't exist.
// This method blocks until context is cancelled (proxy layer runs it in a goroutine).
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - sub: subscription configuration (must be *RabbitMQSubMessage)
//
// Returns error if subscription fails
func (c *RabbitMQConn) GmqSubscribe(ctx context.Context, sub types.Subscribe) (err error) {
	cfg, ok := sub.GetSubMsg().(*RabbitMQSubMessage)
	if !ok {
		rabbitmqLogger.Error("subscribe:invalid message type", "expected", "*RabbitMQSubMessage", "plugin", rabbitmqPluginName)
		return fmt.Errorf("%s: subscribe: %w: expected *RabbitMQSubMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	_, _, err = c.setupQueue(cfg.Topic, false)
	if err != nil {
		rabbitmqLogger.Error("setup queue failed", "queue", cfg.Topic, "error", err)
		return fmt.Errorf("%s: setup_queue: %w", rabbitmqPluginName, err)
	}

	// Cancel existing consumer with same tag before creating new one
	// This prevents "NOT_ALLOWED - attempt to reuse consumer tag" errors
	if existingTag, exists := c.activeConsumers[cfg.Topic]; exists {
		if err := c.channel.Cancel(existingTag, false); err != nil {
			rabbitmqLogger.Warn("cancel existing consumer before subscribe", "topic", cfg.Topic, "consumerTag", existingTag, "error", err)
		}
	}

	if err = c.channel.Qos(cfg.FetchCount, 0, false); err != nil {
		rabbitmqLogger.Error("set qos failed", "prefetch", cfg.FetchCount, "error", err)
		return fmt.Errorf("%s: set_qos: %w", rabbitmqPluginName, err)
	}

	// Use cfg.ConsumerName as the consumer tag for consistent message distribution
	// The consumer tag is explicitly cancelled during reconnection to prevent reuse errors
	msgs, err := c.channel.Consume(
		cfg.Topic,        // queue
		cfg.ConsumerName, // consumer - use configured consumer name as tag
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		rabbitmqLogger.Error("consume failed", "queue", cfg.Topic, "consumerName", cfg.ConsumerName, "error", err)
		return fmt.Errorf("%s: consume: %w", rabbitmqPluginName, err)
	}
	rabbitmqLogger.Info("subscribe success", "queue", cfg.Topic, "consumerName", cfg.ConsumerName)
	// Track active consumer for cleanup during reconnection
	c.activeConsumers[cfg.Topic] = cfg.ConsumerName
	// Clean up consumer tracking when exit
	defer delete(c.activeConsumers, cfg.Topic)

	// Consume messages - this will block until context is cancelled
	// Proxy layer already runs this in a goroutine, so we don't need another one
	for msgv := range msgs {
		if err = sub.GetAckHandleFunc()(ctx, &types.AckMessage{
			MessageData:     msgv.Body,
			AckRequiredAttr: &msgv,
		}); err != nil {
			rabbitmqLogger.Error("message handler failed", "queue", cfg.Topic, "consumerName", cfg.ConsumerName, "deliveryTag", msgv.DeliveryTag, "error", err)
			continue
		}
	}

	return nil
}

// GmqAck acknowledges successful processing of a RabbitMQ message.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if acknowledgment fails
func (c *RabbitMQConn) GmqAck(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*amqp.Delivery)
	if !ok {
		rabbitmqLogger.Error("ack:invalid message type", "expected", "*amqp.Delivery", "plugin", rabbitmqPluginName)
		return fmt.Errorf("%s: ack: %w: expected *amqp.Delivery", rabbitmqPluginName, types.ErrInvalidMessageType)
	}
	if err := msgCfg.Ack(false); err != nil {
		rabbitmqLogger.Error("ack failed", "deliveryTag", msgCfg.DeliveryTag, "error", err)
		return fmt.Errorf("%s: ack: %w", rabbitmqPluginName, err)
	}
	return nil
}

// GmqNak negatively acknowledges a RabbitMQ message, indicating processing failure.
// The message is NOT re-queued and will be sent to dead letter queue.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if negative acknowledgment fails
func (c *RabbitMQConn) GmqNak(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*amqp.Delivery)
	if !ok || msg.AckRequiredAttr == nil {
		rabbitmqLogger.Error("nak:invalid message type", "expected", "*amqp.Delivery", "plugin", rabbitmqPluginName)
		return fmt.Errorf("%s: ack: %w: expected *amqp.Delivery", rabbitmqPluginName, types.ErrInvalidMessageType)
	}
	// requeue=true: message re-enters queue and will be redelivered
	// requeue=false: message does not re-enter queue, goes to dead letter queue (if dead letter exchange configured)
	if err := msgCfg.Nack(false, false); err != nil {
		rabbitmqLogger.Error("nak failed", "deliveryTag", msgCfg.DeliveryTag, "error", err)
		return fmt.Errorf("%s: nak: %w", rabbitmqPluginName, err)
	}
	return nil
}
