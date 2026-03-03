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

// RabbitMQPubMessage represents a RabbitMQ publish message with durability option.
// Embeds PubMessage for basic message fields.
type RabbitMQPubMessage struct {
	types.PubMessage
	Durable bool // whether to persist messages to disk (persistent messages survive broker restart)
}

// RabbitMQPubDelayMessage represents a RabbitMQ delayed publish message with durability option.
// Requires x-delayed-message plugin to be installed on RabbitMQ server.
// Embeds PubDelayMessage for delayed message fields.
type RabbitMQPubDelayMessage struct {
	types.PubDelayMessage
	Durable bool // whether to persist messages to disk (persistent messages survive broker restart)
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
	conn              *amqp.Connection // RabbitMQ connection object
	channel           *amqp.Channel    // RabbitMQ channel for operations (channels are lightweight)
	unifiedDLExchange string           // unified dead letter exchange name for all failed messages
	unifiedDLQueue    string           // unified dead letter queue name for collecting failed messages
	unifiedDLConsumer string           // unified dead letter consumer name for monitoring failed messages
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
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	config := new(rabbitMQConfig)
	if err = utils.MapToStruct(config, cfg); err != nil {
		logger.Error("config parse failed", "error", err)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, err)
	}
	if config.Addr == "" {
		logger.Error("config validation failed", "error", types.ErrConfigAddrRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigAddrRequired)
	}
	if config.Port == "" {
		logger.Error("config validation failed", "error", types.ErrConfigPortRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigPortRequired)
	}
	if config.Username == "" {
		logger.Error("config validation failed", "error", types.ErrConfigUsernameRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigUsernameRequired)
	}
	if config.Password == "" {
		logger.Error("config validation failed", "error", types.ErrConfigPasswordRequired)
		return fmt.Errorf("%s: config: %w", rabbitmqPluginName, types.ErrConfigPasswordRequired)
	}

	// safely close old connection (only for this data source)
	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}
	if c.channel != nil && !c.channel.IsClosed() {
		c.channel.Close()
	}

	// build connection URL
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", config.Username, config.Password, config.Addr, config.Port, config.VHost)

	// create connection
	newConn, err := amqp.Dial(url)
	if err != nil {
		logger.Error("connect failed", "addr", config.Addr, "port", config.Port, "error", err)
		return fmt.Errorf("%s: connect: %w", rabbitmqPluginName, err)
	}

	// create Channel
	newChannel, err := newConn.Channel()
	if err != nil {
		newConn.Close()
		logger.Error("create channel failed", "error", err)
		return fmt.Errorf("%s: create_channel: %w", rabbitmqPluginName, err)
	}

	c.conn = newConn
	c.channel = newChannel
	logger.Info("connected successfully", "addr", config.Addr, "port", config.Port, "vhost", config.VHost)
	return nil
}

// GmqClose closes the RabbitMQ connection and channel.
// Safe to call multiple times
func (c *RabbitMQConn) GmqClose(_ context.Context) (err error) {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	if c.channel != nil {
		if err = c.channel.Close(); err != nil {
			logger.Error("close channel failed", "error", err)
		}
		c.channel = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	logger.Info("connection closed")
	return nil
}

// setupUnifiedDeadLetter creates or verifies unified dead letter exchange and queue (idempotent operation).
// This is called automatically during publish operations to ensure dead letter infrastructure exists.
// Returns error if declaration or binding fails
func (c *RabbitMQConn) setupUnifiedDeadLetter() error {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	// set unified dead letter exchange and queue names
	c.unifiedDLExchange = "gmq.dead.letter.exchange"
	c.unifiedDLQueue = "gmq.dead.letter.queue"
	c.unifiedDLConsumer = "gmq.dead.letter.consumer"

	// declare unified dead letter exchange (fanout type)
	// if exchange already exists with same config, RabbitMQ will ignore this operation (idempotent)
	if err := c.channel.ExchangeDeclare(
		c.unifiedDLExchange, // exchange name
		"fanout",            // fanout type, broadcast to all bound queues
		true,                // durable
		false,               // autoDelete
		false,               // internal
		false,               // noWait
		nil,                 // args
	); err != nil {
		logger.Error("declare dead letter exchange failed", "exchange", c.unifiedDLExchange, "error", err)
		return fmt.Errorf("%s: declare_dl_exchange: %w", rabbitmqPluginName, err)
	}

	// declare unified dead letter queue
	// if queue already exists with same config, RabbitMQ will ignore this operation (idempotent)
	if _, err := c.channel.QueueDeclare(
		c.unifiedDLQueue, // unified dead letter queue name
		true,             // durable
		false,            // autoDelete
		false,            // exclusive
		false,            // noWait
		nil,              // args
	); err != nil {
		logger.Error("declare dead letter queue failed", "queue", c.unifiedDLQueue, "error", err)
		return fmt.Errorf("%s: declare_dl_queue: %w", rabbitmqPluginName, err)
	}

	// bind unified dead letter queue to unified dead letter exchange
	// if binding already exists, RabbitMQ will ignore this operation (idempotent)
	if err := c.channel.QueueBind(
		c.unifiedDLQueue,    // unified dead letter queue
		"",                  // fanout type does not need routing key
		c.unifiedDLExchange, // unified dead letter exchange
		false,               // noWait
		nil,                 // args
	); err != nil {
		logger.Error("bind dead letter queue failed", "queue", c.unifiedDLQueue, "exchange", c.unifiedDLExchange, "error", err)
		return fmt.Errorf("%s: bind_dl_queue: %w", rabbitmqPluginName, err)
	}

	logger.Debug("dead letter infrastructure ready", "exchange", c.unifiedDLExchange, "queue", c.unifiedDLQueue)
	return nil
}

// GmqPublish publishes a message to RabbitMQ.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must be *RabbitMQPubMessage)
//
// Returns error if publish fails
func (c *RabbitMQConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	cfg, ok := msg.(*RabbitMQPubMessage)
	if !ok {
		logger.Error("invalid message type", "expected", "*RabbitMQPubMessage", "got", fmt.Sprintf("%T", msg))
		return fmt.Errorf("%s: publish: %w: expected *RabbitMQPubMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	if err = c.createPublish(ctx, cfg.Topic, cfg.Durable, 0, cfg.Data); err != nil {
		logger.Error("publish failed", "topic", cfg.Topic, "error", err)
		return err
	}

	logger.Debug("publish success", "topic", cfg.Topic)
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
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	cfg, ok := msg.(*RabbitMQPubDelayMessage)
	if !ok {
		logger.Error("invalid message type", "expected", "*RabbitMQPubDelayMessage", "got", fmt.Sprintf("%T", msg))
		return fmt.Errorf("%s: publish_delay: %w: expected *RabbitMQPubDelayMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	if err = c.createPublish(ctx, cfg.Topic, cfg.Durable, cfg.DelaySeconds, cfg.Data); err != nil {
		logger.Error("publish delay failed", "topic", cfg.Topic, "delay", cfg.DelaySeconds, "error", err)
		return err
	}

	logger.Debug("publish delay success", "topic", cfg.Topic, "delay", cfg.DelaySeconds)
	return nil
}

// createPublish publishes a message to RabbitMQ with optional delay.
// Creates exchange, queue, and binding if they don't exist.
// All failed messages are routed to a unified dead letter exchange.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - topic: business topic name (used as exchange name, routing key, and queue name)
//   - durable: whether to persist messages
//   - delayTime: delay time in seconds (0 for immediate delivery)
//   - data: message payload
//
// Returns error if any operation fails
func (c *RabbitMQConn) createPublish(ctx context.Context, topic string, durable bool, delayTime int, data any) error {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	// 0. ensure unified dead letter exchange and queue are created (idempotent)
	if err := c.setupUnifiedDeadLetter(); err != nil {
		return err
	}

	delayMsg := delayTime > 0
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
		durable,      // whether to persist
		false,        // autoDelete
		false,        // internal
		false,        // noWait
		args,         // exchange arguments (delayed exchange needs x-delayed-type)
	); err != nil {
		logger.Error("declare exchange failed", "exchange", exchangeName, "type", exchangeType, "error", err)
		return fmt.Errorf("%s: declare_exchange: %w", rabbitmqPluginName, err)
	}

	// 4. declare business queue
	if _, err := c.channel.QueueDeclare(
		topic,     // business queue name
		durable,   // whether to persist
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		queueArgs, // queue arguments (including dead letter configuration)
	); err != nil {
		logger.Error("declare queue failed", "queue", topic, "error", err)
		return fmt.Errorf("%s: declare_queue: %w", rabbitmqPluginName, err)
	}

	// 5. bind business queue to business exchange
	if err := c.channel.QueueBind(
		topic,        // business queue name
		routingKey,   // routing key
		exchangeName, // business exchange name
		false,        // noWait
		nil,          // args
	); err != nil {
		logger.Error("bind queue failed", "queue", topic, "exchange", exchangeName, "error", err)
		return fmt.Errorf("%s: bind_queue: %w", rabbitmqPluginName, err)
	}

	// 6. serialize message data
	body, err := json.Marshal(data)
	if err != nil {
		logger.Error("marshal data failed", "error", err)
		return fmt.Errorf("%s: marshal: %w", rabbitmqPluginName, err)
	}

	// 7. build publish message
	deliveryMode := amqp.Transient
	if durable {
		deliveryMode = amqp.Persistent
	}
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: deliveryMode,
		Timestamp:    time.Now(),
	}

	// set delayed message header (if delay needed)
	if delayMsg {
		duration := delayTime * 1000 // milliseconds
		publishing.Headers = amqp.Table{
			"x-delay": duration,
		}
	}

	// 8. publish message
	if err = c.channel.PublishWithContext(
		ctx,
		exchangeName, // business exchange name
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		publishing,
	); err != nil {
		logger.Error("publish message failed", "exchange", exchangeName, "routingKey", routingKey, "error", err)
		return fmt.Errorf("%s: publish: %w", rabbitmqPluginName, err)
	}

	return nil
}

// GmqSubscribe subscribes to RabbitMQ messages from a queue.
// Uses manual acknowledgment mode for reliable message processing.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - sub: subscription configuration (must be *RabbitMQSubMessage)
//
// Returns error if subscription fails
func (c *RabbitMQConn) GmqSubscribe(ctx context.Context, sub types.Subscribe) (err error) {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	cfg, ok := sub.GetSubMsg().(*RabbitMQSubMessage)
	if !ok {
		logger.Error("invalid message type", "expected", "*RabbitMQSubMessage", "got", fmt.Sprintf("%T", sub.GetSubMsg()))
		return fmt.Errorf("%s: subscribe: %w: expected *RabbitMQSubMessage", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	if err = c.channel.Qos(cfg.FetchCount, 0, false); err != nil {
		logger.Error("set qos failed", "prefetch", cfg.FetchCount, "error", err)
		return fmt.Errorf("%s: set_qos: %w", rabbitmqPluginName, err)
	}

	msgs, err := c.channel.Consume(
		cfg.Topic,        // queue
		cfg.ConsumerName, // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		logger.Error("consume failed", "queue", cfg.Topic, "consumer", cfg.ConsumerName, "error", err)
		return fmt.Errorf("%s: consume: %w", rabbitmqPluginName, err)
	}

	logger.Info("subscribed successfully", "queue", cfg.Topic, "consumer", cfg.ConsumerName)

	for msgv := range msgs {
		select {
		case <-ctx.Done():
			logger.Debug("subscription context done", "queue", cfg.Topic, "consumer", cfg.ConsumerName)
			return nil
		default:
			if err = sub.GetAckHandleFunc()(ctx, &types.AckMessage{
				MessageData:     msgv.Body,
				AckRequiredAttr: msgv,
			}); err != nil {
				logger.Error("message handler failed", "queue", cfg.Topic, "consumer", cfg.ConsumerName, "deliveryTag", msgv.DeliveryTag, "error", err)
				continue
			}
		}
	}

	logger.Debug("consume loop ended", "queue", cfg.Topic, "consumer", cfg.ConsumerName)
	return nil
}

// GmqAck acknowledges successful processing of a RabbitMQ message.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if acknowledgment fails
func (c *RabbitMQConn) GmqAck(_ context.Context, msg *types.AckMessage) error {
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	msgCfg, ok := msg.AckRequiredAttr.(amqp.Delivery)
	if !ok {
		logger.Error("invalid ack attr type", "expected", "amqp.Delivery", "got", fmt.Sprintf("%T", msg.AckRequiredAttr))
		return fmt.Errorf("%s: ack: %w: expected amqp.Delivery", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	if err := msgCfg.Ack(false); err != nil {
		logger.Error("ack failed", "deliveryTag", msgCfg.DeliveryTag, "error", err)
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
	logger := utils.LogWithPlugin(rabbitmqPluginName)

	msgCfg, ok := msg.AckRequiredAttr.(amqp.Delivery)
	if !ok {
		logger.Error("invalid nak attr type", "expected", "amqp.Delivery", "got", fmt.Sprintf("%T", msg.AckRequiredAttr))
		return fmt.Errorf("%s: nak: %w: expected amqp.Delivery", rabbitmqPluginName, types.ErrInvalidMessageType)
	}

	// requeue=true: message re-enters queue and will be redelivered
	// requeue=false: message does not re-enter queue, goes to dead letter queue (if dead letter exchange configured)
	if err := msgCfg.Nack(false, false); err != nil {
		logger.Error("nak failed", "deliveryTag", msgCfg.DeliveryTag, "error", err)
		return fmt.Errorf("%s: nak: %w", rabbitmqPluginName, err)
	}

	return nil
}
