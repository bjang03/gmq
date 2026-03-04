// Package mq provides message queue implementations for the GMQ system.
//
// NATS Implementation Notes:
//   - This implementation uses NATS JetStream for persistent message storage and delivery.
//   - Supports both file and memory storage backends for message persistence.
//   - Delayed messages are supported via JetStream message scheduling (Nats-Schedule header).
//   - Uses durable consumers with explicit acknowledgment for reliable message processing.
//   - Stream names are automatically generated based on topic, durability, and delay settings.
//   - Stream naming convention: "ordinary_file_{topic}", "delay_memory_{topic}", etc.
//   - Topic names are sanitized by replacing special characters with underscores.
//
// JetStream Features Used:
//   - Persistent message storage with configurable retention policies
//   - Consumer groups with durable subscriptions
//   - Message scheduling for delayed delivery
//   - Automatic redelivery with backoff strategy on failure
//   - Interest-based retention policy (messages removed after all consumers ack)
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/nats-io/nats.go"
)

// NATS plugin name constant
const natsPluginName = "nats"

// Package-level logger instance to avoid repeated heap allocation from utils.LogWithPlugin
// This eliminates the "nats string escapes to heap" issue in every log call
var natsLogger = utils.GetLogger().WithPlugin(natsPluginName)

// NatsPubMessage represents a NATS publish message with durability option.
// Embeds PubMessage for basic message fields.
type NatsPubMessage struct {
	types.PubMessage
	Durable bool // whether to persist messages to disk using JetStream file storage
}

// NatsPubDelayMessage represents a NATS delayed publish message with durability option.
// Embeds PubDelayMessage for delayed message fields.
type NatsPubDelayMessage struct {
	types.PubDelayMessage
	Durable bool // whether to persist messages to disk using JetStream file storage
}

// NatsSubMessage represents a NATS subscription configuration.
// Embeds SubMessage for basic subscription fields.
type NatsSubMessage struct {
	types.SubMessage
	Durable    bool // whether to persist messages to disk using JetStream file storage
	IsDelayMsg bool // whether this is a delayed message stream (requires AllowMsgSchedules)
}

// NatsConn is the NATS message queue implementation using JetStream for persistent messaging.
// Provides publish, subscribe, delayed message, and acknowledgment capabilities.
//
// JetStream Features:
//   - Persistent message storage (file or memory)
//   - Consumer groups with acknowledgments
//   - Delayed message delivery via message scheduling
//   - Automatic redelivery on failure
//
// Stream Naming Convention:
//   - Ordinary file storage: "ordinary_file_{topic_name}"
//   - Ordinary memory storage: "ordinary_memory_{topic_name}"
//   - Delayed file storage: "delay_file_{topic_name}"
//   - Delayed memory storage: "delay_memory_{topic_name}"
//
// Topic names are sanitized by replacing special characters with underscores.
type NatsConn struct {
	conn          *nats.Conn            // NATS connection object for basic messaging
	js            nats.JetStreamContext // JetStream context for persistent messaging and consumer management
	setSubscribed func(bool)            // setter function to report connection state changes to proxy
}

func (c *NatsConn) SetSubscribedSetter(setter func(bool)) {
	c.setSubscribed = setter
}

// natsConfig holds NATS connection configuration parameters.
// Used with MapToStruct to convert config map to struct.
type natsConfig struct {
	Addr     string // NATS server address
	Port     string // NATS server port
	Username string // authentication username
	Password string // authentication password
}

// GmqPing checks if NATS connection is alive.
// Returns true if both connection and JetStream context are initialized and connected
func (c *NatsConn) GmqPing(_ context.Context) bool {
	if c.conn == nil || c.js == nil {
		return false
	}
	return c.conn != nil && c.conn.IsConnected()
}

// GmqGetConn retrieves the NATS connection objects.
// Returns a map containing the connection and JetStream context
func (c *NatsConn) GmqGetConn(_ context.Context) any {
	m := map[string]any{
		"conn": c.conn,
		"js":   c.js,
	}
	return m
}

// GmqConnect establishes connection to NATS server and initializes JetStream.
// Sets up connection event handlers for disconnect, reconnect, connect, and close events.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - cfg: connection configuration parameters
//
// Returns error if connection or JetStream initialization fails
func (c *NatsConn) GmqConnect(_ context.Context, cfg map[string]any) (err error) {

	config := new(natsConfig)
	if err = utils.MapToStruct(config, cfg); err != nil {
		natsLogger.Error("config parse failed", "error", err)
		return fmt.Errorf("%s: config: %w", natsPluginName, err)
	}
	if config.Addr == "" {
		natsLogger.Error("config validation failed", "error", types.ErrConfigAddrRequired)
		return fmt.Errorf("%s: config: %w", natsPluginName, types.ErrConfigAddrRequired)
	}
	if config.Port == "" {
		natsLogger.Error("config validation failed", "error", types.ErrConfigPortRequired)
		return fmt.Errorf("%s: config: %w", natsPluginName, types.ErrConfigPortRequired)
	}

	// set connection options
	opts := []nats.Option{
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			natsLogger.Error("connection disconnected", "url", nc.ConnectedUrl(), "error", err)
			if c.setSubscribed != nil {
				c.setSubscribed(false)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			natsLogger.Info("connection reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ConnectHandler(func(nc *nats.Conn) {
			natsLogger.Info("connection established", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			natsLogger.Info("connection closed")
			if c.setSubscribed != nil {
				c.setSubscribed(false)
			}
		}),
	}
	if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	}

	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%s", config.Addr, config.Port), opts...)
	if err != nil {
		natsLogger.Error("connect failed", "addr", config.Addr, "port", config.Port, "error", err)
		return fmt.Errorf("%s: connect: %w", natsPluginName, err)
	}

	js, err := conn.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		conn.Close()
		natsLogger.Error("jetstream init failed", "error", err)
		return fmt.Errorf("%s: jetstream: %w", natsPluginName, err)
	}

	c.conn = conn
	c.js = js
	return nil
}

// GmqClose closes the NATS connection.
// Safe to call multiple times
func (c *NatsConn) GmqClose(_ context.Context) error {
	// Clear external callback reference to avoid memory leak
	c.setSubscribed = nil

	if c.conn == nil {
		natsLogger.Debug("connection already nil")
		return nil
	}

	c.conn.Close()
	c.conn = nil
	c.js = nil
	natsLogger.Info("connection closed")
	return nil
}

// GmqPublish publishes a message to NATS JetStream.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must be *NatsPubMessage)
//
// Returns error if publish fails
func (c *NatsConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	cfg, ok := msg.(*NatsPubMessage)
	if !ok {
		natsLogger.Error("publish:invalid message type", "expected", "*NatsPubMessage", "plugin", natsPluginName)
		return fmt.Errorf("%s: publish: %w: expected *NatsPubMessage", natsPluginName, types.ErrInvalidMessageType)
	}
	if err = c.createPublish(ctx, cfg.Topic, cfg.Durable, 0, cfg.Data); err != nil {
		natsLogger.Error("publish failed", "topic", cfg.Topic, "error", err)
		return err
	}
	return nil
}

// GmqPublishDelay publishes a delayed message to NATS JetStream.
// The message will be delivered after the specified delay period.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: delayed message to publish (must be *NatsPubDelayMessage)
//
// Returns error if publish fails
func (c *NatsConn) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) (err error) {
	cfg, ok := msg.(*NatsPubDelayMessage)
	if !ok {
		natsLogger.Error("publish delay:invalid message type", "expected", "*NatsPubDelayMessage", "plugin", natsPluginName)
		return fmt.Errorf("%s: publish delay: %w: expected *NatsPubDelayMessage", natsPluginName, types.ErrInvalidMessageType)
	}
	if err = c.createPublish(ctx, cfg.Topic, cfg.Durable, cfg.DelaySeconds, cfg.Data); err != nil {
		natsLogger.Error("publish delay failed", "topic", cfg.Topic, "delay", cfg.DelaySeconds, "error", err)
		return err
	}
	return nil
}

// createPublish publishes a message with optional delay to NATS JetStream.
// Creates or updates the stream if necessary, then publishes the message.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - topic: message subject/topic
//   - durable: whether to persist messages
//   - delayTime: delay time in seconds (0 for immediate delivery)
//   - data: message payload
//
// Returns error if stream creation or publish fails
func (c *NatsConn) createPublish(ctx context.Context, topic string, durable bool, delayTime int, data any) (err error) {
	// create Stream
	if _, _, err := c.createStream(ctx, topic, durable, delayTime > 0); err != nil {
		natsLogger.Error("create stream failed", "topic", topic, "durable", durable, "error", err)
		return fmt.Errorf("%s: create_stream: %w", natsPluginName, err)
	}
	// build message
	m := nats.NewMsg(topic)
	payload, err := json.Marshal(data)
	if err != nil {
		natsLogger.Error("marshal data failed", "error", err)
		return fmt.Errorf("%s: marshal: %w", natsPluginName, err)
	}
	m.Data = payload

	// delayed message
	if delayTime > 0 {
		// use @at to specify specific delay time, not @every for repeated execution
		futureTime := time.Now().Add(time.Duration(delayTime) * time.Second).Format(time.RFC3339Nano)
		m.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", futureTime))
		m.Subject = topic + ".schedule"
		m.Header.Set("Nats-Schedule-Target", topic)
	}

	// publish message
	if _, err = c.js.PublishMsg(m, []nats.PubOpt{nats.Context(ctx)}...); err != nil {
		natsLogger.Error("publish message failed", "topic", topic, "error", err)
		return fmt.Errorf("%s: publish: %w", natsPluginName, err)
	}

	return nil
}

// GmqSubscribe subscribes to NATS messages using JetStream consumer.
// Creates stream and durable consumer if they don't exist.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: subscription configuration (must be *NatsSubMessage)
//
// Returns error if subscription fails
func (c *NatsConn) GmqSubscribe(ctx context.Context, msg types.Subscribe) (err error) {
	cfg, ok := msg.GetSubMsg().(*NatsSubMessage)
	if !ok {
		natsLogger.Error("subscribe:invalid message type", "expected", "*NatsSubMessage", "plugin", natsPluginName)
		return fmt.Errorf("%s: subscribe: %w: expected *NatsSubMessage", natsPluginName, types.ErrInvalidMessageType)
	}
	// create Stream
	streamName, _, err := c.createStream(ctx, cfg.Topic, cfg.Durable, cfg.IsDelayMsg)
	if err != nil {
		natsLogger.Error("create stream failed", "topic", cfg.Topic, "error", err)
		return fmt.Errorf("%s: create_stream: %w", natsPluginName, err)
	}

	// build Durable Consumer configuration
	consumerConfig := &nats.ConsumerConfig{
		Durable:        cfg.ConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxAckPending:  cfg.FetchCount,
		FilterSubject:  cfg.Topic,
		DeliverSubject: fmt.Sprintf("DELIVER.%s.%s", streamName, cfg.ConsumerName),
		DeliverPolicy:  nats.DeliverAllPolicy,
		MaxDeliver:     3,
		BackOff:        []time.Duration{time.Second, 3 * time.Second, 6 * time.Second},
	}

	// create Durable Consumer
	if _, err = c.js.AddConsumer(streamName, consumerConfig, []nats.JSOpt{nats.Context(ctx)}...); err != nil {
		// if Consumer already exists, ignore error
		if !strings.Contains(err.Error(), "consumer name already in use") {
			natsLogger.Error("add consumer failed", "stream", streamName, "consumer", cfg.ConsumerName, "error", err)
			return fmt.Errorf("%s: add_consumer: %w", natsPluginName, err)
		}
		natsLogger.Debug("consumer already exists", "stream", streamName, "consumer", cfg.ConsumerName)
	}

	// configure subscription options - bind to created Durable Consumer
	subOpts := []nats.SubOpt{
		nats.Context(ctx),
		nats.Bind(streamName, cfg.ConsumerName),
		nats.ManualAck(), // manual acknowledgment mode
	}

	// use Subscribe to create push subscription
	_, err = c.js.Subscribe(cfg.Topic, func(natsMsg *nats.Msg) {
		if err = msg.GetAckHandleFunc()(ctx, &types.AckMessage{
			MessageData:     natsMsg.Data,
			AckRequiredAttr: natsMsg,
		}); err != nil {
			natsLogger.Error("message handler failed", "subject", natsMsg.Subject, "error", err)
		}
	}, subOpts...)
	if err != nil {
		natsLogger.Error("subscribe failed", "topic", cfg.Topic, "consumer", cfg.ConsumerName, "error", err)
		return fmt.Errorf("%s: subscribe: %w", natsPluginName, err)
	}
	natsLogger.Info("subscribe success", "topic", cfg.Topic, "consumer", cfg.ConsumerName)
	return nil
}

// createStream creates or updates NATS stream for message persistence.
// Determines stream name and storage type based on durable and delay configuration.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - topic: message subject/topic
//   - durable: whether to use file storage (persistent) or memory storage (ephemeral)
//   - isDelayMsg: whether this is a delayed message stream
//
// Returns stream name, storage type, and error
func (c *NatsConn) createStream(_ context.Context, topic string, durable, isDelayMsg bool) (string, nats.StorageType, error) {

	// build stream name and storage type
	// use topic name as unique identifier to avoid conflicts
	// replace special characters in topic name with underscores
	safeTopicName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, topic)

	var streamName string
	var storage nats.StorageType

	// determine storage type based on durable and isDelayMsg
	if isDelayMsg {
		if durable {
			streamName, storage = fmt.Sprintf("delay_file_%s", safeTopicName), nats.FileStorage
		} else {
			streamName, storage = fmt.Sprintf("delay_memory_%s", safeTopicName), nats.MemoryStorage
		}
	} else {
		if durable {
			streamName, storage = fmt.Sprintf("ordinary_file_%s", safeTopicName), nats.FileStorage
		} else {
			streamName, storage = fmt.Sprintf("ordinary_memory_%s", safeTopicName), nats.MemoryStorage
		}
	}

	// build stream configuration
	// for delayed messages, need to include two subjects:
	// 1. subject.schedule - for sending scheduled messages
	// 2. subject - for actual delivery target
	subjects := []string{topic}
	if isDelayMsg {
		subjects = []string{topic, topic + ".schedule"}
	}
	jsConfig := &streamConfig{
		Name:              streamName,
		Subjects:          subjects,
		AllowMsgSchedules: isDelayMsg, // delayed message core switch
		Storage:           storage,
		Discard:           nats.DiscardOld,    // delete old messages when limit reached
		MaxMsgs:           100000,             // keep up to 100k messages
		MaxAge:            7 * 24 * time.Hour, // keep messages for 7 days
		Retention:         nats.InterestPolicy,
		MaxConsumers:      -1,
	}

	// create stream
	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		natsLogger.Error("create stream failed", "stream", streamName, "error", err)
		return "", 0, fmt.Errorf("%s: create_stream: %w", natsPluginName, err)
	}

	natsLogger.Debug("stream created/updated", "stream", streamName, "storage", storage, "subjects", subjects)
	return streamName, storage, nil
}

// GmqAck acknowledges successful processing of a NATS message.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if acknowledgment fails
func (c *NatsConn) GmqAck(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
	if !ok {
		natsLogger.Error("ack:invalid message type", "expected", "*nats.Msg", "plugin", natsPluginName)
		return fmt.Errorf("%s: ack: %w: expected *nats.Msg", natsPluginName, types.ErrInvalidMessageType)
	}
	if err := msgCfg.Ack(); err != nil {
		natsLogger.Error("ack failed", "error", err)
		return fmt.Errorf("%s: ack: %w", natsPluginName, err)
	}
	return nil
}

// GmqNak negatively acknowledges a NATS message, indicating processing failure.
// The message will be redelivered according to consumer configuration.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if negative acknowledgment fails
func (c *NatsConn) GmqNak(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
	if !ok {
		natsLogger.Error("nak:invalid message type", "expected", "*nats.Msg", "plugin", natsPluginName)
		return fmt.Errorf("%s: nak: %w: expected *nats.Msg", natsPluginName, types.ErrInvalidMessageType)
	}
	if err := msgCfg.Nak(); err != nil {
		natsLogger.Error("nak failed", "error", err)
		return fmt.Errorf("%s: nak: %w", natsPluginName, err)
	}
	return nil
}

// streamConfig stream configuration (simplified version, only contains fields actually used)
type streamConfig struct {
	Name              string               `json:"name"`
	Subjects          []string             `json:"subjects,omitempty"`
	Storage           nats.StorageType     `json:"storage"`
	Discard           nats.DiscardPolicy   `json:"discard"`
	AllowMsgSchedules bool                 `json:"allow_msg_schedules"`
	MaxConsumers      int                  `json:"max_consumers"`
	MaxMsgs           int64                `json:"max_msgs,omitempty"`
	MaxAge            time.Duration        `json:"max_age,omitempty"`
	Retention         nats.RetentionPolicy `json:"retention"`
}

// NATS JetStream API templates for stream management
const (
	jSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s" // Stream create API template
	jSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s" // Stream update API template
)

// jsStreamRequest sends a Stream API request to NATS JetStream.
// This is a low-level function that directly calls NATS JetStream REST API.
// Parameters:
//   - nc: NATS connection
//   - apiTemplate: API template string (e.g., "$JS.API.STREAM.CREATE.%s")
//   - cfg: stream configuration to send
//
// Returns error if the request fails or the response indicates an error
func jsStreamRequest(nc *nats.Conn, apiTemplate string, cfg *streamConfig) error {
	// Local response structure to avoid race conditions in concurrent calls
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error,omitempty"`
	}

	j, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	msg, err := nc.Request(fmt.Sprintf(apiTemplate, cfg.Name), j, time.Second*3)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("JS API error: %s", resp.Error.Description)
	}
	return nil
}

// jsStreamCreate creates a NATS JetStream stream using direct API call.
// Automatically attempts to update if stream already exists.
// Parameters:
//   - nc: NATS connection
//   - cfg: stream configuration
//
// Returns error if create and update both fail
func jsStreamCreate(nc *nats.Conn, cfg *streamConfig) (err error) {
	if err = jsStreamRequest(nc, jSApiStreamCreateT, cfg); err != nil {
		if strings.Contains(err.Error(), "10058") {
			// Stream already exists, try to update
			return jsStreamUpdate(nc, cfg)
		} else if strings.Contains(err.Error(), "subjects overlap") {
			// Subjects conflict, means another Stream already uses the same subjects
			return fmt.Errorf("subjects overlap with an existing stream, different durable/delay config for same queue")
		}
	}
	return err
}

// jsStreamUpdate updates an existing NATS JetStream stream.
// Parameters:
//   - nc: NATS connection
//   - cfg: stream configuration
//
// Returns error if update fails
func jsStreamUpdate(nc *nats.Conn, cfg *streamConfig) error {
	// Local response structure to avoid race conditions in concurrent calls
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error,omitempty"`
	}

	j, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	msg, err := nc.Request(fmt.Sprintf(jSApiStreamUpdateT, cfg.Name), j, time.Second*3)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("JS API error: %s", resp.Error.Description)
	}
	return nil
}
