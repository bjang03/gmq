// Package mq provides message queue implementations for the GMQ system.
//
// Redis Implementation Notes:
//   - This implementation uses Redis Streams for message persistence and delivery.
//   - Delayed messages are NOT supported because Redis Streams does not have native
//     delayed message support. GmqPublishDelay will return ErrDelayMessageNotSupported.
//   - Uses consumer group pattern for distributed consumption across multiple consumers.
//   - Topic names are prefixed with "gmq:stream:" to avoid conflicts with other data.
package mq

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// Redis plugin name constant
const redisPluginName = "redis"

// Package-level logger instance to avoid repeated heap allocation from utils.LogWithPlugin
// This eliminates the "redis string escapes to heap" issue in every log call
var redisLogger = utils.GetLogger().WithPlugin(redisPluginName)

// RedisPubMessage represents a Redis publish message.
// Uses Redis Streams for message persistence and delivery.
type RedisPubMessage struct {
	types.PubMessage
}

// RedisSubMessage represents a Redis subscription configuration.
// Uses consumer group pattern for distributed consumption across multiple consumers.
type RedisSubMessage struct {
	types.SubMessage
}

// RedisConn is the Redis message queue implementation using Redis Streams.
// Provides publish, subscribe, and acknowledgment capabilities.
// Note: Delayed messages are not supported in this implementation.
//
// Redis Streams Features:
//   - Message persistence in Redis Streams
//   - Consumer group pattern for distributed consumption
//   - Manual acknowledgment via XAck
//   - Automatic redelivery for unacknowledged messages in PEL (Pending Entries List)
//
// Topic Naming:
//   - All topics are prefixed with "gmq:stream:" to avoid conflicts
//   - Example: business topic "orders" becomes "gmq:stream:orders"
//
// Consumer Groups:
//   - Group name format: "{consumer_name}:default:group"
//   - Each consumer belongs to its own consumer group
//   - Messages are consumed with ">" ID to get only new messages
type RedisConn struct {
	conn          *redis.Client // Redis client connection (go-redis client)
	setSubscribed func(bool)
}

// SetSubscribedSetter sets the callback function to update subscription status.
// This is used to notify the proxy layer when subscription status changes.
// Parameters:
//   - setter: function that accepts a boolean status (true=subscribed, false=unsubscribed)
func (c *RedisConn) SetSubscribedSetter(setter func(bool)) {
	c.setSubscribed = setter
}

// redisConfig holds Redis connection configuration parameters.
// Used with MapToStruct to convert config map to struct.
type redisConfig struct {
	Addr           string // Redis server address
	Port           string // Redis server port
	Db             int    // Redis database number
	Username       string // authentication username
	Password       string // authentication password
	PoolSize       int    // connection pool size
	MinIdleConns   int    // minimum idle connections
	MaxActiveConns int    // maximum active connections (deprecated, use PoolSize)
	MaxRetries     int    // maximum retry attempts for commands
}

// GmqPing checks if Redis connection is alive.
// Returns true if client is initialized and server responds with PONG
func (c *RedisConn) GmqPing(ctx context.Context) bool {
	if c.conn == nil {
		return false
	}
	pong, err := c.conn.Ping(ctx).Result()
	if err != nil || pong != "PONG" {
		return false
	}
	return true
}

// GmqGetConn retrieves the Redis client connection.
// Returns the Redis client instance
func (c *RedisConn) GmqGetConn(_ context.Context) any {
	return c.conn
}

// GmqConnect establishes connection to Redis server.
// Reuses existing connection if already initialized.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - cfg: connection configuration parameters
//
// Returns error if configuration is invalid
func (c *RedisConn) GmqConnect(_ context.Context, cfg map[string]any) (err error) {
	config := new(redisConfig)
	if err = utils.MapToStruct(config, cfg); err != nil {
		redisLogger.Error("config parse failed", "error", err)
		return fmt.Errorf("%s: config: %w", redisPluginName, err)
	}
	if config.Addr == "" {
		redisLogger.Error("config validation failed", "error", types.ErrConfigAddrRequired)
		return fmt.Errorf("%s: config: %w", redisPluginName, types.ErrConfigAddrRequired)
	}
	if config.Port == "" {
		redisLogger.Error("config validation failed", "error", types.ErrConfigPortRequired)
		return fmt.Errorf("%s: config: %w", redisPluginName, types.ErrConfigPortRequired)
	}

	// Connection pool already exists, reuse existing connection (go-redis automatically manages connections internally)
	if c.conn != nil {
		redisLogger.Debug("connection already exists, reusing")
		return nil
	}
	netDialer := &net.Dialer{
		Timeout:   5 * time.Second,  // connection establishment timeout
		KeepAlive: 30 * time.Second, // TCP keep-alive interval (key parameter)
	}
	redisDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return netDialer.DialContext(ctx, network, addr)
	}
	options := redis.Options{
		Addr:         config.Addr + ":" + config.Port,
		DB:           config.Db,
		Dialer:       redisDialer,
		MinIdleConns: 2, // maintain 2 idle connections to avoid empty connection pool
	}
	if config.Username != "" && config.Password != "" {
		options.Username = config.Username
		options.Password = config.Password
	}
	if config.PoolSize > 0 {
		options.PoolSize = config.PoolSize
	}
	if config.MinIdleConns > 0 {
		options.MinIdleConns = config.MinIdleConns
	}
	if config.MaxRetries > 0 {
		options.MaxRetries = config.MaxRetries
	}

	// Connect to Redis server
	c.conn = redis.NewClient(&options)
	return nil
}

// GmqClose closes the Redis connection.
// Safe to call multiple times
func (c *RedisConn) GmqClose(_ context.Context) (err error) {
	// Clear external callback reference to avoid memory leak
	c.setSubscribed = nil

	if c.conn != nil {
		if err = c.conn.Close(); err != nil {
			redisLogger.Error("close connection failed", "error", err)
		}
		c.conn = nil
	}
	redisLogger.Info("connection closed")
	return err
}

// GmqPublish publishes a message to Redis Stream.
// Topic is prefixed with "gmq:stream:" to avoid conflicts with other data.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message to publish (must be *RedisPubMessage)
//
// Returns error if publish fails
func (c *RedisConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		redisLogger.Error("publish:invalid message type", "expected", "*RedisPubMessage", "plugin", redisPluginName)
		return fmt.Errorf("%s: publish: %w: expected *RedisPubMessage", redisPluginName, types.ErrInvalidMessageType)
	}

	cfg.Topic = "gmq:stream:" + cfg.Topic
	toMap, err := utils.ConvertToMap(cfg.Data)
	if err != nil {
		redisLogger.Error("convert data to map failed", "error", err)
		return fmt.Errorf("%s: convert_data: %w", redisPluginName, err)
	}

	// Build XAdd argument structure (type-safe, clear parameter meaning)
	addArgs := &redis.XAddArgs{
		Stream: cfg.Topic, // stream name
		ID:     "*",       // auto-generate ID
		Values: toMap,
	}

	// Call dedicated XAdd method to add message to stream
	if _, err = c.conn.XAdd(ctx, addArgs).Result(); err != nil {
		redisLogger.Error("xadd failed", "stream", cfg.Topic, "error", err)
		return fmt.Errorf("%s: xadd: %w", redisPluginName, err)
	}
	return nil
}

// GmqSubscribe subscribes to Redis Stream messages using consumer group.
// Uses consumer group pattern for distributed consumption.
// This method starts a background goroutine for non-blocking message consumption.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - sub: subscription configuration (must be *RedisSubMessage)
//
// Returns error if subscription fails
func (c *RedisConn) GmqSubscribe(ctx context.Context, sub types.Subscribe) (err error) {
	cfg, ok := sub.GetSubMsg().(*RedisSubMessage)
	if !ok {
		redisLogger.Error("subscribe:invalid message type", "expected", "*RedisSubMessage", "plugin", redisPluginName)
		return fmt.Errorf("%s: subscribe: %w: expected *RedisSubMessage", redisPluginName, types.ErrInvalidMessageType)
	}

	if c.conn == nil {
		redisLogger.Error("connection is nil")
		return fmt.Errorf("%s: %w", redisPluginName, types.ErrConnectionNil)
	}

	topic := "gmq:stream:" + cfg.Topic
	group := fmt.Sprintf("%s:default:group", cfg.ConsumerName)

	_, err = c.conn.XGroupCreateMkStream(ctx, topic, group, "0").Result()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") && !strings.Contains(err.Error(), "already exists") {
			redisLogger.Error("create consumer group failed", "stream", topic, "group", group, "error", err)
			return fmt.Errorf("%s: create_group: %w", redisPluginName, err)
		}
		redisLogger.Debug("consumer group already exists", "stream", topic, "group", group)
	}

	// Start consume loop - this will block until context is cancelled
	// Proxy layer already runs this in a goroutine, so we don't need another one
	err = c.consumeLoop(ctx, cfg, group, sub, topic)
	if err != nil && ctx.Err() == nil {
		// Error occurred but context wasn't cancelled, notify proxy layer
		redisLogger.Error("consume loop failed", "stream", topic, "consumer", cfg.ConsumerName, "error", err)
		if c.setSubscribed != nil {
			c.setSubscribed(false)
		}
		return err
	}
	redisLogger.Info("subscribe success", "topic", cfg.Topic, "consumer", cfg.ConsumerName)
	return nil
}

// consumeLoop runs in a background goroutine to consume messages from Redis Stream.
// It handles context cancellation gracefully and exits when context is cancelled.
// Blocks indefinitely on XReadGroup, waiting for new messages.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - cfg: subscription configuration
//   - group: consumer group name
//   - sub: subscription interface with callback handlers
//   - topic: stream topic name (with prefix)
func (c *RedisConn) consumeLoop(ctx context.Context, cfg *RedisSubMessage, group string, sub types.Subscribe, topic string) error {
	// Build structured parameters (clear parameter meaning, no need to remember command order)
	readArgs := &redis.XReadGroupArgs{
		Group:    group,                        // consumer group name
		Consumer: cfg.ConsumerName,             // consumer name
		Count:    cast.ToInt64(cfg.FetchCount), // number of messages to fetch each time
		Block:    -1,                           // block indefinitely for new messages
		Streams:  []string{topic, ">"},         // consumed stream + start ID (> means consume new messages)
	}

	for {
		select {
		case <-ctx.Done():
			redisLogger.Debug("subscription context done, exiting consume loop", "stream", topic, "consumer", cfg.ConsumerName)
			return ctx.Err()
		default:
		}

		if c.conn == nil {
			redisLogger.Error("connection is nil during consume loop, exiting", "stream", topic)
			return fmt.Errorf("%s: %w", redisPluginName, types.ErrConnectionNil)
		}

		streams, err := c.conn.XReadGroup(ctx, readArgs).Result()
		if err != nil {
			// Check if context was cancelled during the blocking call
			if ctx.Err() != nil {
				redisLogger.Debug("context cancelled during xreadgroup, exiting", "stream", topic, "consumer", cfg.ConsumerName)
				return ctx.Err()
			}
			// Check if it's a timeout (no new messages), just continue
			if err == redis.Nil || strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "key no longer exists") {
				continue
			}
			redisLogger.Error("xreadgroup failed, exiting consume loop", "stream", cfg.Topic, "error", err)
			return fmt.Errorf("xreadgroup failed, exiting consume loop %s: %w", cfg.Topic, err)
		}

		// Process messages
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				select {
				case <-ctx.Done():
					redisLogger.Debug("context cancelled during message processing, exiting", "stream", topic, "consumer", cfg.ConsumerName)
					return ctx.Err()
				default:
				}

				message := types.AckMessage{
					MessageData: msg.Values,
					AckRequiredAttr: map[string]any{
						"MessageId": msg.ID,
						"Topic":     topic,
						"Group":     group,
					},
				}
				if err = sub.GetAckHandleFunc()(ctx, &message); err != nil {
					redisLogger.Error("message handler failed", "stream", topic, "msgId", msg.ID, "error", err)
					continue
				}
			}
		}
	}
}

// GmqAck acknowledges successful processing of a Redis Stream message.
// Acknowledges and removes the message from the pending entries list (PEL).
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - msg: message acknowledgment information
//
// Returns error if acknowledgment fails
func (c *RedisConn) GmqAck(ctx context.Context, msg *types.AckMessage) error {
	attr := cast.ToStringMap(msg.AckRequiredAttr)
	msgId := cast.ToString(attr["MessageId"])
	topic := cast.ToString(attr["Topic"])
	group := cast.ToString(attr["Group"])

	_, err := c.conn.XAck(ctx, topic, group, msgId).Result()
	if err != nil {
		redisLogger.Error("xack failed", "stream", topic, "group", group, "msgId", msgId, "error", err)
		return fmt.Errorf("%s: xack: %w", redisPluginName, err)
	}

	_, err = c.conn.XDel(ctx, topic, msgId).Result()
	if err != nil {
		redisLogger.Error("xdel failed", "stream", topic, "msgId", msgId, "error", err)
		return fmt.Errorf("%s: xdel: %w", redisPluginName, err)
	}

	return nil
}
