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
	"strings"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// Redis plugin name constant
const redisPluginName = "redis"

// RedisPubMessage represents a Redis publish message.
// Uses Redis Streams for message persistence and delivery.
type RedisPubMessage struct {
	types.PubMessage
}

// RedisPubDelayMessage represents a Redis delayed publish message (not supported in current implementation).
// Redis Streams does not have native delayed message support.
type RedisPubDelayMessage struct {
	types.PubDelayMessage
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
	conn *redis.Client // Redis client connection (go-redis client)
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
	logger := utils.LogWithPlugin(redisPluginName)

	config := new(redisConfig)
	if err = utils.MapToStruct(config, cfg); err != nil {
		logger.Error("config parse failed", "error", err)
		return fmt.Errorf("%s: config: %w", redisPluginName, err)
	}
	if config.Addr == "" {
		logger.Error("config validation failed", "error", types.ErrConfigAddrRequired)
		return fmt.Errorf("%s: config: %w", redisPluginName, types.ErrConfigAddrRequired)
	}
	if config.Port == "" {
		logger.Error("config validation failed", "error", types.ErrConfigPortRequired)
		return fmt.Errorf("%s: config: %w", redisPluginName, types.ErrConfigPortRequired)
	}

	// connection pool already exists, reuse existing connection (go-redis automatically manages connections internally)
	if c.conn != nil {
		logger.Debug("connection already exists, reusing")
		return nil
	}

	options := redis.Options{
		Addr: config.Addr + ":" + config.Port,
		DB:   config.Db,
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
	if config.MaxActiveConns > 0 {
		options.MaxActiveConns = config.MaxActiveConns
	}
	if config.MaxRetries > 0 {
		options.MaxRetries = config.MaxRetries
	}

	// connect to Redis
	c.conn = redis.NewClient(&options)
	logger.Info("connected successfully", "addr", config.Addr, "port", config.Port, "db", config.Db)
	return nil
}

// GmqClose closes the Redis connection.
// Safe to call multiple times
func (c *RedisConn) GmqClose(_ context.Context) (err error) {
	logger := utils.LogWithPlugin(redisPluginName)

	if c.conn != nil {
		if err = c.conn.Close(); err != nil {
			logger.Error("close connection failed", "error", err)
		}
		c.conn = nil
	}

	logger.Info("connection closed")
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
	logger := utils.LogWithPlugin(redisPluginName)

	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		logger.Error("invalid message type", "expected", "*RedisPubMessage", "got", fmt.Sprintf("%T", msg))
		return fmt.Errorf("%s: publish: %w: expected *RedisPubMessage", redisPluginName, types.ErrInvalidMessageType)
	}

	cfg.Topic = "gmq:stream:" + cfg.Topic
	toMap, err := utils.ConvertToMap(cfg.Data)
	if err != nil {
		logger.Error("convert data to map failed", "error", err)
		return fmt.Errorf("%s: convert_data: %w", redisPluginName, err)
	}

	// 1. build XAdd argument structure (type-safe, clear parameter meaning)
	addArgs := &redis.XAddArgs{
		Stream: cfg.Topic, // stream name
		ID:     "*",       // auto-generate ID
		Values: toMap,
	}

	// 2. call dedicated XAdd method
	if _, err = c.conn.XAdd(ctx, addArgs).Result(); err != nil {
		logger.Error("xadd failed", "stream", cfg.Topic, "error", err)
		return fmt.Errorf("%s: xadd: %w", redisPluginName, err)
	}

	logger.Debug("publish success", "stream", cfg.Topic)
	return nil
}

// GmqPublishDelay is not supported in Redis implementation.
// Redis Streams does not have native delayed message support.
// Returns ErrDelayMessageNotSupported error
func (c *RedisConn) GmqPublishDelay(_ context.Context, _ types.PublishDelay) (err error) {
	logger := utils.LogWithPlugin(redisPluginName)
	logger.Error("publish delay not supported")
	return fmt.Errorf("%s: %w", redisPluginName, types.ErrDelayMessageNotSupported)
}

// GmqSubscribe subscribes to Redis Stream messages using consumer group.
// Uses consumer group pattern for distributed consumption.
// Parameters:
//   - ctx: context for timeout/cancellation control
//   - sub: subscription configuration (must be *RedisSubMessage)
//
// Returns error if subscription fails
func (c *RedisConn) GmqSubscribe(ctx context.Context, sub types.Subscribe) (err error) {
	logger := utils.LogWithPlugin(redisPluginName)

	cfg, ok := sub.GetSubMsg().(*RedisSubMessage)
	if !ok {
		logger.Error("invalid message type", "expected", "*RedisSubMessage", "got", fmt.Sprintf("%T", sub.GetSubMsg()))
		return fmt.Errorf("%s: subscribe: %w: expected *RedisSubMessage", redisPluginName, types.ErrInvalidMessageType)
	}

	if c.conn == nil {
		logger.Error("connection is nil")
		return fmt.Errorf("%s: %w", redisPluginName, types.ErrConnectionNil)
	}

	cfg.Topic = "gmq:stream:" + cfg.Topic
	group := fmt.Sprintf("%s:default:group", cfg.ConsumerName)

	_, err = c.conn.XGroupCreateMkStream(ctx, cfg.Topic, group, "0").Result()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") && !strings.Contains(err.Error(), "already exists") {
			logger.Error("create consumer group failed", "stream", cfg.Topic, "group", group, "error", err)
			return fmt.Errorf("%s: create_group: %w", redisPluginName, err)
		}
		logger.Debug("consumer group already exists", "stream", cfg.Topic, "group", group)
	}

	// build structured parameters (clear parameter meaning, no need to remember command order)
	readArgs := &redis.XReadGroupArgs{
		Group:    group,                        // consumer group name
		Consumer: cfg.ConsumerName,             // consumer name
		Count:    cast.ToInt64(cfg.FetchCount), // number of messages to fetch each time
		Block:    0,                            // block time (0 = block forever, time.Millisecond unit)
		Streams:  []string{cfg.Topic, ">"},     // consumed stream + start ID (> means consume new messages)
	}

	logger.Info("subscribed successfully", "stream", cfg.Topic, "group", group, "consumer", cfg.ConsumerName)

	for {
		if c.conn == nil {
			logger.Error("connection is nil during consume loop")
			return fmt.Errorf("%s: %w", redisPluginName, types.ErrConnectionNil)
		}

		streams, err := c.conn.XReadGroup(ctx, readArgs).Result()
		if err != nil {
			logger.Error("xreadgroup failed", "stream", cfg.Topic, "error", err)
			return fmt.Errorf("%s: xreadgroup: %w", redisPluginName, err)
		}

		select {
		case <-ctx.Done():
			logger.Debug("subscription context done", "stream", cfg.Topic, "consumer", cfg.ConsumerName)
			return nil
		default:
			// parse structured results
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					message := types.AckMessage{
						MessageData: msg.Values,
						AckRequiredAttr: map[string]any{
							"MessageId": msg.ID,
							"Topic":     cfg.Topic,
							"Group":     group,
						},
					}
					if err = sub.GetAckHandleFunc()(ctx, &message); err != nil {
						logger.Error("message handler failed", "stream", cfg.Topic, "msgId", msg.ID, "error", err)
						continue
					}
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
	logger := utils.LogWithPlugin(redisPluginName)

	attr := cast.ToStringMap(msg.AckRequiredAttr)
	msgId := cast.ToString(attr["MessageId"])
	topic := cast.ToString(attr["Topic"])
	group := cast.ToString(attr["Group"])

	_, err := c.conn.XAck(ctx, topic, group, msgId).Result()
	if err != nil {
		logger.Error("xack failed", "stream", topic, "group", group, "msgId", msgId, "error", err)
		return fmt.Errorf("%s: xack: %w", redisPluginName, err)
	}

	_, err = c.conn.XDel(ctx, topic, msgId).Result()
	if err != nil {
		logger.Error("xdel failed", "stream", topic, "msgId", msgId, "error", err)
		return fmt.Errorf("%s: xdel: %w", redisPluginName, err)
	}

	return nil
}

// GmqNak is a no-op for Redis implementation.
// Redis Streams does not support negative acknowledgment in the same way as other message queues.
// Messages remain in PEL until explicitly acknowledged or expire
func (c *RedisConn) GmqNak(_ context.Context, _ *types.AckMessage) error {
	return nil
}
