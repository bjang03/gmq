package mq

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/bjang03/gmq/utils"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

type RedisPubMessage struct {
	types.PubMessage
}

type RedisPubDelayMessage struct {
	types.PubDelayMessage
}

type RedisSubMessage struct {
	types.SubMessage
}

// RedisConn Redis消息队列实现
type RedisConn struct {
	types.RedisConfig
	conn *redis.Client
}

// GmqPing 检测Redis连接状态
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

func (c *RedisConn) GmqGetConn(_ context.Context) any {
	return c.conn
}

// GmqConnect 连接Redis服务器
func (c *RedisConn) GmqConnect(_ context.Context) (err error) {
	// 验证连接配置（不包含 Name 验证）
	if err := c.RedisConfig.ValidateConn(); err != nil {
		return err
	}
	// 连接池已存在，直接返回（go-redis 会自动管理连接）
	if c.conn != nil {
		return nil
	}
	c.Url = c.Url + ":" + c.Port
	options := redis.Options{
		Addr: c.Url,
		DB:   c.Db,
	}
	if c.Username != "" && c.Password != "" {
		options.Username = c.Username
		options.Password = c.Password
	}
	if c.PoolSize > 0 {
		options.PoolSize = c.PoolSize
	}
	if c.MinIdleConns > 0 {
		options.MinIdleConns = c.MinIdleConns
	}
	if c.MaxActiveConns > 0 {
		options.MaxActiveConns = c.MaxActiveConns
	}
	if c.MaxRetries > 0 {
		options.MaxRetries = c.MaxRetries
	}
	if c.DialTimeout > 0 {
		options.DialTimeout = time.Duration(c.DialTimeout) * time.Second
	}
	if c.ReadTimeout > 0 {
		options.ReadTimeout = time.Duration(c.ReadTimeout) * time.Second
	}
	if c.WriteTimeout > 0 {
		options.WriteTimeout = time.Duration(c.WriteTimeout) * time.Second
	}
	if c.PoolTimeout > 0 {
		options.PoolTimeout = time.Duration(c.PoolTimeout) * time.Second
	}
	if c.ConnMaxIdleTime > 0 {
		options.ConnMaxIdleTime = time.Duration(c.ConnMaxIdleTime) * time.Second
	}
	if c.ConnMaxLifetime > 0 {
		options.ConnMaxLifetime = time.Duration(c.ConnMaxLifetime) * time.Second
	}
	// 连接 Redis
	c.conn = redis.NewClient(&options)
	return
}

// GmqClose 关闭Redis连接
func (c *RedisConn) GmqClose(_ context.Context) (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	return err
}

// GmqPublish 发布消息
func (c *RedisConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisPubMessage")
	}
	cfg.Topic = "gmq:stream:" + cfg.Topic
	toMap, err := utils.ConvertToMap(cfg.Data)
	if err != nil {
		return err
	}
	// 1. 构造 XAdd 参数结构体（类型安全，参数含义清晰）
	addArgs := &redis.XAddArgs{
		Stream: cfg.Topic, // 流名称
		ID:     "*",       // 自动生成 ID
		Values: toMap,
	}
	// 2. 调用专用 XAdd 方法
	if _, err = c.conn.XAdd(ctx, addArgs).Result(); err != nil {
		return fmt.Errorf("publish message failed：%v\n", err)
	}
	return
}

// GmqPublishDelay 发布延迟消息
func (c *RedisConn) GmqPublishDelay(_ context.Context, _ types.PublishDelay) (err error) {
	return fmt.Errorf("redis not support delay message")
}

// GmqSubscribe 订阅Redis消息
func (c *RedisConn) GmqSubscribe(ctx context.Context, sub types.Subscribe) (err error) {
	cfg, ok := sub.GetSubMsg().(*RedisSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisSubMessage")
	}
	cfg.Topic = "gmq:stream:" + cfg.Topic
	group := fmt.Sprintf("%s:default:group", cfg.ConsumerName)
	_, err = c.conn.XGroupCreateMkStream(ctx, cfg.Topic, group, "0").Result()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("subscribe message failed：%v\n", err)
		}
	}
	// 构造结构化参数（参数含义清晰，无需记命令顺序）
	readArgs := &redis.XReadGroupArgs{
		Group:    group,                        // 消费者组名称
		Consumer: cfg.ConsumerName,             // 消费者名称
		Count:    cast.ToInt64(cfg.FetchCount), // 每次拉取的消息数
		Block:    0,                            // 阻塞时间（0 = 永久阻塞，time.Millisecond 单位）
		Streams:  []string{cfg.Topic, ">"},     // 消费的流 + 起始 ID（> 表示消费新消息）
	}
	for {
		if c.conn == nil {
			return fmt.Errorf("redis connection is nil")
		}
		streams, err := c.conn.XReadGroup(ctx, readArgs).Result()
		if err != nil {
			return fmt.Errorf("subscribe message failed：%v\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
			// 解析结构化结果（示例）
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
						log.Printf("⚠️ Message processing failed: %v", err)
						continue
					}
				}
			}
		}
	}
}

// GmqAck 确认消息
func (c *RedisConn) GmqAck(ctx context.Context, msg *types.AckMessage) error {
	attr := cast.ToStringMap(msg.AckRequiredAttr)
	msgId := cast.ToString(attr["MessageId"])
	topic := cast.ToString(attr["Topic"])
	group := cast.ToString(attr["Group"])
	_, err := c.conn.XAck(ctx, topic, group, msgId).Result()
	return err
}

// GmqNak 否定确认消息 - Redis 不支持 Nak，需要手动重新投递或进入死信队列
func (c *RedisConn) GmqNak(_ context.Context, _ *types.AckMessage) error {
	return nil
}
