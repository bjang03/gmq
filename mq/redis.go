package mq

import (
	"context"
	"fmt"
	"github.com/bjang03/gmq/utils"
	"log"
	"strings"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

type RedisPubMessage struct {
	core.PubMessage
}

type RedisSubMessage struct {
	core.SubMessage
}

// RedisConn Redis消息队列实现
type RedisConn struct {
	Url             string // Redis连接地址
	Db              int    // Redis数据库
	Username        string // Redis用户名
	Password        string // Redis密码
	PoolSize        int    // 连接池大小
	MinIdleConns    int    // 最小空闲连接数
	MaxActiveConns  int    // 最大活跃连接数
	MaxRetries      int    // 最大重试次数
	DialTimeout     int    // 拨号超时时间
	ReadTimeout     int    // 读超时时间
	WriteTimeout    int    // 写超时时间
	PoolTimeout     int    // 连接池超时时间
	ConnMaxIdleTime int    // 连接最大空闲时间
	ConnMaxLifetime int    // 连接最大存活时间
	conn            *redis.Client
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

// GmqConnect 连接Redis服务器
func (c *RedisConn) GmqConnect(ctx context.Context) (err error) {
	if c.Url == "" {
		return fmt.Errorf("redis connect address is empty")
	}

	// 连接池已存在，直接返回（go-redis 会自动管理连接）
	if c.conn != nil {
		return nil
	}
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
	log.Println(fmt.Sprintf("Redis connect success: %s", c.Url))
	return nil
}

// GmqClose 关闭Redis连接
func (c *RedisConn) GmqClose(ctx context.Context) (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	return err
}

// GmqPublish 发布消息
func (c *RedisConn) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisPubMessage")
	}
	toMap, err := utils.ConvertToMap(cfg.Data)
	if err != nil {
		return err
	}
	// 1. 构造 XAdd 参数结构体（类型安全，参数含义清晰）
	addArgs := &redis.XAddArgs{
		Stream: cfg.QueueName, // 流名称
		ID:     "*",           // 自动生成 ID
		Values: toMap,
	}
	// 2. 调用专用 XAdd 方法
	// 直接返回消息 ID（string 类型）和错误，无需额外解析
	msgID, err := c.conn.XAdd(ctx, addArgs).Result()
	if err != nil {
		return fmt.Errorf("publish message failed：%v\n", err)
	}
	log.Println(fmt.Sprintf("Redis publish message success: queue=%s, msgID=%s", cfg.QueueName, msgID))
	return
}

// GmqPublishDelay 发布延迟消息
func (c *RedisConn) GmqPublishDelay(_ context.Context, _ core.PublishDelay) (err error) {
	return fmt.Errorf("redis not support delay message")
}

// GmqSubscribe 订阅Redis消息
func (c *RedisConn) GmqSubscribe(ctx context.Context, sub core.Subscribe) (err error) {
	// 类型断言获取 NatsSubMessage 特定字段
	cfg, ok := sub.GetSubMsg().(*RedisSubMessage)
	if !ok {
		log.Printf("⚠️  invalid message type, expected *RedisSubMessage")
		return fmt.Errorf("invalid message type, expected *RedisSubMessage")
	}
	group := fmt.Sprintf("%s_default_group", cfg.ConsumerName)
	_, err = c.conn.XGroupCreateMkStream(ctx, cfg.QueueName, group, "0").Result()
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
		Streams:  []string{cfg.QueueName, ">"}, // 消费的流 + 起始 ID（> 表示消费新消息）
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
					message := core.AckMessage{
						MessageData: msg.Values,
						AckRequiredAttr: map[string]any{
							"MessageId": msg.ID,
							"QueueName": cfg.QueueName,
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
func (c *RedisConn) GmqAck(ctx context.Context, msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgId := cast.ToString(attr["MessageId"])
	queueName := cast.ToString(attr["QueueName"])
	group := cast.ToString(attr["Group"])
	_, err := c.conn.XAck(ctx, queueName, group, msgId).Result()
	return err
}

// GmqNak 否定确认消息 - Redis 不支持 Nak，需要手动重新投递或进入死信队列
func (c *RedisConn) GmqNak(ctx context.Context, msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgId := cast.ToString(attr["MessageId"])
	queueName := cast.ToString(attr["QueueName"])
	group := cast.ToString(attr["Group"])

	//TODO implement me
	// 发送到死信队列 cast.ToStringMap(attr["MessageData"])

	// 确认原消息（从 pending 列表中移除）
	_, err := c.conn.XAck(ctx, queueName, group, msgId).Result()
	return err
}

// GmqGetDeadLetter 从死信队列查询所有消息（不删除，仅读取）
func (c *RedisConn) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) (msgs []core.DeadLetterMsgDTO, err error) {
	if c.conn == nil {
		return nil, fmt.Errorf("redis connection is nil")
	}

	// 死信队列名称规则：{queueName}.dlq
	deadLetterQueue := queueName + ".dlq"

	// 设置默认限制
	if limit <= 0 {
		limit = 10
	}

	// 使用 XRANGE 获取死信队列中的所有消息
	streamMessages, err := c.conn.XRange(context.Background(), deadLetterQueue, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("get dead letter messages failed: %w", err)
	}

	// 转换为 DeadLetterMsgDTO 格式
	msgs = make([]core.DeadLetterMsgDTO, 0, len(streamMessages))
	for _, streamMsg := range streamMessages {
		// 解析消息字段
		deadLetterDTO := core.DeadLetterMsgDTO{
			MessageID: streamMsg.ID,
			QueueName: deadLetterQueue,
			Headers:   make(map[string]interface{}),
		}

		// 提取各个字段
		for key, value := range streamMsg.Values {
			switch key {
			case "origin_msg_id":
				deadLetterDTO.DeliveryTag, _ = cast.ToUint64E(value)
			case "origin_stream":
				deadLetterDTO.Exchange = value.(string)
			case "payload":
				deadLetterDTO.Body = value.(string)
			case "dead_reason":
				deadLetterDTO.DeadReason = value.(string)
			case "dead_at":
				if timestamp, err := cast.ToInt64E(value); err == nil {
					deadLetterDTO.Timestamp = time.UnixMilli(timestamp).Format("2006-01-02 15:04:05")
				}
			default:
				deadLetterDTO.Headers[key] = value
			}
		}

		// 如果没有设置死信原因，使用默认值
		if deadLetterDTO.DeadReason == "" {
			deadLetterDTO.DeadReason = "unknown"
		}

		msgs = append(msgs, deadLetterDTO)

		// 限制返回数量
		if len(msgs) >= limit {
			break
		}
	}

	return msgs, nil
}

func (c *RedisConn) GmqGetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:       "redis",
		Type:       "redis",
		ServerAddr: c.Url,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	// 从 Redis 获取服务端统计信息
	if c.conn != nil && c.GmqPing(ctx) {
		info, err := c.conn.Info(ctx, "server", "clients", "memory", "stats").Result()
		if err == nil {
			m.ServerMetrics = map[string]interface{}{
				"info": info,
			}
		}

		// 获取数据库大小
		dbSize, err := c.conn.DBSize(ctx).Result()
		if err == nil {
			if m.ServerMetrics == nil {
				m.ServerMetrics = map[string]interface{}{}
			}
			m.ServerMetrics["dbSize"] = dbSize
		}
	}

	return m
}
