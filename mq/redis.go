package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
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
	core.SubMessage[any]
}

// RedisMsg Redis消息队列实现
type RedisMsg struct {
	redisConnURL         string
	redisUsername        string
	redisPassword        string
	redisDb              int
	redisPoolSize        int
	redisMinIdleConns    int
	redisMaxActiveConns  int
	redisMaxRetries      int
	redisDialTimeout     int
	redisReadTimeout     int
	redisWriteTimeout    int
	redisPoolTimeout     int
	redisConnMaxIdleTime int
	redisConnMaxLifetime int
	redisDsName          string
	redisConn            *redis.Client
	redisConnectedAt     time.Time
	redisLastPingLatency float64
}

// GmqPing 检测Redis连接状态
func (c *RedisMsg) GmqPing(ctx context.Context) bool {
	if c.redisConn == nil {
		return false
	}

	pong, err := c.redisConn.Ping(ctx).Result()
	if err != nil || pong != "PONG" {
		return false
	}

	start := time.Now()
	c.redisLastPingLatency = float64(time.Since(start).Milliseconds())

	return true
}

// GmqConnect 连接Redis服务器
func (c *RedisMsg) GmqConnect(ctx context.Context) (err error) {
	if c.redisConnURL == "" {
		return fmt.Errorf("redis connect address is empty")
	}

	// 安全地关闭旧连接
	if c.redisConn != nil {
		c.redisConn.Close()
	}
	options := redis.Options{
		Addr:            c.redisConnURL,
		DB:              c.redisDb,
		PoolSize:        c.redisPoolSize,
		MinIdleConns:    c.redisMinIdleConns,
		MaxActiveConns:  c.redisMaxActiveConns,
		MaxRetries:      c.redisMaxRetries,
		DialTimeout:     time.Duration(c.redisDialTimeout) * time.Second,
		ReadTimeout:     time.Duration(c.redisReadTimeout) * time.Second,
		WriteTimeout:    time.Duration(c.redisWriteTimeout) * time.Second,
		PoolTimeout:     time.Duration(c.redisPoolTimeout) * time.Second,
		ConnMaxIdleTime: time.Duration(c.redisConnMaxIdleTime) * time.Second,
		ConnMaxLifetime: time.Duration(c.redisConnMaxLifetime) * time.Second,
	}
	if c.redisUsername != "" && c.redisPassword != "" {
		options.Username = c.redisUsername
		options.Password = c.redisPassword
	}
	// 连接 Redis
	newConn := redis.NewClient(&options)

	// 测试连接
	pong, err := newConn.Ping(ctx).Result()
	if err != nil {
		log.Println(fmt.Sprintf("Redis [%s] connect failed: %s", c.redisDsName, err))
		return fmt.Errorf("redis [%s] connect failed: %w", c.redisDsName, err)
	}

	if pong != "PONG" {
		log.Println(fmt.Sprintf("Redis [%s] ping response invalid", c.redisDsName))
		return fmt.Errorf("redis [%s] ping response invalid", c.redisDsName)
	}

	c.redisConn = newConn
	c.redisConnectedAt = time.Now()
	log.Println(fmt.Sprintf("Redis [%s] connect success: %s", c.redisDsName, c.redisConnURL))
	return nil
}

// GmqClose 关闭Redis连接
func (c *RedisMsg) GmqClose(ctx context.Context) (err error) {
	if c.redisConn != nil {
		err = c.redisConn.Close()
		c.redisConn = nil
	}
	return err
}

// GmqPublish 发布消息
func (c *RedisMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisPubMessage")
	}
	// 将数据转换为 map[string]interface{}
	values := make(map[string]interface{})
	switch v := cfg.Data.(type) {
	case map[string]interface{}:
		values = v
	case map[string]string:
		for k, val := range v {
			values[k] = val
		}
	default:
		// 使用反射将结构体转换为 map
		rv := reflect.ValueOf(cfg.Data)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			rt := rv.Type()
			for i := 0; i < rv.NumField(); i++ {
				field := rt.Field(i)
				fieldValue := rv.Field(i)
				// 获取字段名（优先使用 json tag）
				fieldName := field.Name
				if tag := field.Tag.Get("json"); tag != "" && tag != "-" {
					fieldName = strings.Split(tag, ",")[0]
				}
				// 只导出可访问的字段
				if fieldValue.CanInterface() {
					values[fieldName] = fieldValue.Interface()
				}
			}
		} else if fieldValue, ok := cfg.Data.(interface{ String() string }); ok {
			// 如果数据实现了 String() 方法，使用字符串
			values["data"] = fieldValue.String()
		} else {
			// 其他情况直接使用 map 转换
			values = cast.ToStringMap(cfg.Data)
		}
	}
	// 检查转换后的 map 是否为空
	if len(values) == 0 {
		return fmt.Errorf("data cannot be empty after conversion")
	}
	// 1. 构造 XAdd 参数结构体（类型安全，参数含义清晰）
	addArgs := &redis.XAddArgs{
		Stream: cfg.QueueName, // 流名称
		ID:     "*",           // 自动生成 ID
		Values: values,
	}
	if c.redisConn == nil {
		return fmt.Errorf("redis connection is nil")
	}
	// 2. 调用专用 XAdd 方法
	// 直接返回消息 ID（string 类型）和错误，无需额外解析
	msgID, err := c.redisConn.XAdd(ctx, addArgs).Result()
	if err != nil {
		return fmt.Errorf("publish message failed：%v\n", err)
	}
	log.Println(fmt.Sprintf("Redis [%s] publish message success: queue=%s, msgID=%s", c.redisDsName, cfg.QueueName, msgID))
	return
}

// GmqPublishDelay 发布延迟消息
func (c *RedisMsg) GmqPublishDelay(_ context.Context, _ core.PublishDelay) (err error) {
	return fmt.Errorf("redis not support delay message")
}

// GmqSubscribe 订阅Redis消息
func (c *RedisMsg) GmqSubscribe(ctx context.Context, msg any) (err error) {
	cfg, ok := msg.(*RedisSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisSubMessage")
	}
	if cfg.HandleFunc == nil {
		return fmt.Errorf("must provide handle func")
	}

	if c.redisConn == nil {
		return fmt.Errorf("redis connection is nil")
	}
	group := fmt.Sprintf("%s_default_group", cfg.ConsumerName)
	_, err = c.redisConn.XGroupCreateMkStream(ctx, cfg.QueueName, group, "0").Result()
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
	if cfg.AutoAck {
		readArgs.NoAck = true
	}
	for {
		if c.redisConn == nil {
			return fmt.Errorf("redis connection is nil")
		}
		streams, err := c.redisConn.XReadGroup(ctx, readArgs).Result()
		if err != nil {
			return fmt.Errorf("subscribe message failed：%v\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
			// 获取重试次数配置
			maxDeliver := core.MsgRetryDeliver
			retryDelay := core.MsgRetryDelay

			// 解析结构化结果（示例）
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					// 从消息中获取当前重试次数
					currentRetry := 0
					if retryCount, ok := msg.Values["retry_count"]; ok {
						currentRetry, _ = cast.ToIntE(retryCount)
					}

					data, err := json.Marshal(msg.Values)
					if err != nil {
						// 处理反序列化错误
						log.Printf("JSON反序列化失败: %v", err)
						err = c.sendToDeadLetter(ctx, cfg.QueueName, msg.ID, string(data), "JSON反序列化失败")
						if err != nil {
							log.Printf("❌ 发送到死信队列失败: %v", err)
							return err
						}
						// 确认原消息
						_, _ = c.redisConn.XAck(ctx, cfg.QueueName, group, msg.ID).Result()
						continue
					}

					// 调用用户提供的处理函数处理业务逻辑
					if err := cfg.HandleFunc(ctx, data); err == nil {
						if !cfg.AutoAck {
							// 业务处理完后，手动确认消息（XACK）
							result, err := c.redisConn.XAck(ctx, cfg.QueueName, group, msg.ID).Result()
							if err != nil {
								log.Printf("❌ 确认消息失败: %v", err)
							} else {
								log.Printf("✅ 确认消息成功: %v", result)
							}
						}
					} else {
						// 处理失败，检查是否超过最大重试次数
						currentRetry++
						if currentRetry > maxDeliver {
							// 超过最大重试次数，发送到死信队列
							deadReason := fmt.Sprintf("处理失败，重试%d次后进入死信队列: %v", maxDeliver, err)
							err := c.sendToDeadLetter(ctx, cfg.QueueName, msg.ID, string(data), deadReason)
							if err != nil {
								log.Printf("❌ 发送到死信队列失败: %v", err)
								continue
							}
							// 确认原消息
							_, _ = c.redisConn.XAck(ctx, cfg.QueueName, group, msg.ID).Result()
						} else {
							// 未超过最大重试次数，等待后重新投递
							log.Printf("⚠️ 消息处理失败 (第%d次重试，最大%d次): %v", currentRetry, maxDeliver, err)
							time.Sleep(time.Duration(retryDelay) * time.Millisecond)
							// XCLAIM 重新投递消息给自己，实现重试
							claimed, err := c.redisConn.XClaim(ctx, &redis.XClaimArgs{
								Group:    group,
								Consumer: cfg.ConsumerName,
								MinIdle:  0,
								Messages: []string{msg.ID},
							}).Result()
							if err != nil {
								log.Printf("❌ 重新投递消息失败: %v", err)
								// 如果重新投递失败，也发送到死信队列
								deadReason := fmt.Sprintf("重新投递失败: %v", err)
								err := c.sendToDeadLetter(ctx, cfg.QueueName, msg.ID, string(data), deadReason)
								if err != nil {
									log.Printf("❌ 发送到死信队列失败: %v", err)
									continue
								}
								_, _ = c.redisConn.XAck(ctx, cfg.QueueName, group, msg.ID).Result()
							} else {
								// 更新重试次数
								for _, claimedMsg := range claimed {
									claimedMsg.Values["retry_count"] = currentRetry
								}
							}
						}
					}
				}
			}
		}
	}
}

// -------------------------- 核心：死信队列操作 --------------------------
// sendToDeadLetter 将消息移入死信Stream
func (c *RedisMsg) sendToDeadLetter(ctx context.Context, queueName, msgID, payload, reason string) error {
	if c.redisConn == nil {
		return fmt.Errorf("redis connection is nil")
	}

	// 死信队列名称规则：{queueName}.dlq
	deadLetterQueue := queueName + ".dlq"
	// 构建死信消息
	deadMsg := map[string]interface{}{
		"origin_msg_id": msgID,
		"origin_stream": queueName,
		"payload":       payload,
		"dead_reason":   reason,
		"dead_at":       time.Now().UnixMilli(),
	}

	// XAdd写入死信Stream
	_, err := c.redisConn.XAdd(ctx, &redis.XAddArgs{
		Stream: deadLetterQueue,
		Values: deadMsg,
		MaxLen: 10000, // 死信Stream最大长度
		Approx: true,
	}).Result()

	if err != nil {
		log.Printf("❌ send to dead letter failed: %v", err)
		return err
	}

	log.Printf("☠️ msg send to dead letter: stream=%s, msgID=%s, reason=%s", queueName, msgID, reason)
	return nil
}

// GmqGetDeadLetter 从死信队列查询所有消息（不删除，仅读取）
func (c *RedisMsg) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) (msgs []core.DeadLetterMsgDTO, err error) {
	if c.redisConn == nil {
		return nil, fmt.Errorf("redis connection is nil")
	}

	// 死信队列名称规则：{queueName}.dlq
	deadLetterQueue := queueName + ".dlq"

	// 设置默认限制
	if limit <= 0 {
		limit = 10
	}

	// 使用 XRANGE 获取死信队列中的所有消息
	streamMessages, err := c.redisConn.XRange(context.Background(), deadLetterQueue, "-", "+").Result()
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

	log.Printf("Redis [%s] get dead letter messages: queue=%s, count=%d", c.redisDsName, deadLetterQueue, len(msgs))
	return msgs, nil
}
func (c *RedisMsg) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:            "redis",
		Type:            "redis",
		ServerAddr:      c.redisConnURL,
		ConnectedAt:     c.redisConnectedAt.Format("2006-01-02 15:04:05"),
		LastPingLatency: c.redisLastPingLatency,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	// 计算运行时间
	if !c.redisConnectedAt.IsZero() {
		m.UptimeSeconds = int64(time.Since(c.redisConnectedAt).Seconds())
	}

	// 从 Redis 获取服务端统计信息
	if c.redisConn != nil && c.GmqPing(ctx) {
		info, err := c.redisConn.Info(ctx, "server", "clients", "memory", "stats").Result()
		if err == nil {
			m.ServerMetrics = map[string]interface{}{
				"info": info,
			}
		}

		// 获取数据库大小
		dbSize, err := c.redisConn.DBSize(ctx).Result()
		if err == nil {
			if m.ServerMetrics == nil {
				m.ServerMetrics = map[string]interface{}{}
			}
			m.ServerMetrics["dbSize"] = dbSize
		}
	}

	return m
}
