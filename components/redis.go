package components

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/utils"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"log"
	"reflect"
	"strings"
	"time"
)

type RedisPubMessage struct {
	core.PubMessage
}

type RedisPubDelayMessage struct {
	core.PubMessage
}

type RedisSubMessage struct {
	core.SubMessage[any]
}

func (n RedisPubMessage) GetGmqPublishMsgType() {
	//TODO implement me
	panic("implement me")
}

func (n RedisPubDelayMessage) GetGmqPublishDelayMsgType() {
	//TODO implement me
	panic("implement me")
}

// redisMsg Redis消息队列实现
type redisMsg struct {
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
func (c *redisMsg) GmqPing(ctx context.Context) bool {
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
func (c *redisMsg) GmqConnect(ctx context.Context) (err error) {
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
func (c *redisMsg) GmqClose(ctx context.Context) (err error) {
	if c.redisConn != nil {
		err = c.redisConn.Close()
		c.redisConn = nil
	}
	return err
}

// GmqPublish 发布消息
func (c *redisMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RedisPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RedisPubMessage")
	}
	if cfg.QueueName == "" {
		return fmt.Errorf("must provide queue name")
	}
	if utils.IsEmpty(cfg.Data) {
		return fmt.Errorf("must provide data")
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
LOOP:
	if c.redisConn == nil {
		time.Sleep(2 * time.Second)
		goto LOOP
	}
	// 2. 调用专用 XAdd 方法
	// 直接返回消息 ID（string 类型）和错误，无需额外解析
	msgID, err := c.redisConn.XAdd(ctx, addArgs).Result()
	if err != nil {
		if !c.GmqPing(ctx) {
			time.Sleep(2 * time.Second)
			goto LOOP
		} else {
			return fmt.Errorf("publish message failed：%v\n", err)
		}
	}
	log.Println(fmt.Sprintf("Redis [%s] publish message success: queue=%s, msgID=%s", c.redisDsName, cfg.QueueName, msgID))
	return
}

// GmqPublishDelay 发布延迟消息
func (c *redisMsg) GmqPublishDelay(_ context.Context, _ core.PublishDelay) (err error) {
	return fmt.Errorf("redis not support delay message")
}

// GmqSubscribe 订阅Redis消息
func (c *redisMsg) GmqSubscribe(ctx context.Context, msg any) (result interface{}, err error) {
	cfg, ok := msg.(*RedisSubMessage)
	if !ok {
		return nil, fmt.Errorf("invalid message type, expected *RabbitMQPubMessage")
	}
	if cfg.QueueName == "" {
		return nil, fmt.Errorf("must provide queue name")
	}
	if cfg.ConsumerName == "" {
		return nil, fmt.Errorf("must provide consumer name")
	}
	if cfg.FetchCount <= 0 {
		return nil, fmt.Errorf("must provide fetch count")
	}
	if cfg.HandleFunc == nil {
		return nil, fmt.Errorf("must provide handle func")
	}

	go func() {
		err = func() error {
		LOOP:
			if c.redisConn == nil {
				time.Sleep(2 * time.Second)
				goto LOOP
			}
			group := fmt.Sprintf("%s_default_group", cfg.ConsumerName)
			// 1. 调用专用方法 XGroupCreateMkStream
			// 参数：ctx, 流名称, 消费者组名称, 起始 ID（"0" 表示从流的第一条消息开始消费）
			// 底层自动拼接 "XGROUP CREATE key group 0 MKSTREAM" 命令，无需手动传 MKSTREAM
			_, err = c.redisConn.XGroupCreateMkStream(ctx, cfg.QueueName, group, "0").Result()
			if err != nil {
				errStr := err.Error()
				if !strings.Contains(errStr, "BUSYGROUP") && !strings.Contains(errStr, "already exists") {
					if !c.GmqPing(ctx) {
						time.Sleep(2 * time.Second)
						goto LOOP
					} else {
						return fmt.Errorf("subscribe message failed：%v\n", err)
					}
				}
			}
			// 构造结构化参数（参数含义清晰，无需记命令顺序）
			readArgs := &redis.XReadGroupArgs{
				Group:    group,                        // 消费者组名称
				Consumer: cfg.ConsumerName,             // 消费者名称
				Count:    cast.ToInt64(cfg.FetchCount), // 每次拉取的消息数
				Block:    0,                            // 阻塞时间（0 = 永久阻塞，time.Millisecond 单位）
				Streams:  []string{cfg.QueueName, ">"}, // 消费的流 + 起始 ID（> 表示消费新消息）
				NoAck:    true,                         // 是否自动确认（false = 需要手动 XACK，默认值）
			}
		LOOPREAD:
			for {
				if c.redisConn == nil {
					time.Sleep(2 * time.Second)
					goto LOOPREAD
				}
				// 调用专用 XReadGroup 方法，返回结构化结果
				// 结果类型：[]redis.XStream（每个 XStream 对应一个流的消息）
				streams, err := c.redisConn.XReadGroup(ctx, readArgs).Result()
				select {
				case <-ctx.Done():
					return nil
				default:
					if err != nil {
						if errors.Is(err, redis.Nil) || !c.GmqPing(ctx) {
							time.Sleep(2 * time.Second)
							goto LOOPREAD
						} else {
							log.Printf("❌ 订阅消息失败: %v", err)
							return fmt.Errorf("subscribe message failed：%v\n", err)
						}
					}
					// 解析结构化结果（示例）
					for _, stream := range streams {
						for _, msg := range stream.Messages {
							data, err := json.Marshal(msg.Values)
							if err != nil {
								// 处理反序列化错误
								log.Printf("JSON反序列化失败: %v", err)
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
							}
						}
					}
				}
			}
		}()
	}()

	return
}

// GetMetrics 获取基础监控指标
func (c *redisMsg) GetMetrics(ctx context.Context) *core.Metrics {
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
