package mq

import (
	"context"
	"fmt"
	"github.com/bjang03/gmq/utils"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

type RedisPubMessage struct {
	core.PubMessage
}

type RedisPubDelayMessage struct {
	core.PubDelayMessage
}

type RedisSubMessage struct {
	core.SubMessage
}

// RedisConn Redis消息队列实现
type RedisConn struct {
	Url             string // Redis连接地址
	Port            string
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
func (c *RedisConn) GmqConnect(_ context.Context) (err error) {
	if c.Url == "" {
		return fmt.Errorf("redis connect address is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("redis connect port is empty")
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
func (c *RedisConn) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
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
func (c *RedisConn) GmqPublishDelay(_ context.Context, _ core.PublishDelay) (err error) {
	return fmt.Errorf("redis not support delay message")
}

// GmqSubscribe 订阅Redis消息
func (c *RedisConn) GmqSubscribe(ctx context.Context, sub core.Subscribe) (err error) {
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
					message := core.AckMessage{
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
func (c *RedisConn) GmqAck(ctx context.Context, msg *core.AckMessage) error {
	attr := cast.ToStringMap(msg.AckRequiredAttr)
	msgId := cast.ToString(attr["MessageId"])
	topic := cast.ToString(attr["Topic"])
	group := cast.ToString(attr["Group"])
	_, err := c.conn.XAck(ctx, topic, group, msgId).Result()
	return err
}

// GmqNak 否定确认消息 - Redis 不支持 Nak，需要手动重新投递或进入死信队列
func (c *RedisConn) GmqNak(_ context.Context, _ *core.AckMessage) error {
	return nil
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

func (c *RedisConn) GmqGetDeadLetter(ctx context.Context) ([]core.DeadLetterMsgDTO, error) {
	var deadLetterList []core.DeadLetterMsgDTO

	// 1. 获取所有StreamKey
	streamKeys, err := c.getAllStreamKeys(ctx)
	if err != nil {
		log.Printf("获取StreamKey失败: %v", err)
		return nil, fmt.Errorf("获取StreamKey失败: %w", err)
	}
	if len(streamKeys) == 0 {
		return deadLetterList, nil
	}

	// 2. 遍历每个Stream
	for _, streamKey := range streamKeys {
		// 2.1 获取消费组列表
		groups, err := c.getStreamGroups(ctx, streamKey)
		if err != nil {
			log.Printf("Stream [%s] 获取消费组失败: %v", streamKey, err)
			continue
		}
		if len(groups) == 0 {
			continue
		}

		// 2.2 遍历消费组
		for _, groupName := range groups {
			// 2.3 获取pending消息列表
			pendingList, err := c.getPendingMsgList(ctx, streamKey, groupName)
			if err != nil {
				log.Printf("消费组 [%s] 获取pending失败: %v", groupName, err)
				continue
			}
			if len(pendingList) == 0 {
				continue
			}

			// 筛选：仅处理空闲时间超过minIdleTime的消息
			filterPendingList := make([]redis.XPendingExt, 0)
			msgIDs := make([]string, 0)
			for _, p := range pendingList {
				if p.Idle >= 180*1000 { // Idle单位是毫秒
					filterPendingList = append(filterPendingList, p)
					msgIDs = append(msgIDs, p.ID)
				}
			}
			if len(filterPendingList) == 0 {
				continue
			}
			// 批量查询消息
			msgMap, err := c.batchGetStreamMsgByIDs(ctx, streamKey, msgIDs)
			if err != nil {
				log.Printf("批量查询失败: %v", err)
				continue
			}
			// 转换DTO
			for _, p := range filterPendingList {
				msgID := p.ID
				xMsg, ok := msgMap[msgID]
				if !ok {
					continue
				}
				// 调用修复后的转换函数
				deadLetter := convertXMsgToDeadLetterDTO(streamKey, msgID, xMsg, p.Idle)
				deadLetterList = append(deadLetterList, deadLetter)
			}
			if len(deadLetterList) > 0 {
				// TODO : 需要补充deadLetterList写入数据库

				// 死信队列中的消息保存成功后才能进行确认消息
				if err = c.conn.XAck(ctx, streamKey, groupName, msgIDs...).Err(); err != nil {
					log.Printf("批量确认消息失败: %v", err)
				} else {
					log.Println("批量确认消息成功，已清理pending列表")
				}
			}
		}
	}
	return deadLetterList, nil
}

func convertXMsgToDeadLetterDTO(streamKey, msgID string, xMsg redis.XMessage, idleMs time.Duration) core.DeadLetterMsgDTO {
	deadReason := fmt.Sprintf("消息处理超时未确认（空闲时间：%d秒）", idleMs/time.Second)
	timestampFormatted := ""
	if ts, err := strconv.ParseInt(strings.Split(msgID, "-")[0], 10, 64); err == nil {
		timestampFormatted = time.UnixMilli(ts).Format("2006-01-02 15:04:05")
	}
	return core.DeadLetterMsgDTO{
		MessageID:  msgID,
		Body:       xMsg.Values,
		Timestamp:  timestampFormatted,
		DeadReason: deadReason,
		Topic:      streamKey,
	}
}

func (c *RedisConn) getAllStreamKeys(ctx context.Context) ([]string, error) {
	var streamKeys []string
	cursor := uint64(0)
	count := int64(1000)
	matchPattern := "gmq:stream:*"
	for {
		keys, newCursor, err := c.conn.Scan(ctx, cursor, matchPattern, count).Result()
		if err != nil {
			return nil, fmt.Errorf("SCAN失败: %w", err)
		}
		for _, key := range keys {
			keyType, err := c.conn.Type(ctx, key).Result()
			if err != nil || keyType != "stream" {
				continue
			}
			streamKeys = append(streamKeys, key)
		}
		cursor = newCursor
		if cursor == 0 {
			break
		}
	}
	return streamKeys, nil
}

func (c *RedisConn) getStreamGroups(ctx context.Context, streamKey string) ([]string, error) {
	var groups []string
	xGroups, err := c.conn.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		return nil, fmt.Errorf("XInfoGroups失败: %w", err)
	}
	for _, g := range xGroups {
		groups = append(groups, g.Name)
	}
	return groups, nil
}

func (c *RedisConn) getPendingMsgList(ctx context.Context, streamKey, groupName string) ([]redis.XPendingExt, error) {
	pendingList, err := c.conn.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("XPendingExt失败: %w", err)
	}
	return pendingList, nil
}

func (c *RedisConn) batchGetStreamMsgByIDs(ctx context.Context, streamKey string, msgIDs []string) (map[string]redis.XMessage, error) {
	msgMap := make(map[string]redis.XMessage)
	if len(msgIDs) == 0 {
		return msgMap, nil
	}

	minID := msgIDs[0]
	maxID := msgIDs[0]
	for _, id := range msgIDs {
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
	}

	msgs, err := c.conn.XRange(ctx, streamKey, minID, maxID).Result()
	if err != nil {
		return nil, fmt.Errorf("XRANGE失败: %w", err)
	}

	for _, msg := range msgs {
		msgMap[msg.ID] = msg
	}

	return msgMap, nil
}
