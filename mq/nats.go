package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bjang03/gmq/utils"
	"log"
	"strings"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/nats-io/nats.go"
)

type NatsPubMessage struct {
	types.PubMessage
	Durable bool // 是否持久化
}

type NatsPubDelayMessage struct {
	types.PubDelayMessage
	Durable bool // 是否持久化
}

type NatsSubMessage struct {
	types.SubMessage
	Durable    bool // 是否持久化
	IsDelayMsg bool // 是延迟消息
}

// NatsConn NATS消息队列实现
type NatsConn struct {
	conn *nats.Conn // NATS 连接对象
	js   nats.JetStreamContext
}

// NatsConfig NATS 配置项
type natsConfig struct {
	Addr     string
	Port     string
	Username string
	Password string
}

// GmqPing 检测NATS连接状态
func (c *NatsConn) GmqPing(_ context.Context) bool {
	if c.conn == nil || c.js == nil {
		return false
	}
	return c.conn != nil && c.conn.IsConnected()
}

func (c *NatsConn) GmqGetConn(_ context.Context) any {
	m := map[string]any{
		"conn": c.conn,
		"js":   c.js,
	}
	return m
}

// GmqConnect 连接NATS服务器
func (c *NatsConn) GmqConnect(_ context.Context, cfg map[string]any) (err error) {
	config := new(natsConfig)
	err = utils.MapToStruct(config, cfg)
	if err != nil {
		return err
	}
	if config.Addr == "" {
		return fmt.Errorf("nats config addr is empty")
	}
	if config.Port == "" {
		return fmt.Errorf("nats config port is empty")
	}
	// 设置连接选项
	opts := []nats.Option{
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[NATS] Connection disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ConnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection established to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] Connection closed")
		}),
	}
	if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	}
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%s", config.Addr, config.Port), opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	js, err := conn.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		conn.Close()
		return fmt.Errorf("NATS JetStream connect failed: %w", err)
	}
	c.conn = conn
	c.js = js
	return
}

// GmqClose 关闭NATS连接
func (c *NatsConn) GmqClose(_ context.Context) error {
	if c.conn == nil {
		return nil
	}
	c.conn.Close()
	return nil
}

// GmqPublish 发布消息
func (c *NatsConn) GmqPublish(ctx context.Context, msg types.Publish) (err error) {
	cfg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}
	return c.createPublish(ctx, cfg.Topic, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *NatsConn) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) (err error) {
	cfg, ok := msg.(*NatsPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubDelayMessage")
	}
	return c.createPublish(ctx, cfg.Topic, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// Publish 发布消息
func (c *NatsConn) createPublish(ctx context.Context, topic string, durable bool, delayTime int, data any) (err error) {
	// 创建 Stream
	if _, _, err := c.createStream(ctx, topic, durable, delayTime > 0); err != nil {
		return err
	}
	// 构建消息
	m := nats.NewMsg(topic)
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}
	m.Data = payload
	// 延迟消息
	if delayTime > 0 {
		// 使用 @at 指定具体延迟时间，而不是 @every 重复执行
		futureTime := time.Now().Add(time.Duration(delayTime) * time.Second).Format(time.RFC3339Nano)
		m.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", futureTime))
		m.Subject = topic + ".schedule"
		m.Header.Set("Nats-Schedule-Target", topic)
	}
	// 发布消息
	if _, err = c.js.PublishMsg(m, []nats.PubOpt{nats.Context(ctx)}...); err != nil {
		return fmt.Errorf("NATS Failed to publish message: %w", err)
	}
	return
}

// GmqSubscribe 订阅NATS消息
func (c *NatsConn) GmqSubscribe(ctx context.Context, msg types.Subscribe) (err error) {
	cfg, ok := msg.GetSubMsg().(*NatsSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}
	// 创建 Stream
	streamName, _, err := c.createStream(ctx, cfg.Topic, cfg.Durable, cfg.IsDelayMsg)
	if err != nil {
		return err
	}
	//构建 Durable Consumer 配置
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
	// 创建 Durable Consumer
	if _, err = c.js.AddConsumer(streamName, consumerConfig, []nats.JSOpt{nats.Context(ctx)}...); err != nil {
		// 如果 Consumer 已存在，忽略错误
		if !strings.Contains(err.Error(), "consumer name already in use") {
			return fmt.Errorf("NATS Failed to add Consumer: %w", err)
		}
	}
	// 配置订阅选项 - 绑定到已创建的 Durable Consumer
	subOpts := []nats.SubOpt{
		nats.Context(ctx),
		nats.Bind(streamName, cfg.ConsumerName),
		nats.ManualAck(), // 手动确认模式
	}
	// 使用 Subscribe 创建推送订阅
	sub, err := c.js.Subscribe(cfg.Topic, func(natsMsg *nats.Msg) {
		if err = msg.GetAckHandleFunc()(ctx, &types.AckMessage{
			MessageData:     natsMsg.Data,
			AckRequiredAttr: natsMsg,
		}); err != nil {
			log.Printf("⚠️ Message processing failed: %v", err)
		}
	}, subOpts...)
	if err != nil {
		return fmt.Errorf("NATS Failed to subscribe: %w", err)
	}
	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()
	return
}

func (c *NatsConn) createStream(_ context.Context, topic string, durable, isDelayMsg bool) (string, nats.StorageType, error) {
	// 构建流名称和存储类型
	// 使用主题名称作为唯一标识，避免冲突
	// 将主题名称中的特殊字符替换为下划线
	safeTopicName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, topic)

	var streamName string
	var storage nats.StorageType

	// 根据 durable 和 isDelayMsg 确定存储类型
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

	// 构建流配置
	// 如果是延迟消息，需要包含两个 subjects:
	// 1. subject.schedule - 用于发送调度消息
	// 2. subject - 用于实际投递目标
	subjects := []string{topic}
	if isDelayMsg {
		subjects = []string{topic, topic + ".schedule"}
	}
	jsConfig := &streamConfig{
		Name:              streamName,
		Subjects:          subjects,
		AllowMsgSchedules: isDelayMsg, // 延迟消息核心开关
		Storage:           storage,
		Discard:           nats.DiscardOld,    // 达到上限删除旧消息
		MaxMsgs:           100000,             // 最多保留10万条消息
		MaxAge:            7 * 24 * time.Hour, // 消息保留7天
		Retention:         nats.InterestPolicy,
		MaxConsumers:      -1,
	}
	// 创建流
	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		return "", 0, fmt.Errorf("NATS Failed to create Stream: %w", err)
	}
	return streamName, storage, nil
}

// GmqAck 确认消息
func (c *NatsConn) GmqAck(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
	if !ok {
		return fmt.Errorf("invalid message type, expected *nats.Msg")
	}
	return msgCfg.Ack()
}

// GmqNak 否定确认消息，消息会重新投递（直到达到 MaxDeliver 限制）
func (c *NatsConn) GmqNak(_ context.Context, msg *types.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
	if !ok {
		return fmt.Errorf("invalid message type, expected *nats.Msg")
	}
	return msgCfg.Nak()
}

// streamConfig 流配置（精简版，仅包含实际使用的字段）
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

const (
	jSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"
	jSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s"
)

// 检查 API 响应中的错误
var resp struct {
	Error *struct {
		Code        int    `json:"code"`
		ErrCode     int    `json:"err_code"`
		Description string `json:"description"`
	} `json:"error,omitempty"`
}

// jsStreamRequest 发送 Stream API 请求（创建或更新）
func jsStreamRequest(nc *nats.Conn, apiTemplate string, cfg *streamConfig) error {
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

// jsStreamCreate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamCreate(nc *nats.Conn, cfg *streamConfig) (err error) {
	if err = jsStreamRequest(nc, jSApiStreamCreateT, cfg); err != nil {
		if strings.Contains(err.Error(), "10058") {
			// Stream 已存在，尝试更新
			return jsStreamUpdate(nc, cfg)
		} else if strings.Contains(err.Error(), "subjects overlap") {
			// Subjects 冲突，说明有另一个 Stream 已使用相同的 subjects
			return fmt.Errorf("subjects overlap with an existing stream, different durable/delay config for same queue")
		}
	}
	return err
}

// jsStreamUpdate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamUpdate(nc *nats.Conn, cfg *streamConfig) error {
	return jsStreamRequest(nc, jSApiStreamUpdateT, cfg)
}
