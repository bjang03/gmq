package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/nats-io/nats.go"
)

type NatsPubMessage struct {
	core.PubMessage
	Durable bool // 是否持久化
}

type NatsPubDelayMessage struct {
	core.PubDelayMessage
	Durable bool // 是否持久化
}

type NatsSubMessage struct {
	core.SubMessage
	Durable    bool // 是否持久化
	IsDelayMsg bool // 是延迟消息
}

// NatsConn NATS消息队列实现
type NatsConn struct {
	Url  string     // NATS连接地址
	conn *nats.Conn // NATS 连接对象
	js   nats.JetStreamContext
}

// GmqPing 检测NATS连接状态
func (c *NatsConn) GmqPing(_ context.Context) bool {
	if c.conn == nil || c.js == nil {
		return false
	}
	return c.conn != nil && c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器
func (c *NatsConn) GmqConnect(_ context.Context) (err error) {
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
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", c.Url), opts...)
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
func (c *NatsConn) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*NatsPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *NatsConn) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*NatsPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubDelayMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// Publish 发布消息
func (c *NatsConn) createPublish(ctx context.Context, queueName string, durable bool, delayTime int, data any) (err error) {
	// 创建 Stream
	if _, _, err := c.createStream(ctx, queueName, durable, delayTime > 0); err != nil {
		return err
	}
	// 构建消息
	m := nats.NewMsg(queueName)
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
		m.Subject = queueName + ".schedule"
		m.Header.Set("Nats-Schedule-Target", queueName)
	}
	// 发布消息
	if _, err = c.js.PublishMsg(m, []nats.PubOpt{nats.Context(ctx)}...); err != nil {
		return fmt.Errorf("NATS Failed to publish message: %w", err)
	}
	return
}

// GmqSubscribe 订阅NATS消息
func (c *NatsConn) GmqSubscribe(ctx context.Context, msg core.Subscribe) (err error) {
	cfg, ok := msg.GetSubMsg().(*NatsSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}
	// 创建 Stream
	streamName, _, err := c.createStream(ctx, cfg.QueueName, cfg.Durable, cfg.IsDelayMsg)
	if err != nil {
		return err
	}
	//构建 Durable Consumer 配置
	consumerConfig := &nats.ConsumerConfig{
		Durable:        cfg.ConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxAckPending:  cfg.FetchCount,
		FilterSubject:  cfg.QueueName,
		DeliverSubject: fmt.Sprintf("DELIVER.%s.%s", streamName, cfg.ConsumerName),
		DeliverPolicy:  nats.DeliverAllPolicy,
		MaxDeliver:     1,
		BackOff:        []time.Duration{time.Second},
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
	sub, err := c.js.Subscribe(cfg.QueueName, func(natsMsg *nats.Msg) {
		if err = msg.GetAckHandleFunc()(ctx, &core.AckMessage{
			MessageData: natsMsg.Data,
			AckRequiredAttr: map[string]any{
				"MessageBody": natsMsg,
			},
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

func (c *NatsConn) createStream(_ context.Context, queueName string, durable, isDelayMsg bool) (string, nats.StorageType, error) {
	// 构建流名称和存储类型
	streamName, storage := "ordinary_msg_memory", nats.MemoryStorage
	if isDelayMsg {
		if durable {
			streamName, storage = "delay_msg_file", nats.FileStorage
		} else {
			streamName, storage = "delay_msg_memory", nats.MemoryStorage
		}
	} else {
		if durable {
			streamName, storage = "ordinary_msg_file", nats.FileStorage
		}
	}
	// 构建流配置
	// 如果是延迟消息，需要包含两个 subjects:
	// 1. subject.schedule - 用于发送调度消息
	// 2. subject - 用于实际投递目标
	subjects := []string{queueName}
	if isDelayMsg {
		subjects = []string{queueName, queueName + ".schedule"}
	}
	jsConfig := &streamConfig{
		Name:              streamName,
		Subjects:          subjects,
		AllowMsgSchedules: isDelayMsg, // 延迟消息核心开关
		Storage:           storage,
		Discard:           nats.DiscardNew, // 达到上限删除旧消息
		MaxConsumers:      -1,
	}
	// 创建流
	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		return "", 0, fmt.Errorf("NATS Failed to create Stream: %w", err)
	}
	return streamName, storage, nil
}

// GmqAck 确认消息
func (c *NatsConn) GmqAck(_ context.Context, msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgCfg, ok := attr["MessageBody"].(*nats.Msg)
	if !ok {
		return fmt.Errorf("invalid message type, expected *nats.Msg")
	}
	return msgCfg.Ack()
}

// GmqNak 否定确认消息，消息会重新投递（直到达到 MaxDeliver 限制）
func (c *NatsConn) GmqNak(_ context.Context, msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgCfg, ok := attr["MessageBody"].(*nats.Msg)
	if !ok {
		return fmt.Errorf("invalid message type, expected *nats.Msg")
	}
	return msgCfg.Nak()
}

// GmqGetMetrics 获取基础监控指标
func (c *NatsConn) GmqGetMetrics(_ context.Context) *core.Metrics {
	m := &core.Metrics{
		Type:       "nats",
		ServerAddr: c.Url,
	}

	// 检查连接是否为 nil
	if c.conn == nil {
		m.Status = "disconnected"
		return m
	}

	// 从 NATS 连接获取服务端统计信息
	stats := c.conn.Stats()
	// NATS 提供的统计信息
	m.MsgsIn = int64(stats.InMsgs)
	m.MsgsOut = int64(stats.OutMsgs)
	m.BytesIn = int64(stats.InBytes)
	m.BytesOut = int64(stats.OutBytes)
	m.ReconnectCount = int64(c.conn.Reconnects)

	// 只提供客户端可获取的真实指标，移除硬编码的虚假数据
	m.ServerMetrics = map[string]interface{}{
		"serverId":      c.conn.ConnectedServerId(),
		"serverVersion": c.conn.ConnectedServerVersion(),
	}

	return m
}

// GmqGetDeadLetter 获取死信消息（NATS 暂不支持死信队列）
func (c *NatsConn) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) ([]core.DeadLetterMsgDTO, error) {
	return nil, fmt.Errorf("nats does not support dead letter queue yet")
}

// streamConfig 流配置（精简版，仅包含实际使用的字段）
type streamConfig struct {
	Name              string             `json:"name"`
	Subjects          []string           `json:"subjects,omitempty"`
	Storage           nats.StorageType   `json:"storage"`
	Discard           nats.DiscardPolicy `json:"discard"`
	AllowMsgSchedules bool               `json:"allow_msg_schedules"`
	MaxConsumers      int                `json:"max_consumers"`
}

const (
	JSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"
	JSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s"
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
	if err = jsStreamRequest(nc, JSApiStreamCreateT, cfg); err != nil {
		if strings.Contains(err.Error(), "10058") {
			return jsStreamUpdate(nc, cfg)
		}
	}
	return err
}

// jsStreamUpdate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamUpdate(nc *nats.Conn, cfg *streamConfig) error {
	return jsStreamRequest(nc, JSApiStreamUpdateT, cfg)
}
