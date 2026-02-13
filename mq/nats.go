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
func (c *NatsConn) GmqConnect(_ context.Context) error {
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
	// 创建 JetStream（可选）
	js, err := conn.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		conn.Close()
		return fmt.Errorf("NATS JetStream connect failed: %w", err)
	}
	c.conn = conn
	c.js = js
	return nil
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
		log.Printf("⚠️  invalid message type, expected *NatsPubMessage")
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *NatsConn) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*NatsPubDelayMessage)
	if !ok {
		log.Printf("⚠️  invalid message type, expected *NatsPubDelayMessage")
		return fmt.Errorf("invalid message type, expected *NatsPubDelayMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// Publish 发布消息
func (c *NatsConn) createPublish(ctx context.Context, queueName string, durable bool, delayTime int, data any) error {
	// 创建 Stream
	if _, _, err := c.createStream(ctx, queueName, durable, delayTime > 0, false); err != nil {
		return err
	}
	// 构建消息
	m := nats.NewMsg(queueName)
	payload, err := json.Marshal(data)
	if err != nil {
		log.Printf("⚠️  json marshal failed: %v", err)
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
		log.Printf("⚠️  NATS Failed to publish message: %v", err)
		return fmt.Errorf("NATS Failed to publish message: %w", err)
	}
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *NatsConn) GmqSubscribe(ctx context.Context, msg core.Subscribe) (err error) {
	// 类型断言获取 NatsSubMessage 特定字段
	natsMsg, ok := msg.GetSubMsg().(*NatsSubMessage)
	if !ok {
		log.Printf("⚠️  invalid message type, expected *NatsSubMessage")
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}

	// 创建 Stream
	streamName, _, err := c.createStream(ctx, natsMsg.QueueName, natsMsg.Durable, natsMsg.IsDelayMsg, false)
	if err != nil {
		return err
	}

	consumerName := natsMsg.ConsumerName
	queueName := natsMsg.QueueName
	fetchCount := natsMsg.FetchCount

	//构建 Durable Consumer 配置
	consumerConfig := &nats.ConsumerConfig{
		Durable:        consumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxAckPending:  fetchCount,
		FilterSubject:  queueName,
		DeliverSubject: fmt.Sprintf("DELIVER.%s.%s", streamName, consumerName),
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
		nats.Bind(streamName, consumerName),
		nats.ManualAck(), // 手动确认模式
	}

	// 使用 Subscribe 创建推送订阅
	sub, err := c.js.Subscribe(queueName, func(natsMsg *nats.Msg) {
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
		log.Printf("⚠️  NATS 订阅失败: %v, Queue=%s, Consumer=%s, Stream=%s", err, queueName, consumerName, streamName)
		return fmt.Errorf("NATS Failed to subscribe: %w", err)
	}
	log.Printf("✅ NATS 订阅成功: Queue=%s, Consumer=%s, Stream=%s", queueName, consumerName, streamName)

	// ✅ 新增: 启动 DLQ 监听器，处理超过最大投递次数的消息
	go c.listenForDeliveryExceeded(ctx, streamName, consumerName)

	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		log.Printf("🛑 NATS 订阅已取消: Queue=%s, Consumer=%s", queueName, consumerName)
	}()

	return nil
}

func (c *NatsConn) createStream(_ context.Context, queueName string, durable, isDelayMsg, isDlq bool) (string, nats.StorageType, error) {
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
	if isDlq {
		streamName += "_DLQ"
		queueName += "_DLQ"
		isDelayMsg = false
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
		log.Printf("⚠️  NATS 流创建失败: %v, Stream=%s", err, streamName)
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

// findDLQStreamName 查找死信队列的流名称
func (c *NatsConn) findDLQStreamName() (string, error) {
	// 按优先级检查流是否存在
	streamNames := []string{
		"ordinary_msg_file_DLQ",
		"delay_msg_file_DLQ",
		"ordinary_msg_memory_DLQ",
		"delay_msg_memory_DLQ",
	}

	for _, name := range streamNames {
		if _, err := c.js.StreamInfo(name); err == nil {
			return name, nil
		}
	}

	return "", fmt.Errorf("no DLQ stream found")
}

func (c *NatsConn) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) (msgs []core.DeadLetterMsgDTO, err error) {
	// 死信队列名称
	deadLetterQueue := queueName + "_DLQ"

	// 获取对应的流信息
	streamName, err := c.findDLQStreamName()
	if err != nil {
		return nil, fmt.Errorf("failed to find DLQ stream: %w", err)
	}

	// 获取死信队列的消费者(用于读取消息)
	consumerName := "dlq_reader_" + queueName
	if _, err := c.js.ConsumerInfo(streamName, consumerName); err != nil {
		// 创建消费者
		if _, err := c.js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:   consumerName,
			AckPolicy: nats.AckExplicitPolicy,
		}); err != nil && !strings.Contains(err.Error(), "consumer name already in use") {
			return nil, fmt.Errorf("failed to create dlq consumer: %w", err)
		}
	}

	// 获取消息
	sub, err := c.js.PullSubscribe(deadLetterQueue, consumerName, nats.BindStream(streamName))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to dead letter queue: %w", err)
	}
	defer sub.Unsubscribe()

	// 获取指定数量的消息
	if limit <= 0 {
		return nil, nil
	}

	msgs = make([]core.DeadLetterMsgDTO, 0, limit)
	batch, err := sub.Fetch(limit, nats.MaxWait(5*time.Second))
	if err != nil && !strings.Contains(err.Error(), "timeout") {
		return nil, fmt.Errorf("failed to fetch messages: %w", err)
	}

	for _, msg := range batch {
		// 解析消息头
		headers := make(map[string]interface{})
		if msg.Header != nil {
			for k, v := range msg.Header {
				if len(v) > 0 {
					headers[k] = v[0]
				}
			}
		}

		// 获取元数据
		metadata, _ := msg.Metadata()

		// 构建死信消息DTO
		msgs = append(msgs, core.DeadLetterMsgDTO{
			MessageID:   fmt.Sprintf("%d", metadata.Sequence.Stream),
			Body:        string(msg.Data),
			Headers:     headers,
			Timestamp:   metadata.Timestamp.Format("2006-01-02 15:04:05"),
			Exchange:    streamName,
			RoutingKey:  msg.Subject,
			DeadReason:  "Maximum delivery count exceeded",
			QueueName:   deadLetterQueue,
			DeliveryTag: metadata.Sequence.Stream,
		})
		_ = msg.Ack()
	}

	return msgs, nil
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
	JSApiStreamCreateT                      = "$JS.API.STREAM.CREATE.%s"
	JSApiStreamUpdateT                      = "$JS.API.STREAM.UPDATE.%s"
	JSApiEventAdvisoryConsumerMaxDeliveries = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s"
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

// jSConsumerDelivery 消息投递超过最大次数的通知
// 参考: nats-server-main/server/jetstream_events.go
type jSConsumerDelivery struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	Time       time.Time `json:"time"`
	Stream     string    `json:"stream"`
	Consumer   string    `json:"consumer"`
	StreamSeq  uint64    `json:"stream_seq"`
	Deliveries uint64    `json:"deliveries"`
	Domain     string    `json:"domain,omitempty"`
}

// listenForDeliveryExceeded 监听消息投递超过最大次数的通知，将消息转移到死信队列
// 参考: nats-server-main/server/jetstream_events.go JSConsumerDeliveryExceededAdvisory
func (c *NatsConn) listenForDeliveryExceeded(ctx context.Context, streamName, consumerName string) {
	// 订阅超过最大投递次数的通知
	sub, err := c.conn.Subscribe(fmt.Sprintf(JSApiEventAdvisoryConsumerMaxDeliveries, streamName, consumerName), func(msg *nats.Msg) {
		var advisory jSConsumerDelivery
		if err := json.Unmarshal(msg.Data, &advisory); err != nil {
			log.Printf("⚠️  解析 DLQ advisory 失败: %v, Subject=%s", err, msg.Subject)
			return
		}

		log.Printf("📥 收到 MaxDeliver exceeded 通知: Stream=%s, Consumer=%s, StreamSeq=%d, Deliveries=%d",
			advisory.Stream, advisory.Consumer, advisory.StreamSeq, advisory.Deliveries)

		//TODO implement me
		// 发送到死信队列
	})
	if err != nil {
		log.Printf("⚠️  订阅 DLQ advisory 失败: %v", err)
		return
	}
	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		log.Printf("🛑 DLQ 监听器已停止: Stream=%s, Consumer=%s", streamName, consumerName)
	}()

	log.Printf("✅ DLQ 监听器启动成功: Stream=%s, Consumer=%s", streamName, consumerName)
}
