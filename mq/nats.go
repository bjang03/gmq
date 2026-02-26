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
	Url  string // NATS连接地址
	Port string
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
	if c.Url == "" {
		return fmt.Errorf("nats connect address is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("nats connect port is empty")
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
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%s", c.Url, c.Port), opts...)
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
			MessageData:     natsMsg.Data,
			AckRequiredAttr: natsMsg,
		}); err != nil {
			log.Printf("⚠️ Message processing failed: %v", err)
		}
	}, subOpts...)
	if err != nil {
		return fmt.Errorf("NATS Failed to subscribe: %w", err)
	}
	// 订阅死信队列
	go c.subscribeDeadLetter(ctx)
	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()
	return
}

func (c *NatsConn) createStream(_ context.Context, queueName string, durable, isDelayMsg bool) (string, nats.StorageType, error) {
	// 构建流名称和存储类型
	// 使用队列名称作为唯一标识，避免冲突
	// 将队列名称中的特殊字符替换为下划线
	safeQueueName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, queueName)

	var streamName string
	var storage nats.StorageType

	// 根据 durable 和 isDelayMsg 确定存储类型
	if isDelayMsg {
		if durable {
			streamName, storage = fmt.Sprintf("delay_file_%s", safeQueueName), nats.FileStorage
		} else {
			streamName, storage = fmt.Sprintf("delay_memory_%s", safeQueueName), nats.MemoryStorage
		}
	} else {
		if durable {
			streamName, storage = fmt.Sprintf("ordinary_file_%s", safeQueueName), nats.FileStorage
		} else {
			streamName, storage = fmt.Sprintf("ordinary_memory_%s", safeQueueName), nats.MemoryStorage
		}
	}

	// 构建流配置
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
		Discard:           nats.DiscardOld,    // 达到上限删除旧消息
		MaxMsgs:           100000,             // 最多保留10万条消息
		MaxAge:            7 * 24 * time.Hour, // 消息保留7天
		Retention:         nats.LimitsPolicy,
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
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
	if !ok {
		return fmt.Errorf("invalid message type, expected *nats.Msg")
	}
	return msgCfg.Ack()
}

// GmqNak 否定确认消息，消息会重新投递（直到达到 MaxDeliver 限制）
func (c *NatsConn) GmqNak(_ context.Context, msg *core.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(*nats.Msg)
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

	// 设置连接状态
	if c.conn.IsConnected() {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
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

// JSConsumerDeliveryExceededAdvisory 消息投递超过最大次数的通知
// 参考: nats-server-main/server/jetstream_events.go
type JSConsumerDeliveryExceededAdvisory struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	Time       time.Time `json:"time"`
	Stream     string    `json:"stream"`
	Consumer   string    `json:"consumer"`
	StreamSeq  uint64    `json:"stream_seq"`
	Deliveries uint64    `json:"deliveries"`
	Domain     string    `json:"domain,omitempty"`
}

// GmqGetDeadLetter 获取死信消息（NATS 暂不支持死信队列）
func (c *NatsConn) GmqGetDeadLetter(ctx context.Context) ([]core.DeadLetterMsgDTO, error) {
	// 1. 配置核心参数
	advisoryStreamName := "JS_ADVISORY_STREAM"
	advisorySubject := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
	advisoryConsumerName := "ADVISORY_PUSH_CONSUMER"
	// 2. 创建告警专用流（关键：补充 Retention 和 MaxAge，避免消息丢失）
	jsConfig := &streamConfig{
		Name:     advisoryStreamName,
		Subjects: []string{advisorySubject},
		Storage:  nats.FileStorage,
	}
	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		log.Printf("⚠️ 创建告警流失败: %v", err)
	}
	// 3. 创建持久化 Consumer（关键：补充 AckWait/MaxDeliver，绑定到告警流）
	consumerCfg := &nats.ConsumerConfig{
		Durable:       advisoryConsumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second, // 关键：设置未Ack重投等待时间
		MaxDeliver:    5,                // 告警消息最多重投5次
		MaxAckPending: 50,               // 限流：最大未确认消息数
	}
	// 关键：AddConsumer 第一个参数是告警流名称，绑定 Consumer 到该流
	if _, err := c.js.AddConsumer(advisoryStreamName, consumerCfg); err != nil {
		if !strings.Contains(err.Error(), "consumer name already in use") && !strings.Contains(err.Error(), "consumer already exists") {
			log.Printf("⚠️ 创建告警Consumer失败: %v", err)
		}
	}
	// 4. 创建 PullSubscribe（关键：使用 Bind 绑定到告警流+Consumer，避免创建临时流）
	subOpts := []nats.SubOpt{
		nats.Bind(advisoryStreamName, advisoryConsumerName), // 核心：绑定到告警流
		nats.ManualAck(), // 手动Ack
	}
	// 关键：PullSubscribe 第二个参数是 Durable 名称，第三个参数是订阅选项
	subscribe, err := c.js.PullSubscribe(
		advisorySubject,      // 订阅的主题
		advisoryConsumerName, // Durable 名称（和上面的 Consumer 一致）
		subOpts...,           // 绑定到告警流
	)
	if err != nil {
		log.Printf("⚠️ 订阅告警流失败: %v", err)
		return nil, err
	}
	defer subscribe.Unsubscribe()
	log.Printf("✅ 告警流 Pull 订阅成功: Stream=%s, Consumer=%s", advisoryStreamName, advisoryConsumerName)

	// 5. 持续拉取消息（修复循环逻辑，非致命错误不终止）
	for {
		// 检查上下文是否取消（优先退出）
		select {
		case <-ctx.Done():
			log.Printf("🛑 告警监听器收到取消信号，停止拉取")
			return nil, ctx.Err()
		default:
		}

		// 创建拉取上下文（每次拉取用独立超时，避免整体超时）
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
		// 关键：Fetch 配置 Expires（长轮询），MaxMessages=10，等待10秒
		msgs, err := subscribe.Fetch(10, nats.Context(fetchCtx))
		fetchCancel() // 释放上下文
		// 处理拉取错误（区分致命错误和非致命错误）
		if err != nil {
			if err == context.DeadlineExceeded || strings.Contains(err.Error(), "timeout") {
				// 超时：继续循环，等待下一次拉取
				continue
			} else if strings.Contains(err.Error(), "context canceled") {
				// 上下文取消：退出循环
				log.Printf("🛑 拉取上下文已取消，停止监听器")
				return nil, err
			} else {
				// 致命错误：打印日志，继续循环（而非 break）
				log.Printf("⚠️ 拉取告警消息失败（非致命）: %v", err)
				time.Sleep(1 * time.Second) // 退避1秒，避免高频报错
				continue
			}
		}

		// 处理拉取到的消息
		for _, msg := range msgs {
			var advisory JSConsumerDeliveryExceededAdvisory
			if err := json.Unmarshal(msg.Data, &advisory); err != nil {
				log.Printf("⚠️ 解析告警失败: %v, Subject=%s", err, msg.Subject)
				// 解析失败：Nack 让消息重投
				msg.Nak()
				continue
			}

			// 打印告警信息（业务逻辑可替换）
			log.Printf("📥 收到 MaxDeliver 告警: Stream=%s, Consumer=%s, MsgID=%s, Deliveries=%d/%d", advisory.Stream, advisory.Consumer, advisory.ID, advisory.Deliveries, advisory.Deliveries)

			// TODO: 需要补充写入数据库
			// 参考 RabbitMQ 实现，将死信消息转换为 DeadLetterMsgDTO 并存储到数据库
			// dto := core.DeadLetterMsgDTO{
			//     MessageID:   advisory.ID,
			//     Body:        fmt.Sprintf("Stream=%s, Consumer=%s, StreamSeq=%d", advisory.Stream, advisory.Consumer, advisory.StreamSeq),
			//     Timestamp:   advisory.Time.Format("2006-01-02 15:04:05"),
			//     QueueName:   advisory.Stream,
			//     DeadReason:  fmt.Sprintf("MaxDeliver exceeded: %d", advisory.Deliveries),
			// }
			// 死信队列中的消息保存成功后才能进行确认消息

			// 关键：消费成功后 Ack，避免消息重投
			if err := msg.Ack(); err != nil {
				log.Printf("⚠️ 确认告警消息失败: %v, MsgID=%s", err, advisory.ID)
			}
		}
	}

}

// subscribeDeadLetter 监听消息投递超过最大次数的通知，将消息转移到死信队列
// 参考: nats-server-main/server/jetstream_events.go JSConsumerDeliveryExceededAdvisory
func (c *NatsConn) subscribeDeadLetter(ctx context.Context) error {
	// 1. 配置核心参数
	advisoryStreamName := "JS_ADVISORY_STREAM"
	advisorySubject := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
	advisoryConsumerName := "ADVISORY_PUSH_CONSUMER"
	// 2. 创建告警专用流（关键：补充 Retention 和 MaxAge，避免消息丢失）
	jsConfig := &streamConfig{
		Name:     advisoryStreamName,
		Subjects: []string{advisorySubject},
		Storage:  nats.FileStorage,
	}
	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		log.Printf("⚠️ 创建告警流失败: %v", err)
	}
	// 3. 创建持久化 Consumer（关键：补充 AckWait/MaxDeliver，绑定到告警流）
	consumerCfg := &nats.ConsumerConfig{
		Durable:        advisoryConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubject:  advisorySubject,
		DeliverSubject: fmt.Sprintf("DELIVER.%s.%s", advisoryStreamName, advisoryConsumerName),
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckWait:        30 * time.Second, // 关键：设置未Ack重投等待时间
		MaxDeliver:     5,                // 告警消息最多重投5次
		MaxAckPending:  50,               // 限流：最大未确认消息数
	}
	// 关键：AddConsumer 第一个参数是告警流名称，绑定 Consumer 到该流
	if _, err := c.js.AddConsumer(advisoryStreamName, consumerCfg); err != nil {
		if !strings.Contains(err.Error(), "consumer name already in use") && !strings.Contains(err.Error(), "consumer already exists") {
			log.Printf("⚠️ 创建告警Consumer失败: %v", err)
		}
	}
	// 4. 创建 PullSubscribe（关键：使用 Bind 绑定到告警流+Consumer，避免创建临时流）
	// 订阅选项：绑定到已创建的流和Consumer，指定上下文
	subOpts := []nats.SubOpt{
		nats.Bind(advisoryStreamName, advisoryConsumerName), // 核心：绑定到告警流
		nats.ManualAck(), // 手动Ack
	}
	sub, err := c.js.Subscribe(advisorySubject, func(msg *nats.Msg) {
		var advisory JSConsumerDeliveryExceededAdvisory
		if err := json.Unmarshal(msg.Data, &advisory); err != nil {
			log.Printf("⚠️  解析 DLQ advisory 失败: %v, Subject=%s", err, msg.Subject)
			return
		}
		log.Printf("📥 收到 MaxDeliver exceeded 通知: Stream=%s, Consumer=%s, StreamSeq=%d, Deliveries=%d", advisory.Stream, advisory.Consumer, advisory.StreamSeq, advisory.Deliveries)

		// TODO: 需要补充写入数据库
		// 参考 RabbitMQ 实现，将死信消息转换为 DeadLetterMsgDTO 并存储到数据库
		// dto := core.DeadLetterMsgDTO{
		//     MessageID:   advisory.ID,
		//     Body:        fmt.Sprintf("Stream=%s, Consumer=%s, StreamSeq=%d", advisory.Stream, advisory.Consumer, advisory.StreamSeq),
		//     Timestamp:   advisory.Time.Format("2006-01-02 15:04:05"),
		//     QueueName:   advisory.Stream,
		//     DeadReason:  fmt.Sprintf("MaxDeliver exceeded: %d", advisory.Deliveries),
		// }
		// 死信队列中的消息保存成功后才能进行确认消息

		// 关键：消费成功后 Ack，避免消息重投
		if err := msg.Ack(); err != nil {
			log.Printf("⚠️ 确认告警消息失败: %v, MsgID=%s", err, advisory.ID)
		}
	}, subOpts...)
	if err != nil {
		log.Printf("⚠️  订阅 DLQ advisory 失败: %v, Subject=%s", err, advisorySubject)
		return err
	}
	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()
	log.Printf("✅ DLQ 监听器启动成功: AdvisorySubject=%s", advisorySubject)
	return nil
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
	return jsStreamRequest(nc, JSApiStreamUpdateT, cfg)
}
