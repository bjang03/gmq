package mq

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/utils"
	"github.com/nats-io/nats.go"
)

type NatsPubMessage struct {
	core.PubMessage
	Durable bool // 是否持久化
}

type NatsPubDelayMessage struct {
	core.PubMessage
	Durable      bool // 是否持久化
	DelaySeconds int  // 延迟时间(秒)
}

type NatsSubMessage struct {
	core.SubMessage[any]
	Durable    bool // 是否持久化
	IsDelayMsg bool // 是延迟消息
}

func (n NatsPubMessage) GetGmqPublishMsgType() {
	//TODO implement me
	panic("implement me")
}

func (n NatsPubDelayMessage) GetGmqPublishDelayMsgType() {
	//TODO implement me
	panic("implement me")
}

// NatsConn NATS消息队列实现
type NatsConn struct {
	Url            string     // NATS连接地址
	Timeout        int        // 连接超时(秒)
	ReconnectWait  int        // 重连等待(秒)
	MaxReconnects  int        // 最大重连次数(-1为无限)
	MessageTimeout int        // 消息处理超时(秒)
	conn           *nats.Conn // NATS 连接对象
	js             nats.JetStreamContext
}

// GmqPing 检测NATS连接状态
func (c *NatsConn) GmqPing(_ context.Context) bool {
	return c.conn != nil && c.conn.IsConnected()
}

// GmqConnect 连接NATS服务器
func (c *NatsConn) GmqConnect(_ context.Context) error {
	// 设置连接选项
	opts := []nats.Option{
		nats.Timeout(time.Duration(c.Timeout) * time.Second),
		nats.ReconnectWait(time.Duration(c.ReconnectWait) * time.Second),
		nats.MaxReconnects(c.MaxReconnects),
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

	conn, err := nats.Connect(c.Url, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 创建 JetStream（可选）
	newJS, err := conn.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		conn.Close()
		return fmt.Errorf("NATS JetStream connect failed: %w", err)
	}
	c.conn = conn
	c.js = newJS
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
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}
	if cfg.QueueName == "" {
		return fmt.Errorf("must provide queue name")
	}
	if utils.IsEmpty(cfg.Data) {
		return fmt.Errorf("must provide data")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *NatsConn) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*NatsPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsPubDelayMessage")
	}
	if cfg.QueueName == "" {
		return fmt.Errorf("must provide queue name")
	}
	if utils.IsEmpty(cfg.Data) {
		return fmt.Errorf("must provide data")
	}
	if cfg.DelaySeconds <= 0 {
		return fmt.Errorf("must provide delay seconds")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// getStreamNameAndStorage 获取流名称和存储类型
func getStreamNameAndStorage(isDelayMsg, durable bool) (string, nats.StorageType) {
	if isDelayMsg {
		if durable {
			return "delay_msg_file", nats.FileStorage
		}
		return "delay_msg_memory", nats.MemoryStorage
	}
	if durable {
		return "ordinary_msg_file", nats.FileStorage
	}
	return "ordinary_msg_memory", nats.MemoryStorage
}

// checkInitialized 检查NATS连接和JetStream是否已初始化
func (c *NatsConn) checkInitialized() error {
	if c.conn == nil {
		return fmt.Errorf("NATS connection is not initialized")
	}
	if c.js == nil {
		return fmt.Errorf("NATS JetStream is not initialized")
	}
	return nil
}

// Publish 发布消息
func (c *NatsConn) createPublish(ctx context.Context, queueName string, durable bool, delayTime int, data any) error {
	delayMsg := delayTime > 0

	// 构建流名称和存储类型
	streamName, storage := getStreamNameAndStorage(delayMsg, durable)

	// 构建流配置
	// 如果是延迟消息，需要包含两个 subjects:
	// 1. subject.schedule - 用于发送调度消息
	// 2. subject - 用于实际投递目标
	subjects := []string{queueName}
	if delayMsg {
		subjects = []string{queueName, queueName + ".schedule"}
	}
	jsConfig := &StreamConfig{
		Name:              streamName,
		Subjects:          subjects,
		AllowMsgSchedules: delayMsg, // 延迟消息核心开关
		Storage:           storage,
		Discard:           nats.DiscardNew, // 达到上限删除旧消息
	}

	// 检查初始化状态
	if err := c.checkInitialized(); err != nil {
		return err
	}

	if err := jsStreamCreate(c.conn, jsConfig); err != nil {
		return fmt.Errorf("NATS Failed to create Stream: %w", err)
	}

	// 构建消息
	m := nats.NewMsg(queueName)
	// 序列化数据
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	m.Data = payload // 所有消息都需要设置数据

	// 延迟消息
	if delayMsg {
		// 使用 @at 指定具体延迟时间，而不是 @every 重复执行
		futureTime := time.Now().Add(time.Duration(delayTime) * time.Second).Format(time.RFC3339Nano)
		m.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", futureTime))
		m.Subject = queueName + ".schedule"
		m.Header.Set("Nats-Schedule-Target", queueName)
	}

	// 发布消息到 JetStream
	pubOpts := []nats.PubOpt{
		nats.Context(ctx),
	}
	ack, err := c.js.PublishMsg(m, pubOpts...)
	if err != nil {
		return fmt.Errorf("NATS Failed to publish message: %w", err)
	}
	log.Println(fmt.Sprintf("NATS [%s] message success publish: Stream=%v, StreamSeq=%d", c.Url, ack.Stream, ack.Sequence))
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *NatsConn) GmqSubscribe(ctx context.Context, msg any) (err error) {
	cfg, ok := msg.(*NatsSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}
	if cfg.QueueName == "" {
		return fmt.Errorf("must provide queue name")
	}
	if cfg.ConsumerName == "" {
		return fmt.Errorf("must provide consumer name")
	}
	if cfg.FetchCount <= 0 {
		return fmt.Errorf("must provide fetch count")
	}
	if cfg.HandleFunc == nil {
		return fmt.Errorf("must provide handle func")
	}

	// 获取 JetStream 上下文
	if err := c.checkInitialized(); err != nil {
		return err
	}

	// 创建推送订阅的回调函数
	msgHandler := func(natsMsg *nats.Msg) {
		var data map[string]any
		if err := json.Unmarshal(natsMsg.Data, &data); err != nil {
			log.Printf("⚠️  消息反序列化失败: %v, Subject=%s", err, natsMsg.Subject)
			if !cfg.AutoAck {
				_ = natsMsg.Nak()
			}
			return
		}

		// 调用用户提供的处理函数处理业务逻辑
		handleErr := cfg.HandleFunc(ctx, data)

		// 只有在手动确认模式下才需要手动 Ack/Nak
		// 自动确认模式下，NATS 客户端会自动确认消息
		if !cfg.AutoAck {
			// 手动确认模式: 处理成功则 Ack，处理失败则 Nak
			if handleErr == nil {
				if err := natsMsg.Ack(); err != nil {
					log.Printf("⚠️  Ack 失败: %v, Subject=%s", err, natsMsg.Subject)
				}
			} else {
				if err := natsMsg.Nak(); err != nil {
					log.Printf("⚠️  Nak 失败: %v, Subject=%s", err, natsMsg.Subject)
				}
				log.Printf("⚠️  消息处理失败 (Nak，将重试): %v, Subject=%s", handleErr, natsMsg.Subject)
			}
		}
	}

	// 构建流名称和存储类型
	streamName, _ := getStreamNameAndStorage(cfg.IsDelayMsg, cfg.Durable)

	// 配置死信队列
	deadLetterQueue := cfg.QueueName + "_DLQ" // 默认死信队列名称
	dlqStreamName := streamName + "_DLQ"
	dlqSubjects := []string{deadLetterQueue}
	storage := nats.MemoryStorage
	if cfg.Durable {
		storage = nats.FileStorage
	}
	dlqConfig := &StreamConfig{
		Name:         dlqStreamName,
		Subjects:     dlqSubjects,
		Storage:      storage,
		Discard:      nats.DiscardNew,
		MaxConsumers: -1,
	}
	if err = jsStreamCreate(c.conn, dlqConfig); err != nil {
		log.Printf("⚠️  死信队列创建失败: %v, Stream=%s", err, dlqStreamName)
	} else {
		log.Printf("✅ 死信队列创建成功: Stream=%s", dlqStreamName)
	}

	//构建 Durable Consumer 配置
	maxDeliver := core.MsgRetryDeliver
	retryDelay := core.MsgRetryDelay
	consumerConfig := &nats.ConsumerConfig{
		Durable:        cfg.ConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxAckPending:  cfg.FetchCount,
		FilterSubject:  cfg.QueueName,
		DeliverSubject: fmt.Sprintf("DELIVER.%s.%s", streamName, cfg.ConsumerName),
		MaxDeliver:     maxDeliver,
		BackOff:        []time.Duration{retryDelay},
	}
	consumerOpts := []nats.JSOpt{
		nats.Context(ctx),
	}
	_, err = c.js.AddConsumer(streamName, consumerConfig, consumerOpts...)
	if err != nil {
		// 如果 Consumer 已存在，忽略错误
		if !strings.Contains(err.Error(), "consumer name already in use") {
			return fmt.Errorf("NATS Failed to add Consumer: %w", err)
		}
	}

	// 配置订阅选项 - 绑定到已创建的 Durable Consumer
	subOpts := []nats.SubOpt{
		nats.Context(ctx),
		nats.Durable(cfg.ConsumerName),     // 绑定到 Durable Consumer
		nats.MaxAckPending(cfg.FetchCount), // 最大待确认消息数
		nats.BindStream(streamName),        // 绑定到指定 Stream
		nats.DeliverAll(),                  // 从第一条消息开始投递
		nats.Bind(streamName, cfg.ConsumerName),
	}

	// 根据 AutoAck 配置决定是否使用手动确认模式
	if !cfg.AutoAck {
		subOpts = append(subOpts, nats.ManualAck()) // 手动确认模式
	}

	// 使用 Subscribe 创建推送订阅，绑定到已存在的 Consumer
	sub, err := c.js.Subscribe(cfg.QueueName, msgHandler, subOpts...)
	if err != nil {
		log.Printf("⚠️  NATS 订阅失败: %v, Queue=%s, Consumer=%s, Stream=%s", err, cfg.QueueName, cfg.ConsumerName, streamName)
		return fmt.Errorf("NATS Failed to subscribe: %w", err)
	}

	log.Printf("✅ NATS 订阅成功: Queue=%s, Consumer=%s, Stream=%s", cfg.QueueName, cfg.ConsumerName, streamName)

	// ✅ 新增: 启动 DLQ 监听器，处理超过最大投递次数的消息
	go c.listenForDeliveryExceeded(ctx, streamName, cfg.ConsumerName, deadLetterQueue, dlqStreamName)

	// 启动后台 goroutine 监听上下文取消，用于清理订阅
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		log.Printf("🛑 NATS 订阅已取消: Queue=%s, Consumer=%s", cfg.QueueName, cfg.ConsumerName)
	}()

	return nil
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

// listenForDeliveryExceeded 监听消息投递超过最大次数的通知，将消息转移到死信队列
// 参考: nats-server-main/server/jetstream_events.go JSConsumerDeliveryExceededAdvisory
func (c *NatsConn) listenForDeliveryExceeded(ctx context.Context, streamName, consumerName, dlqSubject, dlqStreamName string) {
	// 订阅超过最大投递次数的通知
	// 主题格式: $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>
	advisorySubject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", streamName, consumerName)

	sub, err := c.conn.Subscribe(advisorySubject, func(msg *nats.Msg) {
		var advisory JSConsumerDeliveryExceededAdvisory
		if err := json.Unmarshal(msg.Data, &advisory); err != nil {
			log.Printf("⚠️  解析 DLQ advisory 失败: %v, Subject=%s", err, msg.Subject)
			return
		}

		log.Printf("📥 收到 MaxDeliver exceeded 通知: Stream=%s, Consumer=%s, StreamSeq=%d, Deliveries=%d",
			advisory.Stream, advisory.Consumer, advisory.StreamSeq, advisory.Deliveries)

		// 获取原始消息并转移到 DLQ
		if err := c.moveToDLQ(ctx, streamName, advisory.StreamSeq, dlqSubject, dlqStreamName, advisory.Deliveries); err != nil {
			log.Printf("⚠️  消息转移到 DLQ 失败: %v, Stream=%s, Seq=%d", err, streamName, advisory.StreamSeq)
		}
	})
	if err != nil {
		log.Printf("⚠️  订阅 DLQ advisory 失败: %v, Subject=%s", err, advisorySubject)
		return
	}
	defer sub.Unsubscribe()

	log.Printf("✅ DLQ 监听器启动成功: Stream=%s, Consumer=%s, AdvisorySubject=%s", streamName, consumerName, advisorySubject)

	// 等待上下文取消
	<-ctx.Done()
	log.Printf("🛑 DLQ 监听器已停止: Stream=%s, Consumer=%s", streamName, consumerName)
}

// moveToDLQ 将指定消息转移到死信队列
func (c *NatsConn) moveToDLQ(ctx context.Context, streamName string, streamSeq uint64, dlqSubject, dlqStreamName string, deliveries uint64) error {
	// 使用 JS API 获取原始消息
	// 参考: nats-server-main/server/jetstream_api.go JSApiMsgGetT
	getMsgSubject := fmt.Sprintf("$JS.API.STREAM.MSG.GET.%s", streamName)
	req := map[string]interface{}{
		"seq": streamSeq,
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal get msg request failed: %w", err)
	}

	resp, err := c.conn.Request(getMsgSubject, reqData, 5*time.Second)
	if err != nil {
		return fmt.Errorf("get msg request failed: %w", err)
	}

	// 解析响应 - 参考 nats-server-main/server/stream.go StoredMsg
	// hdrs 是 base64 编码的字节数组，不是 map
	var msgResp struct {
		Type  string `json:"type"`
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error,omitempty"`
		Message *struct {
			Subject string    `json:"subject"`
			Seq     uint64    `json:"seq"`
			Data    string    `json:"data"`           // base64 encoded
			Hdrs    string    `json:"hdrs,omitempty"` // base64 encoded headers
			Time    time.Time `json:"time"`
		} `json:"message,omitempty"`
	}
	if err := json.Unmarshal(resp.Data, &msgResp); err != nil {
		return fmt.Errorf("unmarshal get msg response failed: %w", err)
	}
	if msgResp.Error != nil {
		return fmt.Errorf("get msg API error: %s", msgResp.Error.Description)
	}
	if msgResp.Message == nil {
		return fmt.Errorf("message not found in stream")
	}

	// 解码 base64 数据
	msgData, err := base64.StdEncoding.DecodeString(msgResp.Message.Data)
	if err != nil {
		return fmt.Errorf("decode message data failed: %w", err)
	}

	// 构建 DLQ 消息
	dlqMsg := &nats.Msg{
		Subject: dlqSubject,
		Header:  make(nats.Header),
		Data:    msgData,
	}

	// 添加原始消息信息到 header
	dlqMsg.Header.Set("Nats-DLQ-Original-Stream", streamName)
	dlqMsg.Header.Set("Nats-DLQ-Original-Subject", msgResp.Message.Subject)
	dlqMsg.Header.Set("Nats-DLQ-Original-Seq", fmt.Sprintf("%d", streamSeq))
	dlqMsg.Header.Set("Nats-DLQ-Deliveries", fmt.Sprintf("%d", deliveries))
	dlqMsg.Header.Set("Nats-DLQ-Dead-Time", time.Now().Format(time.RFC3339))
	dlqMsg.Header.Set("Nats-DLQ-Reason", "Maximum delivery count exceeded")

	// 解码并复制原始消息的 headers
	if msgResp.Message.Hdrs != "" {
		hdrsData, err := base64.StdEncoding.DecodeString(msgResp.Message.Hdrs)
		if err != nil {
			log.Printf("⚠️  解码 headers 失败: %v", err)
		} else {
			// 解析 NATS headers 格式 (类似 HTTP headers)
			httpHeader, err := parseNatsHeaders(hdrsData)
			if err != nil {
				log.Printf("⚠️  解析 headers 失败: %v", err)
			} else {
				// 复制原始 headers
				for k, v := range httpHeader {
					for _, val := range v {
						dlqMsg.Header.Add(k, val)
					}
				}
			}
		}
	}

	// 发布到 DLQ Stream
	pubOpts := []nats.PubOpt{
		nats.Context(ctx),
	}
	ack, err := c.js.PublishMsg(dlqMsg, pubOpts...)
	if err != nil {
		return fmt.Errorf("publish to DLQ failed: %w", err)
	}

	log.Printf("✅ 消息已转移到 DLQ: Stream=%s, Seq=%d -> DLQStream=%s, DLQSeq=%d",
		streamName, streamSeq, dlqStreamName, ack.Sequence)

	return nil
}

// parseNatsHeaders 解析 NATS 消息头格式
// NATS headers 格式: "NATS/1.0\r\nkey1: value1\r\nkey2: value2\r\n\r\n"
func parseNatsHeaders(data []byte) (map[string][]string, error) {
	// 查找头部结束位置 (\r\n\r\n)
	idx := bytes.Index(data, []byte("\r\n\r\n"))
	if idx == -1 {
		// 尝试只查找 \r\n
		idx = bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			return nil, fmt.Errorf("invalid header format")
		}
		// 只有一行，可能是版本行
		headers := make(map[string][]string)
		return headers, nil
	}

	headers := make(map[string][]string)
	// 解析头部行
	headerLines := bytes.Split(data[:idx], []byte("\r\n"))
	for i, line := range headerLines {
		// 跳过第一行 (NATS/1.0)
		if i == 0 {
			continue
		}
		// 解析 key: value
		colonIdx := bytes.Index(line, []byte(":"))
		if colonIdx == -1 {
			continue
		}
		key := string(bytes.TrimSpace(line[:colonIdx]))
		value := string(bytes.TrimSpace(line[colonIdx+1:]))
		if key != "" {
			headers[key] = append(headers[key], value)
		}
	}
	return headers, nil
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

func (c *NatsConn) GmqGetDeadLetter(queueName string, limit int) (msgs []core.DeadLetterMsgDTO, err error) {
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

// GetMetrics 获取基础监控指标
func (c *NatsConn) GetMetrics(_ context.Context) *core.Metrics {
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

// StreamConfig 流配置（精简版，仅包含实际使用的字段）
type StreamConfig struct {
	Name              string             `json:"name"`
	Subjects          []string           `json:"subjects,omitempty"`
	Storage           nats.StorageType   `json:"storage"`
	Discard           nats.DiscardPolicy `json:"discard"`
	AllowMsgSchedules bool               `json:"allow_msg_schedules"`
	MaxConsumers      int                `json:"max_consumers"`
}

const (
	// JSApiStreamCreateT is the endpoint to create new streams.
	// Will return JSON response.
	JSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"

	// JSApiStreamUpdateT is the endpoint to update existing streams.
	// Will return JSON response.
	JSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s"
)

// jsStreamCreate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamCreate(nc *nats.Conn, cfg *StreamConfig) error {
	j, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), j, time.Second*3)
	if err != nil {
		return err
	}

	// 检查 API 响应中的错误
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		// 如果 Stream 已存在，尝试更新
		if resp.Error.ErrCode == 10058 { // JSStreamNameExistErr
			return jsStreamUpdate(nc, cfg)
		}
		return fmt.Errorf("JS API error: %s", resp.Error.Description)
	}

	return nil
}

// jsStreamUpdate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamUpdate(nc *nats.Conn, cfg *StreamConfig) error {
	j, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), j, time.Second*3)
	if err != nil {
		return err
	}

	// 检查 API 响应中的错误
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("JS API error: %s", resp.Error.Description)
	}

	return nil
}
