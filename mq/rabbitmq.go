package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bjang03/gmq/core"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPubMessage struct {
	core.PubMessage
	Durable bool // 是否持久化
}

type RabbitMQPubDelayMessage struct {
	core.PubDelayMessage
	Durable bool // 是否持久化
}

// RabbitMQSubMessage RabbitMQ订阅消息结构，支持持久化订阅和延迟消费
type RabbitMQSubMessage struct {
	core.SubMessage[any]
}

// RabbitMQConn RabbitMQ消息队列实现
type RabbitMQConn struct {
	Url      string
	Port     string
	Username string
	Password string
	VHost    string
	conn     *amqp.Connection
	channel  *amqp.Channel
}

// GmqPing 检测RabbitMQ连接状态
func (c *RabbitMQConn) GmqPing(_ context.Context) bool {
	if c.conn == nil || c.channel == nil {
		return false
	}

	if c.conn.IsClosed() || c.channel.IsClosed() {
		return false
	}

	return true
}

// GmqConnect 连接RabbitMQ服务器
func (c *RabbitMQConn) GmqConnect(ctx context.Context) (err error) {
	if c.Url == "" {
		return fmt.Errorf("RabbitMQ connect address is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("RabbitMQ connect port is empty")
	}
	if c.Username == "" {
		return fmt.Errorf("RabbitMQ connect username is empty")
	}
	if c.Password == "" {
		return fmt.Errorf("RabbitMQ connect password is empty")
	}
	// 安全地关闭旧连接（仅针对该数据源）
	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}
	if c.channel != nil && !c.channel.IsClosed() {
		c.channel.Close()
	}
	// 构建连接 URL
	url := "amqp://" + c.Username + ":" + c.Password + "@" + c.Url + ":" + c.Port + "/" + c.VHost
	// 创建连接
	newConn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("RabbitMQ connect failed: %w", err)
	}
	// 创建 Channel
	newChannel, err := newConn.Channel()
	if err != nil {
		newConn.Close()
		return fmt.Errorf("RabbitMQ JetStream connect failed: %w", err)
	}
	c.conn = newConn
	c.channel = newChannel
	return nil
}

// GmqClose 关闭RabbitMQ连接
func (c *RabbitMQConn) GmqClose(ctx context.Context) (err error) {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	if c.channel != nil {
		c.channel.Close()
		c.channel = nil
	}
	return nil
}

// GmqPublish 发布消息
func (c *RabbitMQConn) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RabbitMQPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *RabbitMQConn) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*RabbitMQPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubDelayMessage")
	}
	return c.createPublish(ctx, cfg.QueueName, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// createPublish 发布消息（增加死信队列功能）
// queueName: 业务队列名称
// durable: 是否持久化
// delayTime: 延迟时间（秒），0 表示不延迟
// data: 消息体
func (c *RabbitMQConn) createPublish(ctx context.Context, queueName string, durable bool, delayTime int, data any) error {
	delayMsg := delayTime > 0
	// 1. 基础配置
	exchangeType := "fanout"
	exchangeName := queueName
	routingKey := queueName
	args := amqp.Table{}
	if delayMsg {
		exchangeType = "x-delayed-message"
		exchangeName = queueName + ".delayed"
		args["x-delayed-type"] = "fanout"
	}

	// 2. 声明死信交换机和死信队列
	// 死信交换机名称
	deadLetterExchange := queueName + ".dlx"
	// 死信队列名称
	deadLetterQueue := queueName + ".dlq"
	// 死信路由键
	deadLetterRoutingKey := queueName + ".dlr"

	// 2.1 声明死信交换机（fanout 类型，保证消息广播到死信队列）
	if err := c.channel.ExchangeDeclare(
		deadLetterExchange, // 死信交换机名称
		"direct",           // 死信交换机类型
		durable,            // 是否持久化
		false,              // autoDelete
		false,              // internal
		false,              // noWait
		nil,                // args
	); err != nil {
		return fmt.Errorf("declare dead letter exchange failed: %w", err)
	}

	// 2.2 声明死信队列
	if _, err := c.channel.QueueDeclare(
		deadLetterQueue, // 死信队列名称
		durable,         // 是否持久化
		false,           // autoDelete
		false,           // exclusive
		false,           // noWait
		amqp.Table{},    // 死信队列参数
	); err != nil {
		return fmt.Errorf("declare dead letter queue failed: %w", err)
	}

	// 2.3 绑定死信队列到死信交换机
	if err := c.channel.QueueBind(
		deadLetterQueue,      // 死信队列名称
		deadLetterRoutingKey, // 死信路由键
		deadLetterExchange,   // 死信交换机名称
		false,                // noWait
		nil,                  // args
	); err != nil {
		return fmt.Errorf("bind dead letter queue failed: %w", err)
	}

	// 3. 声明业务 Exchange
	if err := c.channel.ExchangeDeclare(
		exchangeName, // 业务交换机名称
		exchangeType, // 交换机类型（普通/fanout 或 延迟/x-delayed-message）
		durable,      // 是否持久化
		false,        // autoDelete
		false,        // internal
		false,        // noWait
		args,         // 交换机参数（延迟交换机需要 x-delayed-type）
	); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	// 4. 声明业务队列（关联死信配置）
	queueArgs := amqp.Table{
		// 核心：指定当前队列的死信交换机
		"x-dead-letter-exchange": deadLetterExchange,
		// 核心：指定当前队列的死信路由键
		"x-dead-letter-routing-key": deadLetterRoutingKey,
	}
	if _, err := c.channel.QueueDeclare(
		queueName, // 业务队列名称
		durable,   // 是否持久化
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		queueArgs, // 队列参数（包含死信配置）
	); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}

	// 5. 绑定业务队列到业务交换机
	if err := c.channel.QueueBind(
		queueName,    // 业务队列名称
		routingKey,   // 路由键
		exchangeName, // 业务交换机名称
		false,        // noWait
		nil,          // args
	); err != nil {
		return fmt.Errorf("bind queue failed: %w", err)
	}

	// 6. 序列化消息数据
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data failed: %w", err)
	}

	// 7. 构建发布消息
	deliveryMode := amqp.Transient
	if durable {
		deliveryMode = amqp.Persistent
	}
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: deliveryMode,
		Timestamp:    time.Now(),
	}
	// 设置延迟消息头（如果需要延迟）
	if delayMsg {
		duration := delayTime * 1000 // 毫秒
		publishing.Headers = amqp.Table{
			"x-delay": duration,
		}
	}

	// 8. 发布消息
	err = c.channel.PublishWithContext(
		ctx,
		exchangeName, // 业务交换机名称
		routingKey,   // 路由键
		false,        // mandatory
		false,        // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("publish message failed: %w", err)
	}

	log.Printf("📨 publish message success: queueName=%s, deadLetterQueue=%s, data=%v", queueName, deadLetterQueue, data)
	return nil
}

// GmqSubscribe 订阅RabbitMQ消息
func (c *RabbitMQConn) GmqSubscribe(ctx context.Context, msg any) (err error) {
	cfg, ok := msg.(*RabbitMQSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQSubMessage")
	}

	if err = c.channel.Qos(cfg.FetchCount, 0, false); err != nil {
		return fmt.Errorf("set qos failed: %w", err)
	}
	msgs, err := c.channel.Consume(
		cfg.QueueName,    // queue
		cfg.ConsumerName, // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("consume failed: %w", err)
	}

	for msgv := range msgs {
		message := core.AckMessage{
			MessageData: msgv.Body,
			AckRequiredAttr: map[string]any{
				"MessageBody": msgv,
			},
		}
		cfg.HandleFunc(ctx, &message)
	}

	return
}

func (c *RabbitMQConn) Ack(msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgCfg, ok := attr["MessageBody"].(*amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid message type, expected *amqp.Delivery")
	}
	return msgCfg.Ack(false)
}

func (c *RabbitMQConn) Nak(msg *core.AckMessage) error {
	attr := msg.AckRequiredAttr
	msgCfg, ok := attr["MessageBody"].(*amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid message type, expected *amqp.Delivery")
	}
	// requeue=true: 消息重新入队，会被重新投递
	// requeue=false: 消息不重新入队，进入死信队列（如果配置了死信交换机）
	return msgCfg.Nack(false, false)
}

// GmqGetDeadLetter 从死信队列查询所有消息（不删除，仅读取）
// queueName: 队列名称
// limit: 限制查询数量（0表示查询所有）
// return: 结构化的死信消息列表 + 错误
func (c *RabbitMQConn) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) (msgs []core.DeadLetterMsgDTO, err error) {
	if c.channel == nil {
		return nil, fmt.Errorf("rabbitMQChannel is nil")
	}

	if limit <= 0 {
		limit = 10
	}

	// 1. 设置QoS，避免一次性拉取过多消息导致内存溢出
	if err := c.channel.Qos(100, 0, false); err != nil {
		return nil, fmt.Errorf("set qos failed: %w", err)
	}

	var fetchCount int
	// 死信队列名称规则：{queueName}.dlq
	deadLetterQueue := queueName + ".dlq"
	// 2. 循环拉取消息，直到队列为空或达到限制
	for {
		// 停止条件：达到数量限制
		if limit > 0 && fetchCount >= limit {
			break
		}

		// BasicGet 拉取单条消息（noAck=false：不自动确认）
		msg, ok, err := c.channel.Get(deadLetterQueue, false)
		if err != nil {
			log.Printf("get dead letter msg failed: %v", err)
			break
		}

		// 队列为空，退出循环
		if !ok {
			break
		}

		fetchCount++

		// 3. 解析死信消息（转为前端易读格式）
		dto := core.DeadLetterMsgDTO{
			MessageID:   msg.MessageId,
			Body:        string(msg.Body),
			Headers:     convertHeaders(msg.Headers), // 转换headers格式（处理AMQP特殊类型）
			Timestamp:   msg.Timestamp.Format("2006-01-02 15:04:05"),
			Exchange:    msg.Exchange,
			RoutingKey:  msg.RoutingKey,
			QueueName:   deadLetterQueue,
			DeliveryTag: msg.DeliveryTag,
		}

		// 解析死信原因（从headers中提取）
		dto.DeadReason = parseDeadLetterReason(msg.Headers)

		msgs = append(msgs, dto)

		// 4. 关键：Nack并重新入队（保证消息不被删除）
		// requeue=true：将消息重新放回队列
		if err := msg.Nack(false, true); err != nil {
			log.Printf("nack msg failed (deliveryTag=%d): %v", msg.DeliveryTag, err)
		}
	}

	log.Printf("✅ fetch dead letter msgs success: queue=%s, count=%d", deadLetterQueue, len(msgs))
	return msgs, nil
}

// convertHeaders 转换AMQP Headers格式（处理[]uint8等特殊类型，适配JSON序列化）
func convertHeaders(headers amqp.Table) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range headers {
		switch val := v.(type) {
		case []uint8:
			// 处理二进制数据转为字符串
			result[k] = string(val)
		case time.Time:
			// 时间类型转为字符串
			result[k] = val.Format("2006-01-02 15:04:05")
		default:
			result[k] = val
		}
	}
	return result
}

// parseDeadLetterReason 解析死信原因
func parseDeadLetterReason(headers amqp.Table) string {
	// 死信原因常见key（RabbitMQ自动添加）
	if reason, ok := headers["x-death"].([]interface{}); ok && len(reason) > 0 {
		if deathInfo, ok := reason[0].(amqp.Table); ok {
			if reasonVal, ok := deathInfo["reason"].(string); ok {
				switch reasonVal {
				case "expired":
					return "消息过期"
				case "rejected":
					return "消息被拒绝"
				case "maxlen":
					return "队列达到最大长度"
				case "deleted":
					return "队列被删除"
				default:
					return reasonVal
				}
			}
		}
	}
	return "未知原因"
}

// GetMetrics 获取基础监控指标
func (c *RabbitMQConn) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:       "rabbitmq",
		Type:       "rabbitmq",
		ServerAddr: c.Url,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	return m
}
