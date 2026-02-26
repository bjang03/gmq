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
	core.SubMessage
}

// RabbitMQConn RabbitMQ消息队列实现
type RabbitMQConn struct {
	Url               string
	Port              string
	Username          string
	Password          string
	VHost             string
	conn              *amqp.Connection
	channel           *amqp.Channel
	unifiedDLExchange string // 统一死信交换机名称
	unifiedDLQueue    string // 统一死信队列名称
	unifiedDLConsumer string // 统一死信消费者名称
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
func (c *RabbitMQConn) GmqConnect(_ context.Context) (err error) {
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
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", c.Username, c.Password, c.Url, c.Port, c.VHost)
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
	return
}

// GmqClose 关闭RabbitMQ连接
func (c *RabbitMQConn) GmqClose(_ context.Context) (err error) {
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

// setupUnifiedDeadLetter 创建或验证统一死信交换机和队列（幂等操作）
func (c *RabbitMQConn) setupUnifiedDeadLetter() error {
	// 设置统一死信交换机和队列名称
	c.unifiedDLExchange = "gmq.dead.letter.exchange"
	c.unifiedDLQueue = "gmq.dead.letter.queue"
	c.unifiedDLConsumer = "gmq.dead.letter.consumer"

	// 声明统一死信交换机（fanout 类型）
	// 如果交换机已存在且配置相同，RabbitMQ 会忽略该操作（幂等）
	if err := c.channel.ExchangeDeclare(
		c.unifiedDLExchange, // 交换机名称
		"fanout",            // fanout 类型，广播到所有绑定队列
		true,                // 持久化
		false,               // autoDelete
		false,               // internal
		false,               // noWait
		nil,                 // args
	); err != nil {
		return fmt.Errorf("declare unified dead letter exchange failed: %w", err)
	}

	// 声明统一死信队列
	// 如果队列已存在且配置相同，RabbitMQ 会忽略该操作（幂等）
	if _, err := c.channel.QueueDeclare(
		c.unifiedDLQueue, // 统一死信队列名称
		true,             // 持久化
		false,            // autoDelete
		false,            // exclusive
		false,            // noWait
		nil,              // args
	); err != nil {
		return fmt.Errorf("declare unified dead letter queue failed: %w", err)
	}

	// 绑定统一死信队列到统一死信交换机
	// 如果绑定关系已存在，RabbitMQ 会忽略该操作（幂等）
	if err := c.channel.QueueBind(
		c.unifiedDLQueue,    // 统一死信队列
		"",                  // fanout 类型不需要路由键
		c.unifiedDLExchange, // 统一死信交换机
		false,               // noWait
		nil,                 // args
	); err != nil {
		return fmt.Errorf("bind unified dead letter queue failed: %w", err)
	}

	return nil
}

// GmqPublish 发布消息
func (c *RabbitMQConn) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RabbitMQPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubMessage")
	}
	return c.createPublish(ctx, cfg.Topic, cfg.Durable, 0, cfg.Data)
}

// GmqPublishDelay 发布延迟消息
func (c *RabbitMQConn) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*RabbitMQPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubDelayMessage")
	}
	return c.createPublish(ctx, cfg.Topic, cfg.Durable, cfg.DelaySeconds, cfg.Data)
}

// createPublish 发布消息
// topic: 业务主题名称
// durable: 是否持久化
// delayTime: 延迟时间（秒），0 表示不延迟
// data: 消息体
func (c *RabbitMQConn) createPublish(ctx context.Context, topic string, durable bool, delayTime int, data any) error {
	// 0. 确保统一死信交换机和队列已创建（幂等操作）
	if err := c.setupUnifiedDeadLetter(); err != nil {
		return err
	}

	delayMsg := delayTime > 0
	// 1. 基础配置
	exchangeType := "fanout"
	exchangeName := topic
	routingKey := topic
	args := amqp.Table{}
	if delayMsg {
		exchangeType = "x-delayed-message"
		exchangeName = topic + ".delayed"
		args["x-delayed-type"] = "fanout"
	}
	// 2. 声明业务队列（关联死信配置）
	// 直接将死信指向统一死信交换机
	queueArgs := amqp.Table{
		// 核心：指定当前队列的死信交换机为统一死信交换机
		"x-dead-letter-exchange": c.unifiedDLExchange,
		// fanout 类型交换机不需要路由键
		"x-dead-letter-routing-key": "",
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

	// 4. 声明业务队列
	if _, err := c.channel.QueueDeclare(
		topic,     // 业务队列名称
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
		topic,        // 业务队列名称
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
	return err
}

// GmqSubscribe 订阅RabbitMQ消息
func (c *RabbitMQConn) GmqSubscribe(ctx context.Context, sub core.Subscribe) (err error) {
	cfg, ok := sub.GetSubMsg().(*RabbitMQSubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQSubMessage")
	}
	if err = c.channel.Qos(cfg.FetchCount, 0, false); err != nil {
		return fmt.Errorf("set qos failed: %w", err)
	}
	msgs, err := c.channel.Consume(
		cfg.Topic,        // queue
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
	go c.subscribeDeadLetter(ctx)
	for msgv := range msgs {
		if err = sub.GetAckHandleFunc()(ctx, &core.AckMessage{
			MessageData:     msgv.Body,
			AckRequiredAttr: msgv,
		}); err != nil {
			log.Printf("⚠️ Message processing failed: %v", err)
			continue
		}
	}
	return
}

func (c *RabbitMQConn) GmqAck(_ context.Context, msg *core.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid message type, expected *amqp.Delivery")
	}
	return msgCfg.Ack(false)
}

func (c *RabbitMQConn) GmqNak(_ context.Context, msg *core.AckMessage) error {
	msgCfg, ok := msg.AckRequiredAttr.(amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid message type, expected *amqp.Delivery")
	}
	// requeue=true: 消息重新入队，会被重新投递
	// requeue=false: 消息不重新入队，进入死信队列（如果配置了死信交换机）
	return msgCfg.Nack(false, false)
}

// GmqGetMetrics 获取基础监控指标
func (c *RabbitMQConn) GmqGetMetrics(ctx context.Context) *core.Metrics {
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

// GmqGetDeadLetter 从统一死信队列查询所有消息
func (c *RabbitMQConn) GmqGetDeadLetter(ctx context.Context) (msgs []core.DeadLetterMsgDTO, err error) {
	// 0. 确保统一死信交换机和队列已创建（幂等操作）
	if err := c.setupUnifiedDeadLetter(); err != nil {
		return nil, err
	}

	// 1. 设置QoS，避免一次性拉取过多消息导致内存溢出
	if err = c.channel.Qos(100, 0, false); err != nil {
		return nil, fmt.Errorf("set qos failed: %w", err)
	}
	// 2. 从统一死信队列拉取消息
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// BasicGet 拉取单条消息（noAck=false：不自动确认）
			msg, ok, err := c.channel.Get(c.unifiedDLQueue, false)
			if err != nil {
				log.Printf("get dead letter msg failed (queue=%s): %v", c.unifiedDLQueue, err)
				break
			}
			// 队列为空，退出循环
			if !ok {
				break
			}
			// 3. 解析死信消息（转为前端易读格式）
			dto := core.DeadLetterMsgDTO{
				MessageID:   msg.MessageId,
				Body:        string(msg.Body),
				Headers:     convertHeaders(msg.Headers),
				Timestamp:   msg.Timestamp.Format("2006-01-02 15:04:05"),
				Exchange:    msg.Exchange,
				RoutingKey:  msg.RoutingKey,
				Topic:       c.unifiedDLQueue,
				DeliveryTag: msg.DeliveryTag,
			}
			// 解析死信原因（从headers中提取）
			dto.DeadReason = parseDeadLetterReason(msg.Headers)
			msgs = append(msgs, dto)
			// TODO : 需要补充写入数据库
			log.Printf("✅ fetch dead letter msg success: %v", dto)
			// 死信队列中的消息保存成功后才能进行确认消息
			if err := msg.Ack(false); err != nil {
				log.Printf("nack msg failed (deliveryTag=%d): %v", msg.DeliveryTag, err)
			}
		}
	}
}

// subscribeDeadLetter 订阅统一死信队列的消息
func (c *RabbitMQConn) subscribeDeadLetter(ctx context.Context) (err error) {
	// 1. 设置QoS
	if err = c.channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set qos failed: %w", err)
	}
	// 2. 订阅统一死信队列
	messages, err := c.channel.Consume(
		c.unifiedDLQueue, // 统一死信队列
		c.unifiedDLQueue, // 消费者名称
		false,            // auto-ack (手动确认)
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("consume unified dead letter queue failed: %w", err)
	}
	log.Printf("✅ Subscribed to unified dead letter queue: %s", c.unifiedDLQueue)
	// 3. 处理死信消息
	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Stopping consumption of unified dead letter queue")
			return nil
		case msg, ok := <-messages:
			if !ok {
				log.Printf("⚠️ Unified dead letter queue consumer closed")
				return nil
			}
			// 构建死信消息DTO
			dto := core.DeadLetterMsgDTO{
				MessageID:   msg.MessageId,
				Body:        string(msg.Body),
				Headers:     convertHeaders(msg.Headers),
				Timestamp:   msg.Timestamp.Format("2006-01-02 15:04:05"),
				Exchange:    msg.Exchange,
				RoutingKey:  msg.RoutingKey,
				Topic:       c.unifiedDLQueue,
				DeliveryTag: msg.DeliveryTag,
				DeadReason:  parseDeadLetterReason(msg.Headers),
			}
			log.Printf("✅ Received dead letter message: %v", dto)
			// TODO : 需要补充写入数据库
			// 死信队列中的消息保存成功后才能进行确认消息
			if err := msg.Ack(false); err != nil {
				log.Printf("nack msg failed (deliveryTag=%d): %v", msg.DeliveryTag, err)
			}
		}
	}
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
