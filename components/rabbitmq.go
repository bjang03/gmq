package components

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/utils"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strings"
	"time"
)

type RabbitMQPubMessage struct {
	core.PubMessage
	Durable bool // 是否持久化
}

type RabbitMQPubDelayMessage struct {
	core.PubMessage
	Durable      bool // 是否持久化
	DelaySeconds int  // 延迟时间(秒)
}

// RabbitMQSubMessage RabbitMQ订阅消息结构，支持持久化订阅和延迟消费
type RabbitMQSubMessage struct {
	core.SubMessage[any]
}

func (n RabbitMQPubMessage) GetGmqPublishMsgType() {
	//TODO implement me
	panic("implement me")
}

func (n RabbitMQPubDelayMessage) GetGmqPublishDelayMsgType() {
	//TODO implement me
	panic("implement me")
}

// rabbitMQMsg RabbitMQ消息队列实现
type rabbitMQMsg struct {
	rabbitMQUrl             string
	rabbitMQPort            string
	rabbitMQUsername        string
	rabbitMQPassword        string
	rabbitMQVHost           string
	rabbitMQDsName          string
	rabbitMQConn            *amqp.Connection
	rabbitMQChannel         *amqp.Channel
	rabbitMQConnectedAt     time.Time
	rabbitMQLastPingLatency float64
}

// GmqPing 检测RabbitMQ连接状态
func (c *rabbitMQMsg) GmqPing(_ context.Context) bool {
	if c.rabbitMQConn == nil || c.rabbitMQChannel == nil {
		return false
	}

	if c.rabbitMQConn.IsClosed() || c.rabbitMQChannel.IsClosed() {
		return false
	}

	start := time.Now()
	c.rabbitMQLastPingLatency = float64(time.Since(start).Milliseconds())

	return true
}

// GmqConnect 连接RabbitMQ服务器
func (c *rabbitMQMsg) GmqConnect(ctx context.Context) (err error) {
	if c.rabbitMQUrl == "" {
		return fmt.Errorf("RabbitMQ connect address is empty")
	}
	if c.rabbitMQPort == "" {
		return fmt.Errorf("RabbitMQ connect port is empty")
	}
	if c.rabbitMQUsername == "" {
		return fmt.Errorf("RabbitMQ connect username is empty")
	}
	if c.rabbitMQPassword == "" {
		return fmt.Errorf("RabbitMQ connect password is empty")
	}

	// 安全地关闭旧连接（仅针对该数据源）
	if c.rabbitMQConn != nil && !c.rabbitMQConn.IsClosed() {
		c.rabbitMQConn.Close()
	}
	if c.rabbitMQChannel != nil && !c.rabbitMQChannel.IsClosed() {
		c.rabbitMQChannel.Close()
	}
	// 连接 RabbitMQ
	// 构建连接 URL
	url := "amqp://" + c.rabbitMQUsername + ":" + c.rabbitMQPassword + "@" + c.rabbitMQUrl + ":" + c.rabbitMQPort + "/" + c.rabbitMQVHost

	// 创建连接
	newConn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("RabbitMQ [%s] connect failed: %w", c.rabbitMQDsName, err)
	}

	// 创建 Channel
	newChannel, err := newConn.Channel()
	if err != nil {
		newConn.Close()
		return fmt.Errorf("RabbitMQ JetStream connect failed: %w", err)
	}
	c.rabbitMQConn = newConn
	c.rabbitMQChannel = newChannel
	c.rabbitMQConnectedAt = time.Now()
	return nil
}

// GmqClose 关闭RabbitMQ连接
func (c *rabbitMQMsg) GmqClose(ctx context.Context) (err error) {
	if c.rabbitMQConn != nil {
		c.rabbitMQConn.Close()
		c.rabbitMQConn = nil
	}
	if c.rabbitMQChannel != nil {
		c.rabbitMQChannel.Close()
		c.rabbitMQChannel = nil
	}
	return nil
}

// GmqPublish 发布消息
func (c *rabbitMQMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	cfg, ok := msg.(*RabbitMQPubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubMessage")
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
func (c *rabbitMQMsg) GmqPublishDelay(ctx context.Context, msg core.PublishDelay) (err error) {
	cfg, ok := msg.(*RabbitMQPubDelayMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *RabbitMQPubMessage")
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

// Publish 发布消息
func (c *rabbitMQMsg) createPublish(ctx context.Context, queueName string, durable bool, delayTime int, data any) error {
	delayMsg := delayTime > 0

	// 1. 决定 Exchange 类型
	exchangeType := "fanout"
	exchangeName := queueName
	routingKey := queueName
	args := amqp.Table{}
	if delayMsg {
		exchangeType = "x-delayed-message"
		exchangeName = queueName + ".delayed"
		args["x-delayed-type"] = "fanout"
	}
LOOP:
	if c.rabbitMQChannel == nil {
		time.Sleep(2 * time.Second)
		goto LOOP
	}
	// 2. 声明 Exchange（使用 exchangeName 而不是 queueName）
	if err := c.rabbitMQChannel.ExchangeDeclare(
		exchangeName, // 修复：使用正确的交换机名称
		exchangeType,
		durable,
		false, // autoDelete
		false, // internal
		false, // noWait
		args,
	); err != nil {
		if strings.Contains(err.Error(), "channel/connection is not open") || strings.Contains(err.Error(), "i/o timeout") {
			time.Sleep(2 * time.Second)
			goto LOOP
		}
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	// 3. 声明队列
	if _, err := c.rabbitMQChannel.QueueDeclare(
		queueName,
		durable,
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	); err != nil {
		if !c.GmqPing(ctx) {
			time.Sleep(2 * time.Second)
			goto LOOP
		}
		return fmt.Errorf("declare queue failed: %w", err)
	}

	// 4. 绑定队列
	if err := c.rabbitMQChannel.QueueBind(
		queueName,
		routingKey,   // routingKey 路由键
		exchangeName, // exchange 交换机名称
		false,        // noWait
		nil,          // args
	); err != nil {
		if !c.GmqPing(ctx) {
			time.Sleep(2 * time.Second)
			goto LOOP
		}
		return fmt.Errorf("bind queue failed: %w", err)
	}

	// 5. 序列化数据
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data failed: %w", err)
	}
	// 6. 发布消息
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
	if delayMsg {
		duration := delayTime * 1000 // 延迟时间（毫秒）= 秒 * 1000
		publishing.Headers = amqp.Table{
			"x-delay": duration,
		}
	}
	err = c.rabbitMQChannel.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		false, false,
		publishing,
	)
	if err != nil {
		if !c.GmqPing(ctx) {
			time.Sleep(2 * time.Second)
			goto LOOP
		}
		return fmt.Errorf("publish message failed: %w", err)
	}
	log.Printf("📨 publish message success: queueName=%s, data=%v", queueName, data)
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *rabbitMQMsg) GmqSubscribe(ctx context.Context, msg any) (result interface{}, err error) {
	cfg, ok := msg.(*RabbitMQSubMessage)
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
			if c.rabbitMQChannel == nil {
				time.Sleep(2 * time.Second)
				goto LOOP
			}
			if err = c.rabbitMQChannel.Qos(cfg.FetchCount, 0, false); err != nil {
				if !c.GmqPing(ctx) {
					time.Sleep(2 * time.Second)
					goto LOOP
				}
				return fmt.Errorf("set qos failed: %w", err)
			}

			msg, err := c.rabbitMQChannel.Consume(
				cfg.QueueName,    // queue
				cfg.ConsumerName, // consumer
				cfg.AutoAck,      // auto-ack (根据配置决定)
				false,            // exclusive
				false,            // no-local
				false,            // no-wait
				nil,              // args
			)
			if err != nil {
				if !c.GmqPing(ctx) || strings.Contains(err.Error(), "NOT_FOUND - no queue") {
					time.Sleep(2 * time.Second)
					goto LOOP
				}
				return fmt.Errorf("consume failed: %w", err)
			}
			for {
				select {
				case <-ctx.Done():
					return nil
				case m, ok := <-msg:
					if !ok {
						time.Sleep(2 * time.Second)
						goto LOOP
					}
					if cfg.AutoAck {
						_ = m.Ack(false)
					}
					var data map[string]interface{}
					if err := json.Unmarshal(m.Body, &data); err != nil {
						// 如果不是 JSON，直接使用原始内容
						data = map[string]interface{}{
							"data": string(m.Body),
						}
					}
					// 调用用户提供的处理函数处理业务逻辑
					if err := cfg.HandleFunc(ctx, data); err == nil {
						if !cfg.AutoAck {
							// 业务处理完后，手动确认消息
							err := m.Ack(false)
							if err != nil {
								log.Printf("❌ 确认消息失败: %v", err)
							} else {
								log.Printf("✅ 确认消息成功")
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
func (c *rabbitMQMsg) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:            "rabbitmq",
		Type:            "rabbitmq",
		ServerAddr:      c.rabbitMQUrl,
		ConnectedAt:     c.rabbitMQConnectedAt.Format("2006-01-02 15:04:05"),
		LastPingLatency: c.rabbitMQLastPingLatency,
	}

	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	// 计算运行时间
	if !c.rabbitMQConnectedAt.IsZero() {
		m.UptimeSeconds = int64(time.Since(c.rabbitMQConnectedAt).Seconds())
	}

	return m
}
