package api

import (
	"context"
	"fmt"

	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/mq"
)

// PublishReq 发布消息请求
type PublishReq struct {
	ServerName string `json:"serverName" validate:"required"` // 回调服务名
	MqName     string `json:"mqName" validate:"required"`     // 消息队列名
	QueueName  string `json:"queueName" validate:"required"`
	Message    string `json:"message" validate:"required,min=1"`
	Durable    bool   `json:"durable"`
}

// PublishDelayReq 发布延迟消息请求
type PublishDelayReq struct {
	ServerName   string `json:"serverName" validate:"required"` // 回调服务名
	MqName       string `json:"mqName" validate:"required"`     // 消息队列名
	QueueName    string `json:"queueName" validate:"required"`
	Message      string `json:"message" validate:"required,min=1"`
	Durable      bool   `json:"durable"`
	DelaySeconds int    `json:"delaySeconds" validate:"required,min=1"`
}

// Publish 发布消息
func Publish(ctx context.Context, req *PublishReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.MqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.MqName)
	}

	baseMsg := core.PubMessage{
		QueueName: req.QueueName,
		Data:      req.Message,
	}

	switch req.MqName {
	case "nats":
		err = pipeline.GmqPublish(ctx, &mq.NatsPubMessage{PubMessage: baseMsg, Durable: req.Durable})
	case "rabbitmq":
		err = pipeline.GmqPublish(ctx, &mq.RabbitMQPubMessage{PubMessage: baseMsg, Durable: req.Durable})
	case "redis":
		err = pipeline.GmqPublish(ctx, &mq.RedisPubMessage{PubMessage: baseMsg})
	default:
		return nil, fmt.Errorf("unsupported mq type: %s", req.MqName)
	}
	return
}

// PublishDelay 发布延迟消息
func PublishDelay(ctx context.Context, req *PublishDelayReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.MqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.MqName)
	}

	baseMsg := core.PubDelayMessage{
		PubMessage: core.PubMessage{
			QueueName: req.QueueName,
			Data:      req.Message,
		},
		DelaySeconds: req.DelaySeconds,
	}

	switch req.MqName {
	case "nats":
		err = pipeline.GmqPublishDelay(ctx, &mq.NatsPubDelayMessage{PubDelayMessage: baseMsg, Durable: req.Durable})
	case "rabbitmq":
		err = pipeline.GmqPublishDelay(ctx, &mq.RabbitMQPubDelayMessage{PubDelayMessage: baseMsg, Durable: req.Durable})
	case "redis":
		return nil, fmt.Errorf("redis does not support delay message")
	default:
		return nil, fmt.Errorf("unsupported mq type: %s", req.MqName)
	}
	return
}
