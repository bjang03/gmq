package controller

import (
	"context"
	"errors"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web/dto"
	"github.com/gin-gonic/gin"
)

// Publish 发布消息
func Publish(ctx context.Context, req *dto.PublishReq) (res interface{}, err error) {
	plugin := core.GmqPlugins["nats"]
	if plugin == nil {
		return nil, errors.New("nats plugin not registered")
	}

	// 参数校验
	if req.Topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	err = plugin.GmqPublish(ctx, &components.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: req.Topic,
			Data:      req.Message,
		},
	})
	if err != nil {
		return nil, err
	}
	return map[string]string{"message": "publish success", "topic": req.Topic}, nil
}

// Subscribe 订阅消息
func Subscribe(c *gin.Context, req *dto.SubscribeReq) (res interface{}, err error) {
	plugin := core.GmqPlugins["nats"]
	if plugin == nil {
		return nil, errors.New("nats plugin not registered")
	}
	err = plugin.GmqSubscribe(c.Request.Context(), &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName: req.Topic,
		},
	})
	if err != nil {
		return nil, err
	}
	return map[string]string{"message": "subscribe success", "topic": req.Topic}, nil
}
