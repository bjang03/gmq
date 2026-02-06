package controller

import (
	"context"
	"fmt"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web/dto"
)

// Publish 发布消息
func Publish(ctx context.Context, req *dto.PublishReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.ServerName)
	if pipeline != nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.ServerName)
	}
	err = pipeline.GmqPublish(ctx, &components.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: req.QueueName,
			Data:      req.Message,
		},
	})
	return
}

// Subscribe 订阅消息
func Subscribe(ctx context.Context, req *dto.SubscribeReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.ServerName)
	if pipeline != nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.ServerName)
	}

	res, err = pipeline.GmqSubscribe(ctx, &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName: req.QueueName,
		},
	})
	return
}
