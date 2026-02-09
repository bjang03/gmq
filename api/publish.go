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
}

// Publish 发布消息
func Publish(ctx context.Context, req *PublishReq) (res interface{}, err error) {
	pipeline := core.GetGmq(req.MqName)
	if pipeline == nil {
		return nil, fmt.Errorf("[%s] pipeline not found", req.MqName)
	}

	err = pipeline.GmqPublish(ctx, &mq.NatsPubMessage{
		PubMessage: core.PubMessage{
			QueueName: req.QueueName,
			Data:      req.Message,
		},
	})
	return
}
