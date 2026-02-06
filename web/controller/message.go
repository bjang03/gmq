package controller

import (
	"context"
	"errors"
	"regexp"

	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/web/dto"
)

// 队列名称验证规则：只允许字母、数字、下划线、横线、点号
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]+$`)

// 默认使用第一个注册的MQ
func getDefaultMQ() (*core.GmqPipeline, error) {
	// 优先使用 "nats"，如果不存在则使用第一个
	if pipeline := core.GetGmq("nats"); pipeline != nil {
		return pipeline, nil
	}
	// 获取第一个注册的MQ
	for _, pipeline := range core.GetAllGmq() {
		return pipeline, nil
	}
	return nil, errors.New("no MQ plugin registered")
}

// validateTopic 验证队列名称
func validateTopic(topic string) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}
	if len(topic) > 255 {
		return errors.New("topic length exceeds 255 characters")
	}
	if !queueNameRegex.MatchString(topic) {
		return errors.New("topic contains invalid characters")
	}
	return nil
}

// Publish 发布消息
func Publish(ctx context.Context, req *dto.PublishReq) (res interface{}, err error) {
	// 参数校验
	if err := validateTopic(req.Topic); err != nil {
		return nil, err
	}

	pipeline, err := getDefaultMQ()
	if err != nil {
		return nil, err
	}

	err = pipeline.GmqPublish(ctx, &components.NatsPubMessage{
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
func Subscribe(ctx context.Context, req *dto.SubscribeReq) (res interface{}, err error) {
	// 参数校验
	if err := validateTopic(req.Topic); err != nil {
		return nil, err
	}

	pipeline, err := getDefaultMQ()
	if err != nil {
		return nil, err
	}

	_, err = pipeline.GmqSubscribe(ctx, &components.NatsSubMessage{
		SubMessage: core.SubMessage[any]{
			QueueName: req.Topic,
		},
	})
	if err != nil {
		return nil, err
	}
	return map[string]string{"message": "subscribe success", "topic": req.Topic}, nil
}
