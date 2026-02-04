package gmq

import (
	"context"
	"time"
)

type PubMessage struct {
	QueueName string
	Data      any
}
type SubMessage struct {
	QueueName  string
	AutoAck    bool
	FetchCount int
	HandleFunc func(ctx context.Context, message map[string]interface{}) error
}
type Publish interface {
	GetGmqPublishMsgType()
}

type Subscribe interface {
	GetGmqSubscribeMsgType()
}
type Parser interface {
	GmqParseData(data any) (dt any, err error)
}
type Gmq interface {
	// GmqPublish 发布消息
	GmqPublish(ctx context.Context, msg Publish) error
	// GmqSubscribe 订阅消息
	GmqSubscribe(ctx context.Context, msg Subscribe) error
	// GmqPing 检测连接状态
	GmqPing(ctx context.Context) bool
	// GmqConnect 重连
	GmqConnect(ctx context.Context) error
	// GmqClose 关闭连接
	GmqClose(ctx context.Context) error
}

var GmqPlugins map[string]Gmq

func GmqRegister(name string, plugin Gmq) {
	//todo 检查gmq对象中是否有连接配置，如果没有直接返回
	GmqPlugins[name] = plugin
	ctx := context.Background()
	go func(name string, plugin Gmq) {
		for {
			select {
			case <-ctx.Done():
				_ = plugin.GmqClose(ctx)
				return
			default:
				if plugin.GmqPing(ctx) {
					time.Sleep(10 * time.Second)
					continue
				}
				if err := plugin.GmqConnect(ctx); err != nil {
					time.Sleep(10 * time.Second)
					continue
				}

			}
		}
	}(name, plugin)
}
