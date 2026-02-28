package core

import (
	"context"
	"github.com/bjang03/gmq/types"
)

// Gmq 消息队列统一接口定义
type Gmq interface {
	GmqGetConn(ctx context.Context) any                                // 获取消息队列连接对象
	GmqConnect(ctx context.Context, cfg map[string]any) error          // 连接消息队列
	GmqPublish(ctx context.Context, msg types.Publish) error           // 发布消息
	GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error // 发布延迟消息
	GmqSubscribe(ctx context.Context, msg types.Subscribe) error       // 订阅消息
	GmqPing(ctx context.Context) bool                                  // 检测连接状态
	GmqClose(ctx context.Context) error                                // 关闭连接
	GmqAck(ctx context.Context, msg *types.AckMessage) error           // 确认消息
	GmqNak(ctx context.Context, msg *types.AckMessage) error           // 拒绝消息
}
