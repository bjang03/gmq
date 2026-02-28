package types

import "time"

// 默认重连配置
const (
	BaseReconnectDelay = 5 * time.Second  // 基础重连延迟
	MaxReconnectDelay  = 60 * time.Second // 最大重连延迟
	ConnectTimeout     = 30 * time.Second // 连接超时
)

// 默认重试配置
const (
	MsgRetryDeliver = 3               // 消息的最大重试次数，达到此值后进入死信队列(默认3次)
	MsgRetryDelay   = 3 * time.Second // 消息的重试延迟时间(秒，默认3s)
)
