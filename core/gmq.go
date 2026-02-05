// Package gmq 提供统一的消息队列抽象接口，支持多种消息中间件(NATS、Redis-Stream、RabbitMQ等)
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector 通用指标收集器
// 用于收集无法从服务端直接获取的指标（如延迟统计）
type MetricsCollector struct {
	// 延迟统计
	totalLatency    int64 // 总延迟(毫秒)
	latencyCount    int64 // 延迟计数
	maxLatency      int64 // 最大延迟
	minLatency      int64 // 最小延迟
	lastPingLatency int64 // 最后一次ping延迟

	// 客户端重连统计
	reconnectCount int64

	mu sync.RWMutex
}

// NewMetricsCollector 创建新的指标收集器
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		minLatency: 999999, // 初始化为一个大值
	}
}

// RecordLatency 记录延迟
func (c *MetricsCollector) RecordLatency(latencyMs int64) {
	atomic.AddInt64(&c.totalLatency, latencyMs)
	atomic.AddInt64(&c.latencyCount, 1)

	c.mu.Lock()
	if latencyMs > c.maxLatency {
		c.maxLatency = latencyMs
	}
	if latencyMs < c.minLatency {
		c.minLatency = latencyMs
	}
	c.mu.Unlock()
}

// RecordPingLatency 记录ping延迟
func (c *MetricsCollector) RecordPingLatency(latencyMs int64) {
	atomic.StoreInt64(&c.lastPingLatency, latencyMs)
}

// RecordReconnect 记录重连
func (c *MetricsCollector) RecordReconnect() {
	atomic.AddInt64(&c.reconnectCount, 1)
}

// GetLatencyStats 获取延迟统计
func (c *MetricsCollector) GetLatencyStats() (avg, max, min, lastPing float64, count int64) {
	count = atomic.LoadInt64(&c.latencyCount)
	total := atomic.LoadInt64(&c.totalLatency)
	if count > 0 {
		avg = float64(total) / float64(count)
	}
	lastPing = float64(atomic.LoadInt64(&c.lastPingLatency))

	c.mu.RLock()
	max = float64(c.maxLatency)
	if c.minLatency != 999999 {
		min = float64(c.minLatency)
	}
	c.mu.RUnlock()

	return
}

// GetReconnectCount 获取重连次数
func (c *MetricsCollector) GetReconnectCount() int64 {
	return atomic.LoadInt64(&c.reconnectCount)
}

// PubMessage 发布消息基础结构
type PubMessage struct {
	QueueName string // 队列名称
	Data      any    // 消息数据
}

// SubMessage 订阅消息基础结构
type SubMessage[T any] struct {
	QueueName  string                                     // 队列名称
	AutoAck    bool                                       // 是否自动确认
	FetchCount int                                        // 每次拉取消息数量
	HandleFunc func(ctx context.Context, message T) error // 消息处理函数
}

// Publish 发布消息接口
type Publish interface {
	GetGmqPublishMsgType()
}

// Subscribe 订阅消息接口
type Subscribe interface {
	GetGmqSubscribeMsgType()
}

// Parser 数据解析器接口
type Parser interface {
	GmqParseData(data any) (dt any, err error)
}

// Gmq 消息队列统一接口定义
type Gmq interface {
	// GmqPublish 发布消息
	GmqPublish(ctx context.Context, msg Publish) error
	// GmqSubscribe 订阅消息
	GmqSubscribe(ctx context.Context, msg any) error
	// GmqPing 检测连接状态
	GmqPing(ctx context.Context) bool
	// GmqConnect 重连
	GmqConnect(ctx context.Context) error
	// GmqClose 关闭连接
	GmqClose(ctx context.Context) error
	// GetMetrics 获取监控指标
	GetMetrics(ctx context.Context) *Metrics
}

// Metrics 监控指标 - 支持多种消息队列的通用指标结构
// 设计原则：
// 1. 能从服务端直接获取的指标（如NATS varz），优先从服务端获取
// 2. 服务端没有的指标（如客户端延迟），通过MetricsCollector累加
// 3. PublishFailed/SubscribeFailed等客户端错误由客户端记录
type Metrics struct {
	// 基础信息
	Name        string `json:"name"`        // 消息队列名称
	Type        string `json:"type"`        // 消息队列类型：nats/redis/rabbitmq等
	Status      string `json:"status"`      // 连接状态：connected/disconnected
	ConnectedAt string `json:"connectedAt"` // 连接时间
	ServerAddr  string `json:"serverAddr"`  // 服务器地址

	// ==================== 服务端指标 (从MQ服务器获取) ====================
	// 消息统计 - 服务端统计的全局数据
	MessageCount    int64 `json:"messageCount"`    // 已处理消息总数 (服务端)
	MsgsIn          int64 `json:"msgsIn"`          // 流入消息数 (服务端)
	MsgsOut         int64 `json:"msgsOut"`         // 流出消息数 (服务端)
	BytesIn         int64 `json:"bytesIn"`         // 流入字节数 (服务端)
	BytesOut        int64 `json:"bytesOut"`        // 流出字节数 (服务端)
	PendingMessages int64 `json:"pendingMessages"` // 待处理消息数 (服务端)

	// ==================== 客户端指标 (本地累加) ====================
	// 这些指标是客户端特有的，服务端无法提供
	PublishCount    int64 `json:"publishCount"`    // 客户端发布消息数
	SubscribeCount  int64 `json:"subscribeCount"`  // 客户端订阅数
	PublishFailed   int64 `json:"publishFailed"`   // 客户端发布失败数
	SubscribeFailed int64 `json:"subscribeFailed"` // 客户端订阅失败数
	PendingAckCount int64 `json:"pendingAckCount"` // 客户端待确认消息数

	// 延迟指标 - 客户端本地测量
	AverageLatency  float64 `json:"averageLatency"`  // 平均延迟(毫秒)
	LastPingLatency float64 `json:"lastPingLatency"` // 最近一次ping延迟(毫秒)
	MaxLatency      float64 `json:"maxLatency"`      // 最大延迟
	MinLatency      float64 `json:"minLatency"`      // 最小延迟

	// 吞吐量 - 基于客户端数据计算
	ThroughputPerSec float64 `json:"throughputPerSec"` // 每秒总消息数
	PublishPerSec    float64 `json:"publishPerSec"`    // 每秒发布数
	SubscribePerSec  float64 `json:"subscribePerSec"`  // 每秒订阅数

	// 连接质量
	ReconnectCount int64   `json:"reconnectCount"` // 重连次数(客户端)
	ErrorRate      float64 `json:"errorRate"`      // 错误率(%)
	UptimeSeconds  int64   `json:"uptimeSeconds"`  // 运行时间(秒)

	// 服务端指标 (不同MQ类型有不同的指标)
	ServerMetrics ServerMetrics `json:"serverMetrics"` // 服务端详细指标

	// 扩展指标 (特定于不同消息队列的额外指标)
	Extensions map[string]any `json:"extensions"` // 扩展字段
}

// ServerMetrics 服务端指标 (各消息队列通用)
type ServerMetrics struct {
	// 连接相关
	TotalConnections  int64 `json:"totalConnections"`  // 总连接数
	ActiveConnections int64 `json:"activeConnections"` // 活跃连接数
	SlowConsumers     int64 `json:"slowConsumers"`     // 慢消费者数量

	// 内存/资源
	MemoryUsed  int64   `json:"memoryUsed"`  // 内存使用(字节)
	MemoryLimit int64   `json:"memoryLimit"` // 内存限制(字节)
	CPUUsage    float64 `json:"cpuUsage"`    // CPU使用率

	// 消息存储
	TotalSubjects  int64 `json:"totalSubjects"`  // 主题总数
	TotalChannels  int64 `json:"totalChannels"`  // 通道/队列总数
	TotalConsumers int64 `json:"totalConsumers"` // 消费者总数

	// 流量统计
	BytesIn  int64 `json:"bytesIn"`  // 流入字节数
	BytesOut int64 `json:"bytesOut"` // 流出字节数
	MsgsIn   int64 `json:"msgsIn"`   // 流入消息数
	MsgsOut  int64 `json:"msgsOut"`  // 流出消息数

	// 服务器信息
	ServerVersion string `json:"serverVersion"` // 服务器版本
	ServerID      string `json:"serverId"`      // 服务器ID
	ServerTime    string `json:"serverTime"`    // 服务器时间
}

// GmqPlugins 已注册的消息队列插件集合
var GmqPlugins = make(map[string]Gmq)

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，每10秒检测一次，断线自动重连
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
