// Package gmq 提供统一的消息队列抽象接口，支持多种消息中间件(NATS、Redis-Stream、RabbitMQ等)
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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

// Metrics 监控指标
type Metrics struct {
	Name             string                 `json:"name"`             // 消息队列名称
	Type             string                 `json:"type"`             // 消息队列类型：nats/redis/rabbitmq/kafka
	Status           string                 `json:"status"`           // 连接状态：connected/disconnected
	ServerAddr       string                 `json:"serverAddr"`       // 服务器地址
	ConnectedAt      string                 `json:"connectedAt"`      // 连接时间
	UptimeSeconds    int64                  `json:"uptimeSeconds"`    // 运行时间(秒)
	MessageCount     int64                  `json:"messageCount"`     // 已处理消息总数
	PublishCount     int64                  `json:"publishCount"`     // 发布消息数
	SubscribeCount   int64                  `json:"subscribeCount"`   // 订阅消息数
	PendingMessages  int64                  `json:"pendingMessages"`  // 待处理消息数
	PendingAckCount  int64                  `json:"pendingAckCount"`  // 待确认消息数
	PublishFailed    int64                  `json:"publishFailed"`    // 发布失败数
	SubscribeFailed  int64                  `json:"subscribeFailed"`  // 订阅失败数
	MsgsIn           int64                  `json:"msgsIn"`           // 服务端流入消息数
	MsgsOut          int64                  `json:"msgsOut"`          // 服务端流出消息数
	BytesIn          uint64                 `json:"bytesIn"`          // 流入字节数
	BytesOut         uint64                 `json:"bytesOut"`         // 流出字节数
	AverageLatency   float64                `json:"averageLatency"`   // 平均延迟(毫秒)
	LastPingLatency  float64                `json:"lastPingLatency"`  // 最近一次ping延迟(毫秒)
	MaxLatency       float64                `json:"maxLatency"`       // 最大延迟(毫秒)
	MinLatency       float64                `json:"minLatency"`       // 最小延迟(毫秒)
	ThroughputPerSec float64                `json:"throughputPerSec"` // 总吞吐量
	PublishPerSec    float64                `json:"publishPerSec"`    // 发布吞吐
	SubscribePerSec  float64                `json:"subscribePerSec"`  // 订阅吞吐
	ErrorRate        float64                `json:"errorRate"`        // 错误率
	ReconnectCount   int64                  `json:"reconnectCount"`   // 重连次数
	ServerMetrics    map[string]interface{} `json:"serverMetrics"`    // 服务端详细信息
	Extensions       map[string]interface{} `json:"extensions"`       // 扩展指标
}

// GmqPlugins 已注册的消息队列插件集合
var GmqPlugins = make(map[string]*GmqPipeline)

// GmqPipeline 消息队列管道包装器，用于统一监控指标处理
type GmqPipeline struct {
	name    string
	plugin  Gmq
	metrics pipelineMetrics
}

// pipelineMetrics 管道监控指标
type pipelineMetrics struct {
	messageCount    int64
	publishCount    int64
	subscribeCount  int64
	publishFailed   int64
	subscribeFailed int64
	totalLatency    int64
	latencyCount    int64
	pendingMessages int64
	mu              sync.RWMutex
}

// newGmqPipeline 创建新的管道包装器
func newGmqPipeline(name string, plugin Gmq) *GmqPipeline {
	return &GmqPipeline{
		name:   name,
		plugin: plugin,
	}
}

// GmqPublish 发布消息（带统一监控）
func (p *GmqPipeline) GmqPublish(ctx context.Context, msg Publish) error {
	start := time.Now()

	// 调用实际插件的发布方法
	err := p.plugin.GmqPublish(ctx, msg)

	// 记录指标
	latency := int64(time.Since(start).Milliseconds())
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)

	if err != nil {
		atomic.AddInt64(&p.metrics.publishFailed, 1)
	} else {
		atomic.AddInt64(&p.metrics.publishCount, 1)
		atomic.AddInt64(&p.metrics.messageCount, 1)
	}

	return err
}

// GmqSubscribe 订阅消息（带统一监控）
func (p *GmqPipeline) GmqSubscribe(ctx context.Context, msg any) error {
	start := time.Now()

	// 调用实际插件的订阅方法
	err := p.plugin.GmqSubscribe(ctx, msg)

	// 记录指标
	latency := int64(time.Since(start).Milliseconds())
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)

	if err != nil {
		atomic.AddInt64(&p.metrics.subscribeFailed, 1)
	} else {
		atomic.AddInt64(&p.metrics.subscribeCount, 1)
		atomic.AddInt64(&p.metrics.messageCount, 1)
	}

	return err
}

// GmqPing 检测连接状态
func (p *GmqPipeline) GmqPing(ctx context.Context) bool {
	return p.plugin.GmqPing(ctx)
}

// GmqConnect 连接消息队列
func (p *GmqPipeline) GmqConnect(ctx context.Context) error {
	return p.plugin.GmqConnect(ctx)
}

// GmqClose 关闭连接
func (p *GmqPipeline) GmqClose(ctx context.Context) error {
	return p.plugin.GmqClose(ctx)
}

// GetMetrics 获取统一监控指标
func (p *GmqPipeline) GetMetrics(ctx context.Context) *Metrics {
	// 获取插件自身的指标
	pluginMetrics := p.plugin.GetMetrics(ctx)

	// 获取管道层面的指标
	latencyCount := atomic.LoadInt64(&p.metrics.latencyCount)
	totalLatency := atomic.LoadInt64(&p.metrics.totalLatency)
	messageCount := atomic.LoadInt64(&p.metrics.messageCount)
	publishCount := atomic.LoadInt64(&p.metrics.publishCount)
	subscribeCount := atomic.LoadInt64(&p.metrics.subscribeCount)
	publishFailed := atomic.LoadInt64(&p.metrics.publishFailed)
	subscribeFailed := atomic.LoadInt64(&p.metrics.subscribeFailed)

	var avgLatency float64
	if latencyCount > 0 {
		avgLatency = float64(totalLatency) / float64(latencyCount)
	}

	// 计算错误率
	var errorRate float64
	totalOps := publishCount + subscribeCount + publishFailed + subscribeFailed
	if totalOps > 0 {
		errorRate = float64(publishFailed+subscribeFailed) / float64(totalOps) * 100
	}

	// 计算吞吐量（每秒）- 使用插件层的连接时间
	var throughputPerSec, publishPerSec, subscribePerSec float64
	// 优先使用插件层的连接时间，如果没有则使用管道层的
	uptimeSeconds := pluginMetrics.UptimeSeconds
	if uptimeSeconds > 0 {
		duration := float64(uptimeSeconds)
		throughputPerSec = float64(messageCount) / duration
		publishPerSec = float64(publishCount) / duration
		subscribePerSec = float64(subscribeCount) / duration
	}

	// 合并指标（插件提供基础信息，管道层提供客户端统计）
	return &Metrics{
		Name:             p.name,
		Type:             pluginMetrics.Type,
		Status:           pluginMetrics.Status,
		ServerAddr:       pluginMetrics.ServerAddr,
		ConnectedAt:      pluginMetrics.ConnectedAt,
		UptimeSeconds:    uptimeSeconds,
		MessageCount:     messageCount,
		PublishCount:     publishCount,
		SubscribeCount:   subscribeCount,
		PendingMessages:  pluginMetrics.PendingMessages,
		PendingAckCount:  pluginMetrics.PendingAckCount,
		PublishFailed:    publishFailed,
		SubscribeFailed:  subscribeFailed,
		MsgsIn:           pluginMetrics.MsgsIn,
		MsgsOut:          pluginMetrics.MsgsOut,
		BytesIn:          pluginMetrics.BytesIn,
		BytesOut:         pluginMetrics.BytesOut,
		AverageLatency:   avgLatency,
		LastPingLatency:  pluginMetrics.LastPingLatency,
		MaxLatency:       pluginMetrics.MaxLatency,
		MinLatency:       pluginMetrics.MinLatency,
		ThroughputPerSec: throughputPerSec,
		PublishPerSec:    publishPerSec,
		SubscribePerSec:  subscribePerSec,
		ErrorRate:        errorRate,
		ReconnectCount:   pluginMetrics.ReconnectCount,
		ServerMetrics:    pluginMetrics.ServerMetrics,
		Extensions:       pluginMetrics.Extensions,
	}
}

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，每10秒检测一次，断线自动重连
func GmqRegister(name string, plugin Gmq) {
	// 创建管道包装器
	pipeline := newGmqPipeline(name, plugin)
	GmqPlugins[name] = pipeline

	ctx := context.Background()
	go func(name string, p *GmqPipeline) {
		for {
			select {
			case <-ctx.Done():
				_ = p.GmqClose(ctx)
				return
			default:
				if p.GmqPing(ctx) {
					time.Sleep(10 * time.Second)
					continue
				}
				if err := p.GmqConnect(ctx); err != nil {
					time.Sleep(10 * time.Second)
					continue
				}
			}
		}
	}(name, pipeline)
}

// GetGmq 获取已注册的消息队列管道
func GetGmq(name string) (*GmqPipeline, bool) {
	pipeline, ok := GmqPlugins[name]
	return pipeline, ok
}

// GetAllGmq 获取所有已注册的消息队列管道
func GetAllGmq() map[string]*GmqPipeline {
	return GmqPlugins
}
