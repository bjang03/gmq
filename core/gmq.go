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
	Name             string  // 消息队列名称
	Status           string  // 连接状态：connected/disconnected
	ConnectedAt      string  // 连接时间
	MessageCount     int64   // 已处理消息总数
	PublishCount     int64   // 发布消息数
	SubscribeCount   int64   // 订阅消息数
	PendingMessages  int64   // 待处理消息数
	PublishFailed    int64   // 发布失败数
	SubscribeFailed  int64   // 订阅失败数
	AverageLatency   float64 // 平均延迟(毫秒)
	LastPingLatency  float64 // 最近一次ping延迟(毫秒)
	ThroughputPerSec float64 // 每秒吞吐量
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

	var avgLatency float64
	if latencyCount > 0 {
		avgLatency = float64(totalLatency) / float64(latencyCount)
	}

	// 合并指标（以管道层面为准，如果插件没有提供则使用管道统计）
	return &Metrics{
		Name:             p.name,
		Status:           pluginMetrics.Status,
		ConnectedAt:      pluginMetrics.ConnectedAt,
		MessageCount:     atomic.LoadInt64(&p.metrics.messageCount),
		PublishCount:     atomic.LoadInt64(&p.metrics.publishCount),
		SubscribeCount:   atomic.LoadInt64(&p.metrics.subscribeCount),
		PendingMessages:  pluginMetrics.PendingMessages,
		PublishFailed:    atomic.LoadInt64(&p.metrics.publishFailed),
		SubscribeFailed:  atomic.LoadInt64(&p.metrics.subscribeFailed),
		AverageLatency:   avgLatency,
		LastPingLatency:  pluginMetrics.LastPingLatency,
		ThroughputPerSec: pluginMetrics.ThroughputPerSec,
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
