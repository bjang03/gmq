// Package gmq 提供统一的消息队列抽象接口，支持多种消息中间件(NATS、Redis-Stream、RabbitMQ等)
package core

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// 默认重试配置
const (
	DefaultRetryAttempts = 3
	DefaultRetryDelay    = 500 * time.Millisecond
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
	// GmqConnect 连接消息队列
	GmqConnect(ctx context.Context) error
	// GmqPublish 发布消息
	GmqPublish(ctx context.Context, msg Publish) error
	// GmqSubscribe 订阅消息，返回订阅对象
	GmqSubscribe(ctx context.Context, msg any) (interface{}, error)
	// GmqUnsubscribe 取消订阅，传入订阅对象
	GmqUnsubscribe(ctx context.Context, topic, consumerName string, subObj interface{}) error
	// GmqPing 检测连接状态
	GmqPing(ctx context.Context) bool
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
var (
	GmqPlugins = make(map[string]*GmqPipeline)
	pluginsMu  sync.RWMutex
)

// GmqPipeline 消息队列管道包装器，用于统一监控指标处理
type GmqPipeline struct {
	name            string
	plugin          Gmq
	metrics         pipelineMetrics
	connectedAt     int64 // Unix timestamp in seconds, atomic access
	lastPingLatency int64 // milliseconds, atomic access

	// 并发控制 - 统一管理所有并发访问
	mu sync.RWMutex

	// 订阅管理 - 统一管理所有订阅对象（包括状态和具体的订阅引用）
	subscriptions map[string]interface{} // key 为 topic:consumerName，value 为具体的订阅对象
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
}

// newGmqPipeline 创建新的管道包装器
func newGmqPipeline(name string, plugin Gmq) *GmqPipeline {
	return &GmqPipeline{
		name:   name,
		plugin: plugin,
	}
}

// GmqPublish 发布消息（带统一监控和重试）
func (p *GmqPipeline) GmqPublish(ctx context.Context, msg Publish) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	start := time.Now()
	var err error

	// 带重试的发布
	for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				break
			}
		}

		err = p.plugin.GmqPublish(ctx, msg)
		if err == nil {
			break
		}
	}

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

// GmqSubscribe 订阅消息（带统一监控和重试）
func (p *GmqPipeline) GmqSubscribe(ctx context.Context, msg any) error {
	start := time.Now()
	var err error

	// 提取 topic 和 consumerName（从 msg 中解析）
	topic, consumerName := p.extractSubscriptionInfo(msg)
	subKey := p.getSubKey(topic, consumerName)

	// 检查是否已订阅
	p.mu.RLock()
	_, alreadySubscribed := p.subscriptions[subKey]
	p.mu.RUnlock()

	if alreadySubscribed {
		return fmt.Errorf("already subscribed to topic: %s", topic)
	}

	var subObj interface{}
	// 带重试的订阅
	for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				break
			}
		}

		subObj, err = p.plugin.GmqSubscribe(ctx, msg)
		if err == nil {
			break
		}
	}

	if err != nil {
		// 记录指标
		latency := int64(time.Since(start).Milliseconds())
		atomic.AddInt64(&p.metrics.totalLatency, latency)
		atomic.AddInt64(&p.metrics.latencyCount, 1)
		atomic.AddInt64(&p.metrics.subscribeFailed, 1)
		return err
	}

	// 保存订阅对象到管道层
	p.mu.Lock()
	if subObj != nil {
		p.subscriptions[subKey] = subObj
	} else {
		p.subscriptions[subKey] = true
	}
	p.mu.Unlock()

	// 记录指标
	latency := int64(time.Since(start).Milliseconds())
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)
	atomic.AddInt64(&p.metrics.subscribeCount, 1)

	return nil
}

// GmqUnsubscribe 取消订阅
func (p *GmqPipeline) GmqUnsubscribe(ctx context.Context, topic, consumerName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	subKey := p.getSubKey(topic, consumerName)

	// 从管道层获取订阅对象
	subObj, exists := p.subscriptions[subKey]
	if exists {
		delete(p.subscriptions, subKey)
	}

	if !exists {
		return fmt.Errorf("subscription not found for topic: %s", topic)
	}

	// 调用插件层取消订阅，传入订阅对象供插件使用
	return p.plugin.GmqUnsubscribe(ctx, topic, consumerName, subObj)
}

// extractSubscriptionInfo 从订阅消息中提取 topic 和 consumerName
func (p *GmqPipeline) extractSubscriptionInfo(msg any) (topic, consumerName string) {
	// 使用反射提取信息，避免硬编码类型
	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() == reflect.Struct {
		queueNameField := val.FieldByName("QueueName")
		consumerNameField := val.FieldByName("ConsumerName")

		if queueNameField.IsValid() && queueNameField.Kind() == reflect.String {
			topic = queueNameField.String()
		}
		if consumerNameField.IsValid() && consumerNameField.Kind() == reflect.String {
			consumerName = consumerNameField.String()
		}
	}

	return topic, consumerName
}

// getSubKey 生成订阅的唯一key
func (p *GmqPipeline) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	return topic
}

// GmqPing 检测连接状态
func (p *GmqPipeline) GmqPing(ctx context.Context) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	start := time.Now()
	connected := p.plugin.GmqPing(ctx)
	latency := int64(time.Since(start).Milliseconds())
	atomic.StoreInt64(&p.lastPingLatency, latency)
	return connected
}

// GmqConnect 连接消息队列
func (p *GmqPipeline) GmqConnect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.plugin.GmqConnect(ctx)
	if err == nil {
		atomic.StoreInt64(&p.connectedAt, time.Now().Unix())
	}
	return err
}

// GmqClose 关闭连接
func (p *GmqPipeline) GmqClose(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	totalAttempted := publishCount + subscribeCount + publishFailed + subscribeFailed
	if totalAttempted > 0 {
		errorRate = float64(publishFailed+subscribeFailed) / float64(totalAttempted) * 100
	}

	// 使用管道层的连接时间（原子读取）
	var uptimeSeconds int64
	var connectedAtStr string
	connectedAtUnix := atomic.LoadInt64(&p.connectedAt)
	if connectedAtUnix > 0 {
		connectedAt := time.Unix(connectedAtUnix, 0)
		uptimeSeconds = int64(time.Since(connectedAt).Seconds())
		connectedAtStr = connectedAt.Format("2006-01-02 15:04:05")
	}

	// 计算吞吐量（每秒）
	var throughputPerSec, publishPerSec, subscribePerSec float64
	if uptimeSeconds > 0 {
		duration := float64(uptimeSeconds)
		throughputPerSec = float64(messageCount) / duration
		publishPerSec = float64(publishCount) / duration
		subscribePerSec = float64(subscribeCount) / duration
	}

	// 合并指标（插件提供基础信息，管道层提供客户端统计和连接信息）
	return &Metrics{
		Name:             p.name,
		Type:             pluginMetrics.Type,
		Status:           pluginMetrics.Status,
		ServerAddr:       pluginMetrics.ServerAddr,
		ConnectedAt:      connectedAtStr,
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
		LastPingLatency:  float64(atomic.LoadInt64(&p.lastPingLatency)),
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

// globalShutdown 用于优雅关闭信号
var (
	globalShutdown     = make(chan struct{})
	globalShutdownOnce sync.Once
)

// GmqRegister 注册消息队列插件
// 启动后台协程自动维护连接状态，断线自动重连
func GmqRegister(name string, plugin Gmq) {
	// 创建管道包装器
	pipeline := newGmqPipeline(name, plugin)

	pluginsMu.Lock()
	GmqPlugins[name] = pipeline
	pluginsMu.Unlock()

	go func(name string, p *GmqPipeline) {
		// 重连退避配置
		const (
			baseReconnectDelay = 5 * time.Second
			maxReconnectDelay  = 60 * time.Second
		)
		reconnectDelay := baseReconnectDelay

		for {
			select {
			case <-globalShutdown:
				// 收到关闭信号，关闭连接并退出
				_ = p.GmqClose(context.Background())
				return
			default:
				if p.GmqPing(context.Background()) {
					// 连接正常，重置退避时间
					reconnectDelay = baseReconnectDelay
					time.Sleep(10 * time.Second)
					continue
				}

				// 连接断开，尝试重连
				if err := p.GmqConnect(context.Background()); err != nil {
					// 重连失败，增加退避时间
					time.Sleep(reconnectDelay)
					reconnectDelay *= 2
					if reconnectDelay > maxReconnectDelay {
						reconnectDelay = maxReconnectDelay
					}
					continue
				}

				// 重连成功，重置退避时间
				reconnectDelay = baseReconnectDelay
			}
		}
	}(name, pipeline)
}

// Shutdown 优雅关闭所有消息队列连接
func Shutdown(ctx context.Context) error {
	globalShutdownOnce.Do(func() {
		select {
		case <-globalShutdown:
			// channel 已关闭，跳过
		default:
			close(globalShutdown)
		}
	})

	pluginsMu.RLock()
	pipelines := make([]*GmqPipeline, 0, len(GmqPlugins))
	for _, p := range GmqPlugins {
		pipelines = append(pipelines, p)
	}
	pluginsMu.RUnlock()

	var lastErr error
	for _, p := range pipelines {
		if err := p.GmqClose(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GetGmq 获取已注册的消息队列管道
func GetGmq(name string) (*GmqPipeline, bool) {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	pipeline, ok := GmqPlugins[name]
	return pipeline, ok
}

// GetAllGmq 获取所有已注册的消息队列管道的副本
func GetAllGmq() map[string]*GmqPipeline {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()

	// 返回副本，避免外部修改
	result := make(map[string]*GmqPipeline, len(GmqPlugins))
	for k, v := range GmqPlugins {
		result[k] = v
	}
	return result
}
