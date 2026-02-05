package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// GmqPipeline 消息队列管道包装器，用于统一监控指标处理
type GmqPipeline struct {
	name            string
	plugin          Gmq
	metrics         pipelineMetrics
	connectedAt     int64 // Unix timestamp in seconds, atomic access
	lastPingLatency int64 // milliseconds, atomic access
	connected       int32 // 连接状态: 0=未连接, 1=已连接, atomic access

	// 并发控制 - 统一管理所有并发访问
	mu sync.RWMutex

	// 订阅管理 - 统一管理所有订阅对象（包括状态和具体的订阅引用）
	subscriptions map[string]interface{} // key 为 topic:consumerName，value 为具体的订阅对象

	// 指标缓存
	cachedMetrics   *Metrics
	metricsCacheMu  sync.RWMutex
	metricsCacheTTL time.Duration
	metricsCacheExp int64 // 缓存过期时间戳
}

// newGmqPipeline 创建新的管道包装器
func newGmqPipeline(name string, plugin Gmq) *GmqPipeline {
	return &GmqPipeline{
		name:            name,
		plugin:          plugin,
		subscriptions:   make(map[string]interface{}),
		metricsCacheTTL: 5 * time.Second,
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
	latency := time.Since(start).Milliseconds()
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
func (p *GmqPipeline) GmqSubscribe(ctx context.Context, msg any) (interface{}, error) {
	start := time.Now()
	var err error

	// 提取 topic 和 consumerName（从 msg 中解析）
	topic, consumerName := p.extractSubscriptionInfo(msg)
	subKey := p.getSubKey(topic, consumerName)

	// 检查是否已订阅（管道层统一校验）
	p.mu.RLock()
	_, alreadySubscribed := p.subscriptions[subKey]
	p.mu.RUnlock()

	if alreadySubscribed {
		return nil, fmt.Errorf("already subscribed to topic: %s", topic)
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

	// 检查订阅对象是否有效（管道层统一校验）
	if subObj != nil {
		if validator, ok := subObj.(SubscriptionValidator); ok {
			if !validator.IsValid() {
				err = fmt.Errorf("subscription is not valid")
			}
		}
	}

	if err != nil {
		// 记录指标
		latency := time.Since(start).Milliseconds()
		atomic.AddInt64(&p.metrics.totalLatency, latency)
		atomic.AddInt64(&p.metrics.latencyCount, 1)
		atomic.AddInt64(&p.metrics.subscribeFailed, 1)
		return nil, err
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
	latency := time.Since(start).Milliseconds()
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)
	atomic.AddInt64(&p.metrics.subscribeCount, 1)

	return subObj, nil
}

// extractSubscriptionInfo 从订阅消息中提取 topic 和 consumerName
func (p *GmqPipeline) extractSubscriptionInfo(msg any) (topic, consumerName string) {
	// 使用类型断言替代反射
	switch m := msg.(type) {
	case *SubMessage[any]:
		topic = m.QueueName
	case SubMessage[any]:
		topic = m.QueueName
	case interface{ GetQueueName() string }:
		topic = m.GetQueueName()
	case interface{ GetTopic() string }:
		topic = m.GetTopic()
	}

	switch m := msg.(type) {
	case interface{ GetConsumerName() string }:
		consumerName = m.GetConsumerName()
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
	// 管道层统一校验：检查是否已连接
	if atomic.LoadInt32(&p.connected) == 0 {
		return false
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// 先检查连接是否有效（管道层统一校验）
	if !p.plugin.GmqPing(ctx) {
		return false
	}

	start := time.Now()
	latency := time.Since(start).Milliseconds()
	atomic.StoreInt64(&p.lastPingLatency, latency)
	return true
}

// GmqConnect 连接消息队列
func (p *GmqPipeline) GmqConnect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.plugin.GmqConnect(ctx)
	if err == nil {
		atomic.StoreInt32(&p.connected, 1)
		atomic.StoreInt64(&p.connectedAt, time.Now().Unix())
	}
	return err
}

// GmqClose 关闭连接
func (p *GmqPipeline) GmqClose(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}

// GetMetrics 获取统一监控指标（带缓存）
func (p *GmqPipeline) GetMetrics(ctx context.Context) *Metrics {
	now := time.Now().UnixMilli()

	// 检查缓存是否有效
	p.metricsCacheMu.RLock()
	if p.cachedMetrics != nil && now-p.metricsCacheExp < int64(p.metricsCacheTTL.Milliseconds()) {
		// 返回缓存的副本
		m := *p.cachedMetrics
		p.metricsCacheMu.RUnlock()
		return &m
	}
	p.metricsCacheMu.RUnlock()

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
	m := &Metrics{
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

	// 更新缓存
	p.metricsCacheMu.Lock()
	p.cachedMetrics = m
	p.metricsCacheExp = now
	p.metricsCacheMu.Unlock()

	return m
}
