package core

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// subscriptionInfo 订阅信息，用于断线重连后恢复订阅
// P0修复：不保存 context，因为 context 可能会过期
type subscriptionInfo struct {
	msg any
}

// GmqPipeline 消息队列管道包装器，用于统一监控指标处理
type GmqPipeline struct {
	name        string
	plugin      Gmq
	metrics     pipelineMetrics
	connectedAt int64 // Unix timestamp in seconds, atomic access
	connected   int32 // 连接状态: 0=未连接, 1=已连接, atomic access

	// 并发控制 - 统一管理所有并发访问
	mu sync.RWMutex

	// 订阅管理 - 统一管理所有订阅对象（包括状态和具体的订阅引用）
	subscriptions map[string]interface{} // key 为 topic:consumerName，value 为具体的订阅对象

	// 订阅参数缓存 - 用于断线重连后恢复订阅
	subscriptionParams map[string]*subscriptionInfo

	// 指标缓存（使用 atomic 替代锁，提升并发读性能）
	cachedMetrics   atomic.Pointer[Metrics] // 原子指针，无锁读取
	metricsCacheExp atomic.Int64            // 原子过期时间戳
	metricsCacheTTL time.Duration
}

// newGmqPipeline 创建新的管道包装器
func newGmqPipeline(name string, plugin Gmq) *GmqPipeline {
	p := &GmqPipeline{
		name:               name,
		plugin:             plugin,
		subscriptions:      make(map[string]interface{}),
		subscriptionParams: make(map[string]*subscriptionInfo),
		metricsCacheTTL:    5 * time.Second,
	}
	// atomic 字段零值即可使用，无需显式初始化
	return p
}

// safeCloneMap 安全地克隆 map，处理 nil 情况
func safeCloneMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	return maps.Clone(m)
}

// GmqPublish 发布消息（带统一监控和重试）
func (p *GmqPipeline) GmqPublish(ctx context.Context, msg Publish) error {
	// 只检查连接状态，不持有锁进行网络操作
	if atomic.LoadInt32(&p.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	start := time.Now()
	var err error

	// 带重试的发布（无锁，网络操作）
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

	// 记录指标（原子操作，无需锁）
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

	// 使用写锁保证检查和订阅的原子性
	p.mu.Lock()

	if _, alreadySubscribed := p.subscriptions[subKey]; alreadySubscribed {
		p.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to topic: %s", topic)
	}
	p.mu.Unlock() // 解锁后进行网络操作

	var subObj interface{}
	// 带重试的订阅
	for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		subObj, err = p.plugin.GmqSubscribe(ctx, msg)
		if err == nil {
			break
		}
	}

	// 检查订阅对象是否有效
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

	// 重新加锁保存订阅
	p.mu.Lock()
	// 再次检查，防止并发订阅同一 topic
	if _, alreadySubscribed := p.subscriptions[subKey]; alreadySubscribed {
		p.mu.Unlock()
		// 取消刚创建的订阅
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		return nil, fmt.Errorf("already subscribed to topic: %s", topic)
	}

	// 保存订阅对象到管道层
	if subObj != nil {
		p.subscriptions[subKey] = subObj
	} else {
		p.subscriptions[subKey] = true
	}

	// 保存订阅参数，用于断线重连后恢复订阅
	p.subscriptionParams[subKey] = &subscriptionInfo{
		msg: msg,
	}
	p.mu.Unlock()

	// 记录指标
	latency := time.Since(start).Milliseconds()
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)
	atomic.AddInt64(&p.metrics.subscribeCount, 1)

	return subObj, nil
}

// GmqUnsubscribe 取消订阅（问题7修复：添加取消订阅接口）
func (p *GmqPipeline) GmqUnsubscribe(topic, consumerName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	subKey := p.getSubKey(topic, consumerName)
	subObj, exists := p.subscriptions[subKey]
	if !exists {
		return fmt.Errorf("subscription not found: %s", subKey)
	}

	// 如果订阅对象实现了关闭接口，调用关闭
	if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
		if err := closer.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
	}

	delete(p.subscriptions, subKey)
	delete(p.subscriptionParams, subKey) // 同时删除订阅参数缓存
	return nil
}

// clearSubscriptions 清理所有订阅（关闭时清理）- 需要在持有锁的情况下调用
func (p *GmqPipeline) clearSubscriptions() {
	for subKey, subObj := range p.subscriptions {
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		delete(p.subscriptions, subKey)
		delete(p.subscriptionParams, subKey)
	}
}

// restoreSubscriptions 断线重连后恢复所有订阅
func (p *GmqPipeline) restoreSubscriptions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 先清理旧的订阅对象
	for subKey, subObj := range p.subscriptions {
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		delete(p.subscriptions, subKey)
	}

	// 重新订阅
	// P0修复：使用 context.Background() 替代可能已过期的 ctx
	restoreCtx := context.Background()
	for subKey, info := range p.subscriptionParams {
		var subObj interface{}
		var err error

		// 带重试的订阅
		for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
			if attempt > 0 {
				delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
				time.Sleep(delay)
			}

			subObj, err = p.plugin.GmqSubscribe(restoreCtx, info.msg)
			if err == nil {
				break
			}
		}

		if err != nil {
			// 订阅失败，记录错误但继续尝试其他订阅
			continue
		}

		// 保存新的订阅对象
		if subObj != nil {
			p.subscriptions[subKey] = subObj
		} else {
			p.subscriptions[subKey] = true
		}

		// 记录指标
		atomic.AddInt64(&p.metrics.subscribeCount, 1)
	}
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

	// 检查连接是否有效
	return p.plugin.GmqPing(ctx)
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

	// 高危BUG修复：关闭前先清理所有订阅
	p.clearSubscriptions()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}

// GetMetrics 获取统一监控指标（带缓存，使用 atomic 无锁读取）
func (p *GmqPipeline) GetMetrics(ctx context.Context) *Metrics {
	now := time.Now().UnixMilli()

	// 检查缓存是否有效（无锁读取）
	if cm := p.cachedMetrics.Load(); cm != nil {
		if now-p.metricsCacheExp.Load() < p.metricsCacheTTL.Milliseconds() {
			// 返回缓存的副本
			m := *cm
			return &m
		}
	}

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

	// 确定连接状态（问题19修复）
	status := "disconnected"
	if atomic.LoadInt32(&p.connected) == 1 {
		status = "connected"
	}

	// 合并指标（插件提供基础信息，管道层提供客户端统计和连接信息）
	// 深拷贝 map 字段避免数据竞争（问题11修复）
	m := &Metrics{
		Name:            p.name,
		Type:            pluginMetrics.Type,
		Status:          status,
		ServerAddr:      pluginMetrics.ServerAddr,
		ConnectedAt:     connectedAtStr,
		UptimeSeconds:   uptimeSeconds,
		MessageCount:    messageCount,
		PublishCount:    publishCount,
		SubscribeCount:  subscribeCount,
		PendingMessages: pluginMetrics.PendingMessages,
		PendingAckCount: pluginMetrics.PendingAckCount,
		PublishFailed:   publishFailed,
		SubscribeFailed: subscribeFailed,
		MsgsIn:          pluginMetrics.MsgsIn,
		MsgsOut:         pluginMetrics.MsgsOut,
		BytesIn:         pluginMetrics.BytesIn,
		BytesOut:        pluginMetrics.BytesOut,
		AverageLatency:  avgLatency,

		MaxLatency:       pluginMetrics.MaxLatency,
		MinLatency:       pluginMetrics.MinLatency,
		ThroughputPerSec: throughputPerSec,
		PublishPerSec:    publishPerSec,
		SubscribePerSec:  subscribePerSec,
		ErrorRate:        errorRate,
		ReconnectCount:   pluginMetrics.ReconnectCount,
		ServerMetrics:    safeCloneMap(pluginMetrics.ServerMetrics),
		Extensions:       safeCloneMap(pluginMetrics.Extensions),
	}

	// 更新缓存（原子操作，无锁）
	p.cachedMetrics.Store(m)
	p.metricsCacheExp.Store(now)

	return m
}
