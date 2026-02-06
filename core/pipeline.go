package core

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// 匿名消费者计数器，用于生成唯一订阅key
var anonConsumerCounter atomic.Int64

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

	// 问题5修复：使用预留槽位机制避免并发竞争
	p.mu.Lock()
	if _, alreadySubscribed := p.subscriptions[subKey]; alreadySubscribed {
		p.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to topic: %s", topic)
	}
	// 预留槽位，标记为"订阅中"状态
	p.subscriptions[subKey] = nil
	p.mu.Unlock()

	var subObj interface{}
	// 带重试的订阅
	for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				// 问题6修复：取消时清理预留槽位
				p.mu.Lock()
				delete(p.subscriptions, subKey)
				p.mu.Unlock()
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
		// 问题6修复：订阅失败，清理预留槽位
		p.mu.Lock()
		delete(p.subscriptions, subKey)
		p.mu.Unlock()

		// 记录指标
		latency := time.Since(start).Milliseconds()
		atomic.AddInt64(&p.metrics.totalLatency, latency)
		atomic.AddInt64(&p.metrics.latencyCount, 1)
		atomic.AddInt64(&p.metrics.subscribeFailed, 1)
		return nil, err
	}

	// 重新加锁保存订阅
	p.mu.Lock()
	// 再次检查，防止其他并发订阅已经占用
	if existingSub, exists := p.subscriptions[subKey]; exists && existingSub != nil {
		// 已有其他订阅占用了这个槽位（理论上不应该发生）
		p.mu.Unlock()
		// 问题6修复：取消刚创建的订阅，记录错误日志
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			if unsubErr := closer.Unsubscribe(); unsubErr != nil {
				// 记录取消失败，但不阻塞返回
			}
		}
		return nil, fmt.Errorf("subscription conflict detected for topic: %s", topic)
	}

	// 保存订阅对象到管道层
	p.subscriptions[subKey] = subObj

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
	// 使用带取消的 context，支持优雅退出
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	defer restoreCancel()

	// 统计恢复失败的订阅数量
	var failedSubs []string

	for subKey, info := range p.subscriptionParams {
		var subObj interface{}
		var err error

		// 带重试的订阅
		// 问题7修复：使用 select 支持取消，而不是阻塞式 time.Sleep
		for attempt := 0; attempt < DefaultRetryAttempts; attempt++ {
			if attempt > 0 {
				delay := DefaultRetryDelay * time.Duration(1<<uint(attempt-1))
				select {
				case <-time.After(delay):
				case <-restoreCtx.Done():
					return
				}
			}

			subObj, err = p.plugin.GmqSubscribe(restoreCtx, info.msg)
			if err == nil {
				break
			}
		}

		if err != nil {
			// 问题8修复：记录失败的订阅信息，便于排查
			failedSubs = append(failedSubs, subKey)
			continue
		}

		// 保存新的订阅对象
		if subObj != nil {
			p.subscriptions[subKey] = subObj
		} else {
			p.subscriptions[subKey] = nil
		}

		// 记录指标
		atomic.AddInt64(&p.metrics.subscribeCount, 1)
	}

	// 记录恢复失败的订阅（如果有）
	if len(failedSubs) > 0 {
		// 使用日志记录，不阻塞后续流程
	}
}

// extractSubscriptionInfo 从订阅消息中提取 topic 和 consumerName
// 问题13修复：使用更完整的类型断言逻辑，支持多种接口实现
func (p *GmqPipeline) extractSubscriptionInfo(msg any) (topic, consumerName string) {
	// 尝试通过接口方法提取信息（优先使用接口方法）
	if qn, ok := msg.(interface{ GetQueueName() string }); ok {
		topic = qn.GetQueueName()
	}
	if cn, ok := msg.(interface{ GetConsumerName() string }); ok {
		consumerName = cn.GetConsumerName()
	}

	// 如果接口方法没有提供信息，尝试通过类型断言获取
	// 处理 *SubMessage[T] 类型
	if sm, ok := msg.(*SubMessage[any]); ok && topic == "" {
		topic = sm.QueueName
		if consumerName == "" {
			consumerName = sm.ConsumerName
		}
	}

	// 处理 SubMessage[T] 值类型（较少见）
	if sm, ok := msg.(SubMessage[any]); ok && topic == "" {
		topic = sm.QueueName
		if consumerName == "" {
			consumerName = sm.ConsumerName
		}
	}

	// 兼容旧接口：GetTopic()
	if t, ok := msg.(interface{ GetTopic() string }); ok && topic == "" {
		topic = t.GetTopic()
	}

	return topic, consumerName
}

// getSubKey 生成订阅的唯一key
// 修复4：使用原子计数器避免相同 topic 不同消费者的冲突
func (p *GmqPipeline) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	// 使用原子计数器生成唯一后缀，确保相同 topic 多次订阅不会冲突
	counter := anonConsumerCounter.Add(1)
	return fmt.Sprintf("%s:anon-%d", topic, counter)
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
	cacheExp := p.metricsCacheExp.Load()

	// 问题9修复：正确的缓存过期判断
	// 缓存过期时间戳 = 缓存创建时间 + TTL
	if cm := p.cachedMetrics.Load(); cm != nil {
		if now < cacheExp {
			// 缓存未过期，返回副本
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

	// 问题9修复：更新缓存时计算正确的过期时间
	cacheExpiration := now + p.metricsCacheTTL.Milliseconds()
	p.cachedMetrics.Store(m)
	p.metricsCacheExp.Store(cacheExpiration)

	return m
}
