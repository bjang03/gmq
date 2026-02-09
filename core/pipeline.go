package core

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// anonConsumerCounter 匿名消费者计数器，用于生成唯一订阅key
var anonConsumerCounter atomic.Int64

// subscriptionInfo 订阅信息，用于断线重连后恢复订阅
type subscriptionInfo struct {
	msg any
}

// GmqPipeline 消息队列管道包装器，用于统一监控指标处理
type GmqPipeline struct {
	name        string          // 管道名称
	plugin      Gmq             // 消息队列插件实例
	metrics     pipelineMetrics // 管道监控指标
	connectedAt int64           // 连接时间(Unix时间戳，秒)，原子访问
	connected   int32           // 连接状态: 0=未连接, 1=已连接，原子访问

	subscriptions      sync.Map // 订阅管理 - key: subKey, value: subscription object
	subscriptionParams sync.Map // 订阅参数缓存 - key: subKey, value: *subscriptionInfo

	cachedMetrics   atomic.Pointer[Metrics] // 指标缓存 - 原子指针，无锁读取
	metricsCacheExp atomic.Int64            // 指标缓存过期时间戳 - 原子操作
	metricsCacheTTL time.Duration           // 指标缓存TTL
}

// newGmqPipeline 创建新的管道包装器
func newGmqPipeline(name string, plugin Gmq) *GmqPipeline {
	p := &GmqPipeline{
		name:            name,
		plugin:          plugin,
		metricsCacheTTL: 5 * time.Second,
	}
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
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := MsgRetryDelay * time.Duration(1<<uint(attempt-1))
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

// GmqPublishDelay 发布延迟消息（带统一监控和重试）
func (p *GmqPipeline) GmqPublishDelay(ctx context.Context, msg PublishDelay) error {
	// 只检查连接状态，不持有锁进行网络操作
	if atomic.LoadInt32(&p.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	start := time.Now()
	var err error

	// 带重试的发布（无锁，网络操作）
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				break
			}
		}

		err = p.plugin.GmqPublishDelay(ctx, msg)
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
func (p *GmqPipeline) GmqSubscribe(ctx context.Context, msg any) (result interface{}, err error) {
	start := time.Now()

	// 提取 topic 和 consumerName（从 msg 中解析）
	topic, consumerName := p.extractSubscriptionInfo(msg)
	subKey := p.getSubKey(topic, consumerName)

	// 检查是否已订阅
	if _, alreadySubscribed := p.subscriptions.Load(subKey); alreadySubscribed {
		return nil, fmt.Errorf("already subscribed to topic: %s", topic)
	}
	// 预留槽位，标记为"订阅中"状态
	p.subscriptions.Store(subKey, nil)

	var subObj interface{}
	// 带重试的订阅
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			// 重试前等待，使用指数退避
			delay := MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				p.subscriptions.Delete(subKey)
				return nil, ctx.Err()
			}
		}

		err = p.plugin.GmqSubscribe(ctx, msg)
		if err == nil {
			break
		}
	}

	if err != nil {
		p.subscriptions.Delete(subKey)

		// 记录指标
		latency := time.Since(start).Milliseconds()
		atomic.AddInt64(&p.metrics.totalLatency, latency)
		atomic.AddInt64(&p.metrics.latencyCount, 1)
		atomic.AddInt64(&p.metrics.subscribeFailed, 1)
		return nil, err
	}

	if existingSub, exists := p.subscriptions.Load(subKey); exists && existingSub != nil {
		// 取消刚创建的订阅，记录错误日志
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			if unsubErr := closer.Unsubscribe(); unsubErr != nil {
				log.Printf("[GMQ] Failed to unsubscribe after conflict: %v", unsubErr)
			}
		}
		return nil, fmt.Errorf("subscription conflict detected for topic: %s", topic)
	}

	// 保存订阅对象到管道层
	p.subscriptions.Store(subKey, subObj)

	// 保存订阅参数，用于断线重连后恢复订阅
	p.subscriptionParams.Store(subKey, &subscriptionInfo{
		msg: msg,
	})

	// 记录指标
	latency := time.Since(start).Milliseconds()
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)
	atomic.AddInt64(&p.metrics.subscribeCount, 1)

	return subObj, nil
}

// GmqUnsubscribe 取消订阅
func (p *GmqPipeline) GmqUnsubscribe(topic, consumerName string) error {
	subKey := p.getSubKey(topic, consumerName)

	subObj, exists := p.subscriptions.Load(subKey)
	if !exists {
		return fmt.Errorf("subscription not found: %s", subKey)
	}

	// 如果订阅对象实现了关闭接口，调用关闭
	if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
		if err := closer.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
	}

	p.subscriptions.Delete(subKey)
	p.subscriptionParams.Delete(subKey) // 同时删除订阅参数缓存
	return nil
}

// clearSubscriptions 清理所有订阅
func (p *GmqPipeline) clearSubscriptions() {
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		subObj := value
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		p.subscriptions.Delete(subKey)
		p.subscriptionParams.Delete(subKey)
		return true
	})
}

// restoreSubscriptions 断线重连后恢复所有订阅
func (p *GmqPipeline) restoreSubscriptions() {
	// 先清理旧的订阅对象
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		subObj := value
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			_ = closer.Unsubscribe()
		}
		p.subscriptions.Delete(subKey)
		return true
	})

	// 使用带取消的 context，支持优雅退出
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	defer restoreCancel()

	// 统计恢复失败的订阅数量
	var failedSubs []string

	// 重新订阅
	p.subscriptionParams.Range(func(key, value any) bool {
		subKey := key.(string)
		info := value.(*subscriptionInfo)
		var subObj interface{}
		var err error

		// 带重试的订阅
		for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
			if attempt > 0 {
				delay := MsgRetryDelay * time.Duration(1<<uint(attempt-1))
				select {
				case <-time.After(delay):
				case <-restoreCtx.Done():
					return false
				}
			}

			err = p.plugin.GmqSubscribe(restoreCtx, info.msg)
			if err == nil {
				break
			}
		}

		if err != nil {
			failedSubs = append(failedSubs, subKey)
			return true
		}

		// 保存新的订阅对象
		if subObj != nil {
			p.subscriptions.Store(subKey, subObj)
		} else {
			p.subscriptions.Store(subKey, nil)
		}

		// 记录指标
		atomic.AddInt64(&p.metrics.subscribeCount, 1)
		return true
	})

	// 记录恢复失败的订阅（如果有）
	if len(failedSubs) > 0 {
		log.Printf("[GMQ] Failed to restore %d subscriptions after reconnection", len(failedSubs))
		for _, subKey := range failedSubs {
			if info, exists := p.subscriptionParams.Load(subKey); exists {
				if qn, ok := info.(*subscriptionInfo).msg.(interface{ GetQueueName() string }); ok {
					log.Printf("[GMQ] Failed to restore subscription: queue=%s, key=%s", qn.GetQueueName(), subKey)
				} else {
					log.Printf("[GMQ] Failed to restore subscription: key=%s", subKey)
				}
			}
		}
	}
}

// extractSubscriptionInfo 从订阅消息中提取 topic 和 consumerName
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

// getSubKey 生成订阅的唯一key，使用原子计数器避免相同 topic 不同消费者的冲突
func (p *GmqPipeline) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	// 使用原子计数器生成唯一后缀，确保相同 topic 多次订阅不会冲突
	counter := anonConsumerCounter.Add(1)
	return fmt.Sprintf("%s:anon-%d", topic, counter)
}

// GmqGetDeadLetter 获取死信消息
func (p *GmqPipeline) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) ([]DeadLetterMsgDTO, error) {
	return p.plugin.GmqGetDeadLetter(queueName, limit)
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

	err := p.plugin.GmqConnect(ctx)
	if err == nil {
		atomic.StoreInt32(&p.connected, 1)
		atomic.StoreInt64(&p.connectedAt, time.Now().Unix())
	}
	return err
}

// GmqClose 关闭连接
func (p *GmqPipeline) GmqClose(ctx context.Context) error {
	// 关闭前先清理所有订阅
	p.clearSubscriptions()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}

// GetMetrics 获取统一监控指标（带缓存，使用 atomic 无锁读取）
func (p *GmqPipeline) GetMetrics(ctx context.Context) *Metrics {
	now := time.Now().UnixMilli()
	cacheExp := p.metricsCacheExp.Load()

	// 缓存过期判断：缓存过期时间戳 = 缓存创建时间 + TTL
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

	// 确定连接状态
	status := "disconnected"
	if atomic.LoadInt32(&p.connected) == 1 {
		status = "connected"
	}

	// 合并指标（插件提供基础信息，管道层提供客户端统计和连接信息）
	// 深拷贝 map 字段避免数据竞争
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

	// 更新缓存时计算正确的过期时间
	cacheExpiration := now + p.metricsCacheTTL.Milliseconds()
	p.cachedMetrics.Store(m)
	p.metricsCacheExp.Store(cacheExpiration)

	return m
}
