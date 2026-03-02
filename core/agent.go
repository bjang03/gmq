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

// GmqAgent 消息队列代理包装器，用于统一监控指标处理
type GmqAgent struct {
	name        string       // 代理名称
	plugin      Gmq          // 消息队列插件实例
	metrics     agentMetrics // 代理监控指标
	connectedAt int64        // 连接时间(Unix时间戳，秒)，原子访问
	connected   int32        // 连接状态: 0=未连接, 1=已连接，原子访问

	subscriptions      sync.Map // 订阅管理 - key: subKey, value: subscription object
	subscriptionParams sync.Map // 订阅参数缓存 - key: subKey, value: *subscriptionInfo

	cachedMetrics   atomic.Pointer[Metrics] // 指标缓存 - 原子指针，无锁读取
	metricsCacheExp atomic.Int64            // 指标缓存过期时间戳 - 原子操作
	metricsCacheTTL time.Duration           // 指标缓存TTL
}

// newGmqAgent 创建新的代理包装器
func newGmqAgent(name string, plugin Gmq) *GmqAgent {
	p := &GmqAgent{
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

// validatePublishMsg 统一校验发布消息公共参数
func validatePublishMsg(msg Publish) error {
	if msg.GetQueueName() == "" {
		return fmt.Errorf("queue name is required")
	}
	if msg.GetData() == nil {
		return fmt.Errorf("data is required")
	}
	return nil
}

// GmqPublish 发布消息（带统一监控和重试）
func (p *GmqAgent) GmqPublish(ctx context.Context, msg Publish) error {
	// 统一校验公共参数
	if err := validatePublishMsg(msg); err != nil {
		return err
	}

	// 只检查连接状态，不持有锁进行网络操作
	if atomic.LoadInt32(&p.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	start := time.Now()
	var err error

	// 带重试的发布（无锁，网络操作）
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			if p.plugin.GmqPing(ctx) {
				break
			}
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

// validatePublishDelayMsg 统一校验延迟发布消息公共参数
func validatePublishDelayMsg(msg PublishDelay) error {
	if msg.GetQueueName() == "" {
		return fmt.Errorf("queue name is required")
	}
	if msg.GetData() == nil {
		return fmt.Errorf("data is required")
	}
	if msg.GetDelaySeconds() <= 0 {
		return fmt.Errorf("delay seconds must be greater than 0")
	}
	return nil
}

// GmqPublishDelay 发布延迟消息（带统一监控和重试）
func (p *GmqAgent) GmqPublishDelay(ctx context.Context, msg PublishDelay) error {
	// 统一校验公共参数
	if err := validatePublishDelayMsg(msg); err != nil {
		return err
	}

	// 只检查连接状态，不持有锁进行网络操作
	if atomic.LoadInt32(&p.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	start := time.Now()
	var err error

	// 带重试的发布（无锁，网络操作）
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			if p.plugin.GmqPing(ctx) {
				break
			}
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

// validateSubscribeMsg 统一校验订阅消息公共参数
func validateSubscribeMsg(msg Subscribe) error {
	if msg.GetQueueName() == "" {
		return fmt.Errorf("queue name is required")
	}
	if msg.GetConsumerName() == "" {
		return fmt.Errorf("consumer name is required")
	}
	if msg.GetFetchCount() <= 0 {
		return fmt.Errorf("fetch count must be greater than 0")
	}
	if msg.GetHandleFunc() == nil {
		return fmt.Errorf("handle func is required")
	}
	return nil
}

// wrapHandleFunc 包装用户的 HandleFunc，在代理层统一控制ACK
func (p *GmqAgent) wrapHandleFunc(originalFunc func(ctx context.Context, message any) error, autoAck bool) func(ctx context.Context, message any) error {
	return func(ctx context.Context, message any) error {
		ackMessage, ok := message.(*AckMessage)
		if !ok {
			return fmt.Errorf("invalid message type: must be *AckMessage")
		}
		// 执行用户处理函数
		err := originalFunc(ctx, ackMessage.MessageData)
		if autoAck {
			// 自动确认模式：无论处理成功或失败，都确认消息
			err = p.plugin.Ack(ackMessage)
			if err != nil {
				return err
			}
		} else {
			// 手动确认模式：处理成功则确认，处理失败则终止消息
			if err == nil {
				// 处理成功：确认消息
				err = p.plugin.Nak(ackMessage)
				if err != nil {
					return err
				}
			} else {
				// 处理失败：终止消息
				err = p.plugin.Nak(ackMessage)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// GmqSubscribe 订阅消息（带统一监控和重试）
func (p *GmqAgent) GmqSubscribe(ctx context.Context, msg any) (err error) {
	start := time.Now()

	// 统一校验公共参数（先类型断言为 Subscribe 接口）
	subMsg, ok := msg.(Subscribe)
	if !ok {
		return fmt.Errorf("invalid message type: must implement Subscribe interface (GetQueueName, GetConsumerName, GetAutoAck, GetFetchCount)")
	}
	if err := validateSubscribeMsg(subMsg); err != nil {
		return err
	}
	subKey := p.getSubKey(subMsg.GetQueueName(), subMsg.GetConsumerName())

	// 检查是否已订阅
	if _, alreadySubscribed := p.subscriptions.Load(subKey); alreadySubscribed {
		return fmt.Errorf("already subscribed to topic: %s", subMsg.GetQueueName())
	}
	// 预留槽位，标记为"订阅中"状态
	p.subscriptions.Store(subKey, nil)

	// 包装 HandleFunc，在代理层统一控制ACK
	subMsg.SetHandleFunc(p.wrapHandleFunc(subMsg.GetHandleFunc(), subMsg.GetAutoAck()))
	var subObj interface{}
	// 带重试的订阅
	for attempt := 0; attempt < MsgRetryDeliver; attempt++ {
		if attempt > 0 {
			if p.plugin.GmqPing(ctx) {
				break
			}
			// 重试前等待，使用指数退避
			delay := MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				p.subscriptions.Delete(subKey)
				return ctx.Err()
			}
		}
		err = p.plugin.GmqSubscribe(ctx, subMsg)
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
		return err
	}

	if existingSub, exists := p.subscriptions.Load(subKey); exists && existingSub != nil {
		// 取消刚创建的订阅，记录错误日志
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			if unsubErr := closer.Unsubscribe(); unsubErr != nil {
				log.Printf("[GMQ] Failed to unsubscribe after conflict: %v", unsubErr)
			}
		}
		return fmt.Errorf("subscription conflict detected for topic: %s", subMsg.GetQueueName())
	}

	// 保存订阅对象到代理层
	p.subscriptions.Store(subKey, subObj)

	// 保存订阅参数，用于断线重连后恢复订阅
	p.subscriptionParams.Store(subKey, &subscriptionInfo{
		msg: subMsg,
	})

	// 记录指标
	latency := time.Since(start).Milliseconds()
	atomic.AddInt64(&p.metrics.totalLatency, latency)
	atomic.AddInt64(&p.metrics.latencyCount, 1)
	atomic.AddInt64(&p.metrics.subscribeCount, 1)

	return nil
}

// GmqUnsubscribe 取消订阅
func (p *GmqAgent) GmqUnsubscribe(topic, consumerName string) error {
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
func (p *GmqAgent) clearSubscriptions() {
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
func (p *GmqAgent) restoreSubscriptions() {
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

// getSubKey 生成订阅的唯一key，使用原子计数器避免相同 topic 不同消费者的冲突
func (p *GmqAgent) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	// 使用原子计数器生成唯一后缀，确保相同 topic 多次订阅不会冲突
	counter := anonConsumerCounter.Add(1)
	return fmt.Sprintf("%s:anon-%d", topic, counter)
}

// GmqGetDeadLetter 获取死信消息
func (p *GmqAgent) GmqGetDeadLetter(ctx context.Context, queueName string, limit int) ([]DeadLetterMsgDTO, error) {
	return p.plugin.GmqGetDeadLetter(ctx, queueName, limit)
}

// GmqPing 检测连接状态
func (p *GmqAgent) GmqPing(ctx context.Context) bool {
	// 代理层统一校验：检查是否已连接
	if atomic.LoadInt32(&p.connected) == 0 {
		return false
	}

	// 检查连接是否有效
	return p.plugin.GmqPing(ctx)
}

// GmqConnect 连接消息队列
func (p *GmqAgent) GmqConnect(ctx context.Context) error {

	err := p.plugin.GmqConnect(ctx)
	if err == nil {
		atomic.StoreInt32(&p.connected, 1)
		atomic.StoreInt64(&p.connectedAt, time.Now().Unix())
	}
	return err
}

func (p *GmqAgent) Ack(msg *AckMessage) error {
	return p.plugin.Ack(msg)
}

func (p *GmqAgent) Nak(msg *AckMessage) error {
	return p.plugin.Nak(msg)
}

// GmqClose 关闭连接
func (p *GmqAgent) GmqClose(ctx context.Context) error {
	// 关闭前先清理所有订阅
	p.clearSubscriptions()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}

// GetMetrics 获取统一监控指标（带缓存，使用 atomic 无锁读取）
func (p *GmqAgent) GetMetrics(ctx context.Context) *Metrics {
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

	// 获取代理层面的指标
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

	// 使用代理层的连接时间（原子读取）
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

	// 合并指标（插件提供基础信息，代理层提供客户端统计和连接信息）
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
