package core

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjang03/gmq/types"
)

// anonConsumerCounter 匿名消费者计数器，用于生成唯一订阅key
var anonConsumerCounter atomic.Int64

type subMessage struct {
	SubMsg     any
	HandleFunc func(ctx context.Context, message *types.AckMessage) error // 消息处理函数
}

func (m *subMessage) GetSubMsg() any {
	return m.SubMsg
}

func (m *subMessage) GetAckHandleFunc() func(ctx context.Context, message *types.AckMessage) error {
	return m.HandleFunc
}

// GmqProxy 消息队列代理包装器，用于统一监控指标处理
type GmqProxy struct {
	name      string // 代理名称
	plugin    Gmq    // 消息队列插件实例
	connected int32  // 连接状态: 0=未连接, 1=已连接，原子访问

	subscriptions      sync.Map // 订阅管理 - key: subKey, value: subscription object
	subscriptionParams sync.Map // 订阅参数缓存 - key: subKey, value: *subscriptionInfo
}

// newGmqProxy 创建新的代理包装器
func newGmqProxy(name string, plugin Gmq) *GmqProxy {
	p := &GmqProxy{
		name:   name,
		plugin: plugin,
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
func validatePublishMsg(msg types.Publish) error {
	if msg.GetTopic() == "" {
		return fmt.Errorf("topic is required")
	}
	if msg.GetData() == nil {
		return fmt.Errorf("data is required")
	}
	return nil
}

// GmqPublish 发布消息（带统一监控和重试）
func (p *GmqProxy) GmqPublish(ctx context.Context, msg types.Publish) error {
	var err error
	if err = validatePublishMsg(msg); err != nil {
		log.Printf("validate error: %v", err)
		return err
	}
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			// 第一次 ping 失败 或 重试时，执行等待逻辑
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break
			}
			// 第一次延迟用基础值，后续用指数退避
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			log.Printf("attempt %d: ping failed, wait %v", attempt, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				log.Printf("attempt %d: context canceled: %v", attempt, err)
				return err
			}
		}
		// 执行发布
		if err = p.plugin.GmqPublish(ctx, msg); err != nil {
			log.Printf("attempt %d: publish error: %v", attempt, err)
			if attempt == types.MsgRetryDeliver-1 {
				log.Printf("all attempts failed: %v", err)
			}
		} else {
			log.Printf("attempt %d: publish success", attempt)
			return nil
		}
	}
	return err
}

// validatePublishDelayMsg 统一校验延迟发布消息公共参数
func validatePublishDelayMsg(msg types.PublishDelay) error {
	if msg.GetTopic() == "" {
		return fmt.Errorf("topic is required")
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
func (p *GmqProxy) GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error {
	var err error
	if err = validatePublishDelayMsg(msg); err != nil {
		log.Printf("validate error: %v", err)
		return err
	}
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			// 第一次 ping 失败 或 重试时，执行等待逻辑
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break
			}
			// 第一次延迟用基础值，后续用指数退避
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			log.Printf("attempt %d: ping failed, wait %v", attempt, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				log.Printf("attempt %d: context canceled: %v", attempt, err)
				return err
			}
		}
		// 执行发布
		if err = p.plugin.GmqPublishDelay(ctx, msg); err != nil {
			log.Printf("attempt %d: publish error: %v", attempt, err)
			if attempt == types.MsgRetryDeliver-1 {
				log.Printf("all attempts failed: %v", err)
			}
		} else {
			log.Printf("attempt %d: publish success", attempt)
			return nil
		}
	}
	return err
}

// validateSubscribeMsg 统一校验订阅消息公共参数
func validateSubscribeMsg(msg *types.SubMessage) error {
	if msg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if msg.ConsumerName == "" {
		return fmt.Errorf("consumer name is required")
	}
	if msg.FetchCount <= 0 {
		return fmt.Errorf("fetch count must be greater than 0")
	}
	if msg.HandleFunc == nil {
		return fmt.Errorf("handle func is required")
	}
	return nil
}

// wrapHandleFunc 包装用户的 HandleFunc，在代理层统一控制ACK
func (p *GmqProxy) wrapHandleFunc(originalFunc func(ctx context.Context, message any) error, autoAck bool) func(ctx context.Context, message *types.AckMessage) error {
	return func(ctx context.Context, message *types.AckMessage) error {
		if autoAck {
			// 自动确认模式：无论处理成功或失败，都确认消息
			err := p.plugin.GmqAck(ctx, message)
			if err != nil {
				return err
			}
		}
		// 执行用户处理函数
		err := originalFunc(ctx, message.MessageData)
		if !autoAck {
			// 手动确认模式：处理成功则确认，处理失败则终止消息
			if err == nil {
				// 处理成功：确认消息
				err = p.plugin.GmqAck(ctx, message)
				if err != nil {
					return err
				}
			} else {
				// 处理失败：终止消息
				err = p.plugin.GmqNak(ctx, message)
				if err != nil {
					return err
				}
			}
		}
		return err
	}
}

// GmqSubscribe 订阅消息（带统一监控和重试）
func (p *GmqProxy) GmqSubscribe(ctx context.Context, msg types.Subscribe) error {
	var err error
	message, ok := msg.GetSubMsg().(*types.SubMessage)
	if !ok {
		return fmt.Errorf("invalid message type, expected *types.SubMessage")
	}
	// 统一校验公共参数
	if err = validateSubscribeMsg(message); err != nil {
		return err
	}
	// 包装 HandleFunc，在代理层统一控制ACK
	sub := new(subMessage)
	sub.SubMsg = msg
	sub.HandleFunc = p.wrapHandleFunc(message.HandleFunc, message.AutoAck)
	message.HandleFunc = nil
	// 步骤2：生成subKey，检查是否已订阅
	subKey := p.getSubKey(message.Topic, message.ConsumerName)
	// 原子操作：LoadOrStore → 不存在则存入struct{}{}，存在则返回已有值
	_, loaded := p.subscriptions.LoadOrStore(subKey, struct{}{})
	if loaded {
		// 已存在订阅，直接返回
		log.Printf("[GMQ] already subscribed to topic: %s", message.Topic)
		return err
	}
	// 最终清理：订阅失败则删除槽位
	defer func() {
		if err != nil {
			p.subscriptions.Delete(subKey)
			p.subscriptionParams.Delete(subKey)
			log.Printf("[GMQ] subscribe failed, clean slot for subKey: %s, err: %v", subKey, err)
		}
	}()
	// 步骤5：带重试的订阅逻辑
	for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
		// 5.1：Ping检查 + 指数退避等待
		if attempt > 0 || !p.plugin.GmqPing(ctx) {
			if attempt > 0 && p.plugin.GmqPing(ctx) {
				break // ping通，跳过等待
			}
			delay := types.MsgRetryDelay
			if attempt > 0 {
				delay = types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
			}
			log.Printf("[GMQ] subscribe attempt %d: ping failed, wait %v", attempt, delay)
			// 监听上下文取消
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				err = ctx.Err()
				log.Printf("[GMQ] subscribe attempt %d: context canceled: %v", attempt, err)
				return err
			}
		}
		// 5.2：执行订阅（无返回对象，仅判断错误）
		err = p.plugin.GmqSubscribe(ctx, sub)
		if err != nil {
			log.Printf("[GMQ] subscribe attempt %d: error: %v", attempt, err)
			if attempt == types.MsgRetryDeliver-1 {
				log.Printf("[GMQ] subscribe all %d attempts failed: %v", types.MsgRetryDeliver, err)
			}
			continue // 失败则重试
		}
		// 5.3：订阅成功，跳出循环
		log.Printf("[GMQ] subscribe attempt %d: success", attempt)
		break
	}
	// 步骤6：检查最终订阅结果（所有重试都失败则返回）
	if err != nil {
		return err
	}
	// 步骤7：保存订阅参数（用于断线重连）
	p.subscriptionParams.Store(subKey, &sub)
	log.Printf("[GMQ] subscribe success, save subKey: %s", subKey)
	return err
}

// GmqUnsubscribe 取消订阅
func (p *GmqProxy) GmqUnsubscribe(topic, consumerName string) error {
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
func (p *GmqProxy) clearSubscriptions() {
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
func (p *GmqProxy) restoreSubscriptions() {
	// 步骤1：清理旧订阅资源（增加错误日志）
	p.subscriptions.Range(func(key, value any) bool {
		subKey := key.(string)
		subObj := value
		// 取消旧订阅，记录错误
		if closer, ok := subObj.(interface{ Unsubscribe() error }); ok {
			if err := closer.Unsubscribe(); err != nil {
				log.Printf("[GMQ] Failed to unsubscribe old subscription: key=%s, err=%v", subKey, err)
			}
		}
		p.subscriptions.Delete(subKey)
		return true
	})
	// 使用带取消的 context，支持优雅退出
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	defer restoreCancel()
	// 重新订阅
	p.subscriptionParams.Range(func(key, value any) bool {
		subKey := key.(string)
		info, ok := value.(*subMessage)
		if !ok {
			log.Printf("[GMQ] Invalid subMessage info for key=%s", subKey)
			return true
		}
		var err error
		// 带重试+ping检查的订阅
		for attempt := 0; attempt < types.MsgRetryDeliver; attempt++ {
			// 检查上下文是否取消/超时
			if restoreCtx.Err() != nil {
				log.Printf("[GMQ] Restore subscription canceled: key=%s, err=%v", subKey, restoreCtx.Err())
				return false // 终止遍历
			}
			// 重试前ping检查服务端是否可达（首次也检查）
			if attempt > 0 || !p.plugin.GmqPing(restoreCtx) {
				if attempt > 0 && p.plugin.GmqPing(restoreCtx) {
					break // ping通，跳过等待
				}
				// 指数退避等待
				delay := types.MsgRetryDelay * time.Duration(1<<uint(attempt-1))
				log.Printf("[GMQ] Restore subscription attempt %d: ping failed, wait %v (key=%s)", attempt, delay, subKey)
				select {
				case <-time.After(delay):
				case <-restoreCtx.Done():
					return false
				}
			}
			// 执行订阅（无对象版，仅返回错误）
			err = p.plugin.GmqSubscribe(restoreCtx, info)
			if err == nil {
				log.Printf("[GMQ] Restore subscription success: key=%s (attempt=%d)", subKey, attempt)
				break
			}
			// 记录重试失败日志
			log.Printf("[GMQ] Restore subscription attempt %d failed: key=%s, err=%v", attempt, subKey, err)
			if attempt == types.MsgRetryDeliver-1 {
				log.Printf("[GMQ] Restore subscription all attempts failed: key=%s", subKey)
			}
		}
		// 订阅成功：保存状态（用空结构体标记，避免nil）
		if err == nil {
			p.subscriptions.Store(subKey, struct{}{})
		}
		return true
	})
}

// getSubKey 生成订阅的唯一key，使用原子计数器避免相同 topic 不同消费者的冲突
func (p *GmqProxy) getSubKey(topic, consumerName string) string {
	if consumerName != "" {
		return topic + ":" + consumerName
	}
	// 使用原子计数器生成唯一后缀，确保相同 topic 多次订阅不会冲突
	counter := anonConsumerCounter.Add(1)
	return fmt.Sprintf("%s:anon-%d", topic, counter)
}

// GmqPing 检测连接状态
func (p *GmqProxy) GmqPing(ctx context.Context) bool {
	// 代理层统一校验：检查是否已连接
	if atomic.LoadInt32(&p.connected) == 0 {
		return false
	}
	// 检查连接是否有效
	return p.plugin.GmqPing(ctx)
}

// GmqGetConn 获取连接
func (p *GmqProxy) GmqGetConn(ctx context.Context) any {
	return p.plugin.GmqGetConn(ctx)
}

// GmqConnect 连接消息队列
func (p *GmqProxy) GmqConnect(ctx context.Context, cfg map[string]any) error {
	err := p.plugin.GmqConnect(ctx, cfg)
	if err == nil {
		atomic.StoreInt32(&p.connected, 1)
	}
	return err
}

func (p *GmqProxy) GmqAck(ctx context.Context, msg *types.AckMessage) error {
	return p.plugin.GmqAck(ctx, msg)
}

func (p *GmqProxy) GmqNak(ctx context.Context, msg *types.AckMessage) error {
	return p.plugin.GmqNak(ctx, msg)
}

// GmqClose 关闭连接
func (p *GmqProxy) GmqClose(ctx context.Context) error {
	// 关闭前先清理所有订阅
	p.clearSubscriptions()

	err := p.plugin.GmqClose(ctx)
	atomic.StoreInt32(&p.connected, 0)
	return err
}
