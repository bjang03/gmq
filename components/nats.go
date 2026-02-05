package components

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/nats-io/nats.go"
)

func init() {
	core.GmqRegister("nats", &natsMsg{})
}

// NatsPubMessage NATS发布消息结构，支持延迟消息
type NatsPubMessage struct {
	core.PubMessage
	DelaySeconds int // 延迟时间(秒)
}

func (n NatsPubMessage) GetGmqPublishMsgType() {
	panic("implement me")
}

// NatsSubMessage NATS订阅消息结构，支持持久化订阅和延迟消费
type NatsSubMessage struct {
	core.SubMessage[any]
	Durable      bool   // 是否持久化订阅
	ConsumerName string // 消费者名称
}

// natsMsg NATS消息队列实现
type natsMsg struct {
	conn        *nats.Conn
	connURL     string
	monitorURL  string
	connectedAt time.Time
	collector   *core.MetricsCollector // 通用指标收集器

	// 客户端本地累加指标（服务端无法提供的）
	publishCount    int64
	subscribeCount  int64
	publishFailed   int64
	subscribeFailed int64
}

// natsServerVarz NATS服务器varz响应结构
type natsServerVarz struct {
	ServerID      string            `json:"server_id"`
	Version       string            `json:"version"`
	Start         string            `json:"start"`
	Time          string            `json:"time"`
	Uptime        string            `json:"uptime"`
	Mem           int64             `json:"mem"`
	Cores         int               `json:"cores"`
	CPU           float64           `json:"cpu"`
	Connections   int               `json:"connections"`
	TotalCons     int               `json:"total_connections"`
	Routes        int               `json:"routes"`
	Remotes       int               `json:"remotes"`
	SlowCons      int               `json:"slow_consumers"`
	Subscriptions int               `json:"subscriptions"`
	HTTPReqStats  map[string]uint64 `json:"http_req_stats"`
	InMsgs        int64             `json:"in_msgs"`
	OutMsgs       int64             `json:"out_msgs"`
	InBytes       int64             `json:"in_bytes"`
	OutBytes      int64             `json:"out_bytes"`
}

// GmqPing 检测NATS连接状态
func (c *natsMsg) GmqPing(_ context.Context) bool {
	if c.conn == nil {
		return false
	}

	if !c.conn.IsConnected() {
		return false
	}

	// 记录ping延迟
	start := time.Now()
	_ = c.conn.Flush()
	latency := time.Since(start).Milliseconds()
	if c.collector != nil {
		c.collector.RecordPingLatency(latency)
	}

	return true
}

// GmqConnect 连接NATS服务器
func (c *natsMsg) GmqConnect(ctx context.Context) (err error) {
	c.connURL = "nats://localhost:4222"
	c.monitorURL = "http://localhost:8222" // NATS监控接口默认端口

	// 初始化指标收集器
	if c.collector == nil {
		c.collector = core.NewMetricsCollector()
	}

	// 配置连接选项
	opts := []nats.Option{
		nats.ReconnectWait(time.Second * 2),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			fmt.Printf("NATS disconnected: %v\n", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			if c.collector != nil {
				c.collector.RecordReconnect()
			}
			fmt.Printf("NATS reconnected to %s\n", nc.ConnectedUrl())
		}),
	}

	c.conn, err = nats.Connect(c.connURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	c.connectedAt = time.Now()
	return nil
}

// GmqClose 关闭NATS连接
func (c *natsMsg) GmqClose(ctx context.Context) (err error) {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	return nil
}

// GmqPublish 发布NATS消息
func (c *natsMsg) GmqPublish(ctx context.Context, msg core.Publish) (err error) {
	if c.conn == nil {
		c.publishFailed++
		return fmt.Errorf("nats connection not established")
	}

	start := time.Now()

	natsMsg, ok := msg.(*NatsPubMessage)
	if !ok {
		c.publishFailed++
		return fmt.Errorf("invalid message type, expected *NatsPubMessage")
	}

	err = c.conn.Publish(natsMsg.QueueName, []byte(fmt.Sprintf("%v", natsMsg.Data)))

	// 记录延迟
	latency := time.Since(start).Milliseconds()
	if c.collector != nil {
		c.collector.RecordLatency(latency)
	}

	if err != nil {
		c.publishFailed++
		return fmt.Errorf("failed to publish message: %w", err)
	}

	c.publishCount++
	return nil
}

// GmqSubscribe 订阅NATS消息
func (c *natsMsg) GmqSubscribe(ctx context.Context, msg any) (err error) {
	if c.conn == nil {
		c.subscribeFailed++
		return fmt.Errorf("nats connection not established")
	}

	start := time.Now()

	natsMsg, ok := msg.(*NatsSubMessage)
	if !ok {
		c.subscribeFailed++
		return fmt.Errorf("invalid message type, expected *NatsSubMessage")
	}

	if natsMsg.Durable && natsMsg.ConsumerName != "" {
		_, err = c.conn.QueueSubscribe(natsMsg.QueueName, natsMsg.ConsumerName, func(m *nats.Msg) {
			if natsMsg.HandleFunc != nil {
				_ = natsMsg.HandleFunc(ctx, m.Data)
			}
		})
	} else {
		_, err = c.conn.Subscribe(natsMsg.QueueName, func(m *nats.Msg) {
			if natsMsg.HandleFunc != nil {
				_ = natsMsg.HandleFunc(ctx, m.Data)
			}
		})
	}

	// 记录延迟
	latency := time.Since(start).Milliseconds()
	if c.collector != nil {
		c.collector.RecordLatency(latency)
	}

	if err != nil {
		c.subscribeFailed++
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	c.subscribeCount++
	return nil
}

// getServerVarz 从NATS监控接口获取服务器指标
func (c *natsMsg) getServerVarz() (*natsServerVarz, error) {
	if c.monitorURL == "" {
		return nil, fmt.Errorf("monitor URL not configured")
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(c.monitorURL + "/varz")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("monitor API returned status: %d", resp.StatusCode)
	}

	var varz natsServerVarz
	if err := json.NewDecoder(resp.Body).Decode(&varz); err != nil {
		return nil, err
	}

	return &varz, nil
}

// GetMetrics 获取监控指标
func (c *natsMsg) GetMetrics(ctx context.Context) *core.Metrics {
	m := &core.Metrics{
		Name:       "nats",
		Type:       "nats",
		ServerAddr: c.connURL,
		ConnectedAt: func() string {
			if c.connectedAt.IsZero() {
				return "-"
			}
			return c.connectedAt.Format("2006-01-02 15:04:05")
		}(),
	}

	// 连接状态
	if c.GmqPing(ctx) {
		m.Status = "connected"
	} else {
		m.Status = "disconnected"
	}

	// 运行时间
	if !c.connectedAt.IsZero() {
		m.UptimeSeconds = int64(time.Since(c.connectedAt).Seconds())
	}

	// 从NATS服务器获取指标
	varz, err := c.getServerVarz()
	if err == nil && varz != nil {
		// 服务端指标 - 直接从varz获取
		m.MessageCount = varz.InMsgs + varz.OutMsgs // 服务端总消息数
		m.MsgsIn = varz.InMsgs
		m.MsgsOut = varz.OutMsgs
		m.BytesIn = varz.InBytes
		m.BytesOut = varz.OutBytes

		// 服务端详细信息
		m.ServerMetrics = core.ServerMetrics{
			ServerID:          varz.ServerID,
			ServerVersion:     varz.Version,
			ServerTime:        varz.Time,
			TotalConnections:  int64(varz.TotalCons),
			ActiveConnections: int64(varz.Connections),
			SlowConsumers:     int64(varz.SlowCons),
			MemoryUsed:        varz.Mem,
			CPUUsage:          varz.CPU,
			TotalConsumers:    int64(varz.Subscriptions),
			BytesIn:           varz.InBytes,
			BytesOut:          varz.OutBytes,
			MsgsIn:            varz.InMsgs,
			MsgsOut:           varz.OutMsgs,
		}

		// NATS特有的扩展指标
		m.Extensions = map[string]any{
			"monitorUrl":   c.monitorURL,
			"serverUptime": varz.Uptime,
			"cores":        varz.Cores,
			"routes":       varz.Routes,
			"remotes":      varz.Remotes,
			"httpReqStats": varz.HTTPReqStats,
		}
	}

	// 客户端本地累加指标
	m.PublishCount = c.publishCount
	m.SubscribeCount = c.subscribeCount
	m.PublishFailed = c.publishFailed
	m.SubscribeFailed = c.subscribeFailed

	// 连接统计
	if c.conn != nil {
		stats := c.conn.Stats()
		m.PendingAckCount = int64(stats.OutMsgs - stats.InMsgs)
	}

	// 延迟指标 - 从collector获取（客户端本地测量）
	if c.collector != nil {
		avg, max, min, lastPing, _ := c.collector.GetLatencyStats()
		m.AverageLatency = avg
		m.MaxLatency = max
		m.MinLatency = min
		m.LastPingLatency = lastPing
		m.ReconnectCount = c.collector.GetReconnectCount()
	}

	// 计算吞吐量（基于客户端运行时间）
	if m.UptimeSeconds > 0 {
		totalClientMsgs := m.PublishCount + m.SubscribeCount
		m.ThroughputPerSec = float64(totalClientMsgs) / float64(m.UptimeSeconds)
		m.PublishPerSec = float64(m.PublishCount) / float64(m.UptimeSeconds)
		m.SubscribePerSec = float64(m.SubscribeCount) / float64(m.UptimeSeconds)
	}

	// 计算错误率
	totalOps := m.PublishCount + m.SubscribeCount + m.PublishFailed + m.SubscribeFailed
	if totalOps > 0 {
		m.ErrorRate = float64(m.PublishFailed+m.SubscribeFailed) / float64(totalOps) * 100
	}

	return m
}
