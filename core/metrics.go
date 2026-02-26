package core

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
	BytesIn          int64                  `json:"bytesIn"`          // 流入字节数
	BytesOut         int64                  `json:"bytesOut"`         // 流出字节数
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

// proxyMetrics 代理监控指标
type proxyMetrics struct {
	messageCount    int64 // 消息总数
	publishCount    int64 // 发布消息数
	subscribeCount  int64 // 订阅消息数
	publishFailed   int64 // 发布失败数
	subscribeFailed int64 // 订阅失败数
	totalLatency    int64 // 总延迟(毫秒)
	latencyCount    int64 // 延迟计数
}

// DeadLetterMsgDTO 死信消息DTO（给前端返回的结构化数据）
type DeadLetterMsgDTO struct {
	MessageID   string                 `json:"message_id"`   // 消息ID
	Body        any                    `json:"body"`         // 消息体
	Headers     map[string]interface{} `json:"headers"`      // 消息头（包含死信原因等信息）
	Timestamp   string                 `json:"timestamp"`    // 消息发布时间
	Exchange    string                 `json:"exchange"`     // 原交换机
	RoutingKey  string                 `json:"routing_key"`  // 原路由键
	DeadReason  string                 `json:"dead_reason"`  // 死信原因（解析自headers）
	QueueName   string                 `json:"queue_name"`   // 死信队列名称
	DeliveryTag uint64                 `json:"delivery_tag"` // 投递标签（用于手动操作）
}
