package dto

type BaseMessage struct {
	ServerName string `json:"serverName" validate:"required"` //回调服务名
	MqName     string `json:"mqName" validate:"required"`     //消息队列名
	QueueName  string `json:"queueName" validate:"required"`
}
type PublishReq struct {
	BaseMessage
	Message string `json:"message" validate:"required,min=1"`
}

type SubscribeReq struct {
	BaseMessage
	WebHook string `json:"webHook" validate:"required"` //回调路径，与ServerName拼接构成完整回调地址(例: ServerName="http://service-a", WebHook="/api/callback" -> 回调地址为 "http://service-a/api/callback")
}
