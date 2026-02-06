package dto

type PublishReq struct {
	ServerName string `json:"serverName"`
	QueueName  string `json:"queueName"`
	Message    string `json:"message"`
}

type SubscribeReq struct {
	ServerName string `json:"serverName"`
	QueueName  string `json:"queueName"`
}
