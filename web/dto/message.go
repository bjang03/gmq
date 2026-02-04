package dto

type PublishReq struct {
	Topic   string `json:"topic" form:"topic"`
	Message string `json:"message" form:"message"`
}

type SubscribeReq struct {
	Topic string `json:"topic" form:"topic"`
}
