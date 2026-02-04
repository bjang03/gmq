package controller

import (
	"github.com/bjang03/gmq/components"
	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
)

// Publish 发布消息
func Publish(r *gin.Context) (res interface{}, err error) {
	err = core.GmqPlugins["redis"].GmqPublish(r.Request.Context(), &components.NatsPubMessage{})
	return
}

// Subscribe 订阅消息
func Subscribe(r *gin.Context) (res interface{}, err error) {
	err = core.GmqPlugins["redis"].GmqSubscribe(r.Request.Context(), nil)
	return
}
