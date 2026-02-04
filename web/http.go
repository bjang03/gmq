package web

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	Code int    `json:"code"` // 业务状态码（200=成功，非200=失败）
	Msg  string `json:"msg"`  // 提示信息
	Data any    `json:"data"` // 业务数据（成功时返回，失败时可为nil）
}

// Success 成功响应（默认状态码200，自定义消息和数据）
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code: 200,
		Data: data,
	})
}
func Fail(c *gin.Context, msg string) {
	c.JSON(http.StatusOK, Response{
		Code: 500,
		Msg:  msg,
	})
}

type ctrlFunc func(c *gin.Context) (interface{}, error)

func wrap(ctrlFunc ctrlFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		data, err := ctrlFunc(c)
		if err != nil {
			// 区分业务错误和系统错误
			Fail(c, err.Error())
			return
		}
		Success(c, data)
	}
}
