package middleware

import (
	"github.com/gin-gonic/gin"
)

func ResponseMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				Fail(c, c.Err().Error())
				c.Abort() // 终止后续处理
			}
		}()
		c.Next()
		if !c.Writer.Written() {
			// 默认返回成功（可根据业务调整）
			Success(c, c.)
		}
	}
}
