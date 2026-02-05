package middleware

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	ginContextType     = reflect.TypeOf(&gin.Context{})
	contextContextType = reflect.TypeOf((*context.Context)(nil)).Elem()
)

// controllerCache Controller适配器缓存
type controllerCache struct {
	mu       sync.RWMutex
	adapters map[uintptr]gin.HandlerFunc
}

var cache = &controllerCache{
	adapters: make(map[uintptr]gin.HandlerFunc),
}

// getAdapter 从缓存获取适配器
func (c *controllerCache) getAdapter(controllerFunc interface{}) (gin.HandlerFunc, bool) {
	key := getFunctionKey(controllerFunc)
	c.mu.RLock()
	defer c.mu.RUnlock()
	adapter, ok := c.adapters[key]
	return adapter, ok
}

// setAdapter 缓存适配器
func (c *controllerCache) setAdapter(controllerFunc interface{}, adapter gin.HandlerFunc) {
	key := getFunctionKey(controllerFunc)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.adapters[key] = adapter
}

// getFunctionKey 获取函数的唯一key（使用函数指针地址）
func getFunctionKey(fn interface{}) uintptr {
	val := reflect.ValueOf(fn)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val.Pointer()
}

// ControllerAdapter 适配器，支持两种controller写法：
// 1. func(ctx context.Context, req Req) (res interface{}, err error)
// 2. func(c *gin.Context, req Req) (res interface{}, err error)
func ControllerAdapter(controllerFunc interface{}) gin.HandlerFunc {
	// 先检查缓存
	if adapter, ok := cache.getAdapter(controllerFunc); ok {
		return adapter
	}

	// 创建适配器
	adapter := createAdapter(controllerFunc)

	// 缓存适配器
	cache.setAdapter(controllerFunc, adapter)

	return adapter
}

// createAdapter 创建适配器
func createAdapter(controllerFunc interface{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				Fail(c, recoverError(err).Error())
				c.Abort()
			}
		}()

		// 获取controller函数的反射值和类型
		ctrlValue := reflect.ValueOf(controllerFunc)
		ctrlType := ctrlValue.Type()

		// 检查是否是函数类型
		if ctrlType.Kind() != reflect.Func {
			Fail(c, fmt.Sprintf("controller must be a function, got: %v", ctrlType.Kind()))
			c.Abort()
			return
		}

		// 检查函数签名，必须返回 (interface{}, error)
		if ctrlType.NumOut() != 2 || !ctrlType.Out(0).Implements(reflect.TypeOf((*interface{})(nil)).Elem()) || !ctrlType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			Fail(c, "controller function signature error: must return (interface{}, error)")
			c.Abort()
			return
		}

		// 准备参数
		args := make([]reflect.Value, ctrlType.NumIn())

		// 处理第一个参数：context.Context 或 *gin.Context
		firstParamType := ctrlType.In(0)

		// 注意：*gin.Context 也实现了 context.Context 接口，所以需要先检查是否是指针类型
		if firstParamType == ginContextType {
			// *gin.Context 类型 - 必须优先检查，因为它也实现了 context.Context 接口
			args[0] = reflect.ValueOf(c)
		} else if firstParamType.Implements(contextContextType) {
			// context.Context 类型
			args[0] = reflect.ValueOf(c.Request.Context())
		} else {
			Fail(c, "controller first param must be context.Context or *gin.Context, got: "+firstParamType.String())
			c.Abort()
			return
		}

		// 处理第二个参数（如果存在）：请求体
		if ctrlType.NumIn() > 1 {
			reqType := ctrlType.In(1)
			var reqValue reflect.Value

			// 判断是指针类型还是值类型
			if reqType.Kind() == reflect.Ptr {
				// 指针类型：创建指向元素的指针
				reqValue = reflect.New(reqType.Elem())
				args[1] = reqValue
			} else {
				// 值类型：创建指向该类型的指针，然后取值
				reqValue = reflect.New(reqType)
				args[1] = reqValue.Elem()
			}

			// 绑定请求参数（始终使用 reqValue，它是一个指针）
			if err := c.ShouldBindJSON(reqValue.Interface()); err != nil {
				// 如果JSON绑定失败，尝试Query参数绑定
				if err := c.ShouldBindQuery(reqValue.Interface()); err != nil {
					Fail(c, "request params bind error: "+err.Error())
					c.Abort()
					return
				}
			}
		}

		// 调用controller函数
		results := ctrlValue.Call(args)

		// 解析返回值
		if len(results) == 2 {
			data := results[0].Interface()
			err, _ := results[1].Interface().(error)

			if err != nil {
				Fail(c, err.Error())
				return
			}

			Success(c, data)
		}
	}
}

// recoverError 处理panic错误
func recoverError(err interface{}) error {
	switch v := err.(type) {
	case string:
		return errors.New(v)
	case error:
		return v
	default:
		return errors.New("unknown error")
	}
}

// ResponseMiddleware 通用响应中间件
func ResponseMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				Fail(c, recoverError(err).Error())
				c.Abort()
			}
		}()
		c.Next()
	}
}

// Success 成功响应
func Success(c *gin.Context, data interface{}) {
	c.JSON(200, map[string]interface{}{
		"code": 200,
		"msg":  "success",
		"data": data,
	})
}

// Fail 失败响应
func Fail(c *gin.Context, msg string) {
	c.JSON(500, map[string]interface{}{
		"code": 500,
		"msg":  msg,
		"data": nil,
	})
}
