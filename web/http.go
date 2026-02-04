package web

import (
	"net/http"

	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

type Response struct {
	Code int    `json:"code"` // 业务状态码（200=成功，非200=失败）
	Msg  string `json:"msg"`  // 提示信息
	Data any    `json:"data"` // 业务数据（成功时返回，失败时可为nil）
}

type httpServer struct {
	engine *gin.Engine
}

var HttpServer *httpServer

func init() {
	gin.SetMode(gin.ReleaseMode)
	HttpServer = &httpServer{
		engine: gin.Default(),
	}
	HttpServer.Use(middleware.ResponseMiddleware())
}

// Get 注册GET路由，使用中间件自动处理controller方法
func (s *httpServer) Get(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	middlewares := append(handlerMiddlewares, middleware.ControllerAdapter(controller))
	s.engine.GET(path, middlewares...)
}

// Post 注册POST路由，使用中间件自动处理controller方法
func (s *httpServer) Post(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	middlewares := append(handlerMiddlewares, middleware.ControllerAdapter(controller))
	s.engine.POST(path, middlewares...)
}

// Put 注册PUT路由，使用中间件自动处理controller方法
func (s *httpServer) Put(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	middlewares := append(handlerMiddlewares, middleware.ControllerAdapter(controller))
	s.engine.PUT(path, middlewares...)
}

// Delete 注册DELETE路由，使用中间件自动处理controller方法
func (s *httpServer) Delete(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	middlewares := append(handlerMiddlewares, middleware.ControllerAdapter(controller))
	s.engine.DELETE(path, middlewares...)
}

// Use 注册全局中间件
func (s *httpServer) Use(middlewares ...gin.HandlerFunc) {
	s.engine.Use(middlewares...)
}

// Run 启动HTTP服务
func (s *httpServer) Run(addr ...string) error {
	return s.engine.Run(addr...)
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
