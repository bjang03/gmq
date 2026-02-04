package web

import (
	"embed"
	"fmt"
	"net/http"

	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

//go:embed ui
var uiFS embed.FS

type Response struct {
	Code int    `json:"code"` // 业务状态码（200=成功，非200=失败）
	Msg  string `json:"msg"`  // 提示信息
	Data any    `json:"data"` // 业务数据（成功时返回，失败时可为nil）
}

// ServerConfig 服务器配置
type ServerConfig struct {
	PrintRoutes bool   // 是否打印路由信息
	Addr        string // 服务器地址，默认为 :1688
}

type httpServer struct {
	engine      *gin.Engine
	printRoutes bool
}

var HttpServer *httpServer

func init() {
	gin.SetMode(gin.ReleaseMode)
	HttpServer = &httpServer{
		engine:      gin.New(),
		printRoutes: false,
	}
	HttpServer.engine.Use(gin.Recovery())
	HttpServer.engine.Use(middleware.ResponseMiddleware())
}

// RegisterRoutes 注册所有业务路由
// 在 main.go 中调用，用于注册业务路由
func RegisterRoutes(registerFunc func()) {
	registerFunc()
}

// SetPrintRoutes 设置是否打印路由信息
func (s *httpServer) SetPrintRoutes(enabled bool) {
	s.printRoutes = enabled
}

// printRoutesInfo 结构化输出所有路由信息
func (s *httpServer) printRoutesInfo() {
	if !s.printRoutes {
		return
	}

	routes := s.engine.Routes()
	if len(routes) == 0 {
		fmt.Println("No routes registered")
		return
	}

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    Registered HTTP Routes                    ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ %-6s │ %-50s ║\n", "Method", "Path")
	fmt.Println("╠════════╪══════════════════════════════════════════════════════╣")

	for _, route := range routes {
		fmt.Printf("║ %-6s │ %-50s ║\n", route.Method, route.Path)
	}

	fmt.Println("╚════════╧══════════════════════════════════════════════════════╝")
	fmt.Printf("Total routes: %d\n\n", len(routes))
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
	s.printRoutesInfo()
	return s.engine.Run(addr...)
}

// GetEngine 获取 gin 引擎实例
func (s *httpServer) GetEngine() *gin.Engine {
	return s.engine
}

// RegisterStaticRoutes 注册静态文件路由
func RegisterStaticRoutes(engine *gin.Engine) {
	// 注册页面路由
	engine.GET("/", func(c *gin.Context) {
		data, err := uiFS.ReadFile("ui/html/index.html")
		if err != nil {
			c.String(500, "Error: %v", err)
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, string(data))
	})

	// CSS
	engine.GET("/css/style.css", func(c *gin.Context) {
		data, err := uiFS.ReadFile("ui/css/style.css")
		if err != nil {
			c.String(500, "Error: %v", err)
			return
		}
		c.Header("Content-Type", "text/css; charset=utf-8")
		c.String(200, string(data))
	})

	// JS
	engine.GET("/js/app.js", func(c *gin.Context) {
		data, err := uiFS.ReadFile("ui/js/app.js")
		if err != nil {
			c.String(500, "Error: %v", err)
			return
		}
		c.Header("Content-Type", "application/javascript; charset=utf-8")
		c.String(200, string(data))
	})
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
