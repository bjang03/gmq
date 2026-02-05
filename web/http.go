package web

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

//go:embed ui
var uiFS embed.FS

// WebServerConfig Web服务器配置
type WebServerConfig struct {
	PrintRoutes bool   // 是否打印路由信息
	Addr        string // 服务器地址，默认为 :1688
}

type httpServer struct {
	engine      *gin.Engine
	server      *http.Server
	printRoutes bool
	addr        string
}

var HttpServer *httpServer

func init() {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.ResponseMiddleware())

	HttpServer = &httpServer{
		engine:      engine,
		printRoutes: false,
	}
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

// registerRoute 统一的路由注册方法，减少代码重复
func (s *httpServer) registerRoute(method, path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	middlewares := append(handlerMiddlewares, middleware.ControllerAdapter(controller))
	s.engine.Handle(method, path, middlewares...)
}

// Get 注册GET路由，使用中间件自动处理controller方法
func (s *httpServer) Get(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	s.registerRoute("GET", path, controller, handlerMiddlewares...)
}

// Post 注册POST路由，使用中间件自动处理controller方法
func (s *httpServer) Post(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	s.registerRoute("POST", path, controller, handlerMiddlewares...)
}

// Put 注册PUT路由，使用中间件自动处理controller方法
func (s *httpServer) Put(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	s.registerRoute("PUT", path, controller, handlerMiddlewares...)
}

// Delete 注册DELETE路由，使用中间件自动处理controller方法
func (s *httpServer) Delete(path string, controller interface{}, handlerMiddlewares ...gin.HandlerFunc) {
	s.registerRoute("DELETE", path, controller, handlerMiddlewares...)
}

// Use 注册全局中间件
func (s *httpServer) Use(middlewares ...gin.HandlerFunc) {
	s.engine.Use(middlewares...)
}

// Run 启动HTTP服务
func (s *httpServer) Run(addr ...string) error {
	s.printRoutesInfo()

	// 确定服务器地址
	s.addr = ":1688" // 默认地址
	if len(addr) > 0 && addr[0] != "" {
		s.addr = addr[0]
	}

	// 创建 http.Server 实例以支持优雅关闭
	s.server = &http.Server{
		Addr:    s.addr,
		Handler: s.engine,
	}

	return s.server.ListenAndServe()
}

// Shutdown 优雅关闭HTTP服务器
func (s *httpServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// ShutdownWithTimeout 使用超时时间优雅关闭HTTP服务器
func (s *httpServer) ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Shutdown(ctx)
}

// GetEngine 获取 gin 引擎实例
func (s *httpServer) GetEngine() *gin.Engine {
	return s.engine
}

// RegisterStaticRoutes 注册静态文件路由
// 只注册 /ui 前缀，自动处理 ui 目录下的所有文件
func RegisterStaticRoutes(engine *gin.Engine) {
	// 首页路由 - 返回 index.html
	engine.GET("/", func(c *gin.Context) {
		data, err := uiFS.ReadFile("ui/html/index.html")
		if err != nil {
			c.String(500, "Error: %v", err)
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, string(data))
	})

	// 静态文件服务 - /ui/*filepath 自动映射到 ui 目录下的文件
	engine.GET("/ui/*filepath", func(c *gin.Context) {
		filePathParam := c.Param("filepath")
		if filePathParam == "" || filePathParam == "/" {
			c.String(404, "Not found")
			return
		}

		// 移除开头的 /
		if filePathParam[0] == '/' {
			filePathParam = filePathParam[1:]
		}

		// 构建完整的 embed 路径
		fullPath := "ui/" + filePathParam

		data, err := uiFS.ReadFile(fullPath)
		if err != nil {
			c.String(404, "Not found: %s", filePathParam)
			return
		}

		// 根据文件扩展名设置 Content-Type
		contentType := getContentTypeByPath(filePathParam)
		c.Header("Content-Type", contentType)
		c.String(200, string(data))
	})
}

// getContentTypeByPath 根据文件扩展名返回 Content-Type
func getContentTypeByPath(path string) string {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".html":
		return "text/html; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	default:
		return "text/plain; charset=utf-8"
	}
}
