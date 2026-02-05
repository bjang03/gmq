package web

import (
	"embed"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bjang03/gmq/web/middleware"
	"github.com/gin-gonic/gin"
)

//go:embed ui
var uiFS embed.FS

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
		filepath := c.Param("filepath")
		if filepath == "" || filepath == "/" {
			c.String(404, "Not found")
			return
		}

		// 移除开头的 /
		if filepath[0] == '/' {
			filepath = filepath[1:]
		}

		// 构建完整的 embed 路径
		fullPath := "ui/" + filepath

		data, err := uiFS.ReadFile(fullPath)
		if err != nil {
			c.String(404, "Not found: %s", filepath)
			return
		}

		// 根据文件扩展名设置 Content-Type
		contentType := getContentType(filepath)
		c.Header("Content-Type", contentType)
		c.String(200, string(data))
	})
}

// getContentType 根据文件扩展名返回 Content-Type
func getContentType(path string) string {
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
