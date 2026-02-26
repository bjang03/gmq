package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
)

// HandlerFunc 定义处理函数签名
type HandlerFunc interface{}

// Context 请求上下文
type Context struct {
	W      http.ResponseWriter
	R      *http.Request
	Params map[string]string
}

// Response HTTP响应结构体
type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// ServeMux 封装的ServeMux，支持中间件和方法限定
type ServeMux struct {
	mux   *http.ServeMux
	route []RouteInfo
}

// RouteInfo 路由信息(导出类型供外部使用)
type RouteInfo struct {
	Pattern string
	Method  string
	Handler HandlerFunc
}

// Server HTTP服务器
type Server struct {
	server       *http.Server
	addr         string
	shutdownHook func(ctx context.Context) error
}

// ServerManager 管理多个HTTP服务器
type ServerManager struct {
	servers      []*Server
	shutdownHook func(ctx context.Context) error
}

// NewServeMux 创建新的ServeMux
func NewServeMux() *ServeMux {
	return &ServeMux{
		mux:   http.NewServeMux(),
		route: make([]RouteInfo, 0),
	}
}

// NewServer 创建新的HTTP服务器
func NewServer(addr string, mux *ServeMux) *Server {
	return &Server{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		addr: addr,
	}
}

// SetShutdownHook 设置优雅关闭时的回调钩子
func (s *Server) SetShutdownHook(hook func(ctx context.Context) error) {
	s.shutdownHook = hook
}

// Start 启动HTTP服务器
func (s *Server) Start() error {
	log.Printf("HTTP server starting on %s", s.addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %v", err)
	}
	return nil
}

// StartWithGracefulShutdown 启动HTTP服务器并支持优雅关闭
func (s *Server) StartWithGracefulShutdown(shutdownTimeout time.Duration) error {
	// 启动HTTP服务器
	go func() {
		log.Printf("HTTP server starting on %s", s.addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// 执行关闭钩子
	if s.shutdownHook != nil {
		if err := s.shutdownHook(ctx); err != nil {
			log.Printf("Error during shutdown hook: %v", err)
		}
	}

	// 关闭HTTP服务器
	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
		return err
	}

	log.Println("Server stopped")
	return nil
}

// NewServerManager 创建服务器管理器
func NewServerManager(shutdownHook func(ctx context.Context) error) *ServerManager {
	return &ServerManager{
		servers:      make([]*Server, 0),
		shutdownHook: shutdownHook,
	}
}

// AddServer 添加服务器到管理器
func (sm *ServerManager) AddServer(server *Server) {
	sm.servers = append(sm.servers, server)
}

// StartAll 启动所有服务器(不阻塞)
func (sm *ServerManager) StartAll() {
	for _, srv := range sm.servers {
		go func(s *Server) {
			log.Printf("HTTP server starting on %s", s.addr)
			if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("HTTP server error on %s: %v", s.addr, err)
			}
		}(srv)
	}
}

// WaitForShutdown 等待关闭信号并优雅关闭所有服务器
func (sm *ServerManager) WaitForShutdown() {

	log.Println("Shutting down all servers...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 执行全局关闭钩子
	if sm.shutdownHook != nil {
		if err := sm.shutdownHook(ctx); err != nil {
			log.Printf("Error during shutdown hook: %v", err)
		}
	}

	// 关闭所有HTTP服务器
	for _, srv := range sm.servers {
		// 执行服务器级别的关闭钩子
		if srv.shutdownHook != nil {
			if err := srv.shutdownHook(ctx); err != nil {
				log.Printf("Error during server shutdown hook for %s: %v", srv.addr, err)
			}
		}
		// 关闭服务器
		if err := srv.server.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server on %s: %v", srv.addr, err)
		}
	}

	log.Println("All servers stopped")
}

// HandleFunc 包装mux.HandleFunc，支持方法限定
// method: "" (所有方法), "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"
func (sm *ServeMux) HandleFunc(pattern string, method string, handler HandlerFunc) {
	// 记录路由信息
	sm.route = append(sm.route, RouteInfo{
		Pattern: pattern,
		Method:  method,
		Handler: handler,
	})

	sm.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		// 方法校验
		if method != "" && r.Method != method {
			WriteJSONResponse(w, http.StatusMethodNotAllowed, Response{
				Code: 405,
				Msg:  "Method not allowed",
				Data: nil,
			})
			return
		}

		// 创建上下文
		ctx := &Context{
			W:      w,
			R:      r,
			Params: make(map[string]string),
		}

		// 中间件：日志记录
		log.Printf("[%s] %s %s", r.RemoteAddr, r.Method, r.URL.Path)

		// 恢复panic
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic recovered: %v", err)
				WriteJSONResponse(w, http.StatusInternalServerError, Response{
					Code: 500,
					Msg:  "Internal server error",
					Data: nil,
				})
			}
		}()

		// 智能识别handler类型并处理
		handleRequest(ctx, handler)
	})
}

// Get 注册GET请求
func (sm *ServeMux) Get(pattern string, handler HandlerFunc) {
	sm.HandleFunc(pattern, http.MethodGet, handler)
}

// Post 注册POST请求
func (sm *ServeMux) Post(pattern string, handler HandlerFunc) {
	sm.HandleFunc(pattern, http.MethodPost, handler)
}

// Put 注册PUT请求
func (sm *ServeMux) Put(pattern string, handler HandlerFunc) {
	sm.HandleFunc(pattern, http.MethodPut, handler)
}

// Delete 注册DELETE请求
func (sm *ServeMux) Delete(pattern string, handler HandlerFunc) {
	sm.HandleFunc(pattern, http.MethodDelete, handler)
}

// Patch 注册PATCH请求
func (sm *ServeMux) Patch(pattern string, handler HandlerFunc) {
	sm.HandleFunc(pattern, http.MethodPatch, handler)
}

// GetRoutes 获取所有注册的路由信息
func (sm *ServeMux) GetRoutes() []RouteInfo {
	return sm.route
}

// ServeHTTP 实现http.Handler接口
func (sm *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sm.mux.ServeHTTP(w, r)
}

// handleRequest 统一的请求处理逻辑，智能识别handler类型
func handleRequest(ctx *Context, handler HandlerFunc) {
	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()

	// 情况1: func(*Context) - 简单handler
	if handlerType.NumIn() == 1 && handlerType.In(0) == reflect.TypeOf(&Context{}) {
		handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx)})
		return
	}

	// 情况2: http.Handler - 标准库handler
	if httpHandler, ok := handler.(http.Handler); ok {
		httpHandler.ServeHTTP(ctx.W, ctx.R)
		return
	}

	// 情况3: func(context.Context, *Request) (*Response, error) - 业务handler
	if handlerType.NumIn() == 2 {
		// 检查第一个参数是否是context.Context
		if !handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			log.Printf("invalid handler: first parameter must be context.Context")
			http.Error(ctx.W, "Internal server error", http.StatusInternalServerError)
			return
		}

		// 检查第二个参数是否是指针类型
		reqType := handlerType.In(1)
		if reqType.Kind() != reflect.Ptr {
			log.Printf("invalid handler: second parameter must be a pointer")
			http.Error(ctx.W, "Internal server error", http.StatusInternalServerError)
			return
		}

		// 方法校验
		if ctx.R.Method != http.MethodPost {
			http.Error(ctx.W, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// 创建请求对象
		req := reflect.New(reqType.Elem()).Interface()

		// 参数解析
		if err := json.NewDecoder(ctx.R.Body).Decode(req); err != nil {
			WriteJSONResponse(ctx.W, http.StatusBadRequest, Response{
				Code: 400,
				Msg:  "Invalid request: " + err.Error(),
				Data: nil,
			})
			return
		}

		// 参数校验
		if err := ValidateStruct(req); err != nil {
			WriteJSONResponse(ctx.W, http.StatusBadRequest, Response{
				Code: 400,
				Msg:  err.Error(),
				Data: nil,
			})
			return
		}

		// 调用业务函数
		results := handlerValue.Call([]reflect.Value{
			reflect.ValueOf(ctx.R.Context()),
			reflect.ValueOf(req),
		})

		// 处理返回值
		if len(results) != 2 {
			log.Printf("invalid handler return: expected 2 values, got %d", len(results))
			http.Error(ctx.W, "Internal server error", http.StatusInternalServerError)
			return
		}

		// 第一个返回值是结果
		res := results[0].Interface()
		// 第二个返回值是error
		err := results[1].Interface()

		if err != nil {
			WriteJSONResponse(ctx.W, http.StatusInternalServerError, Response{
				Code: 500,
				Msg:  err.(error).Error(),
				Data: nil,
			})
			return
		}

		// 响应返回
		WriteJSONResponse(ctx.W, http.StatusOK, Response{
			Code: 200,
			Msg:  "success",
			Data: res,
		})
		return
	}

	// 不支持的handler类型
	log.Printf("unsupported handler signature")
	http.Error(ctx.W, "Internal server error", http.StatusInternalServerError)
}

// WriteJSONResponse 公开的JSON响应写入方法
func WriteJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
