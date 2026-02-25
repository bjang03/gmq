package api

import (
	"context"
	"sync"
	"time"

	"github.com/bjang03/gmq/core"
	"github.com/bjang03/gmq/utils"
)

var (
	// 监控WebSocket管理器
	metricsWSManager = utils.NewWebSocketManager()

	broadcastOnce sync.Once
)

// WSMetricsHandler WebSocket指标处理器
func WSMetricsHandler(ctx *utils.Context) {
	initialData := &utils.WebSocketMessage{
		Type:    utils.MessageTypeMetrics,
		Payload: getAllMetrics(),
	}

	_, _ = metricsWSManager.HandleConnection(ctx.W, ctx.R, nil, initialData)
}

// StartMetricsBroadcast 启动指标广播协程
func StartMetricsBroadcast() {
	broadcastOnce.Do(func() {
		metricsWSManager.StartBroadcastLoop(2*time.Second, func() *utils.WebSocketMessage {
			return &utils.WebSocketMessage{
				Type:    utils.MessageTypeMetrics,
				Payload: getAllMetrics(),
			}
		})
	})
}

// StopMetricsBroadcast 停止指标广播协程
func StopMetricsBroadcast() {
	metricsWSManager.StopBroadcastLoop()
	metricsWSManager.CloseAll()
}

// getAllMetrics 获取所有消息队列的监控指标
func getAllMetrics() map[string]*core.Metrics {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(map[string]*core.Metrics)
	for name, proxy := range core.GetAllGmq() {
		metrics := proxy.GmqGetMetrics(ctx)
		result[name] = metrics
	}

	return result
}
