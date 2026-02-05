package controller

import (
	"context"

	"github.com/bjang03/gmq/core"
)

type MonitorReq struct {
	Name string `json:"name" form:"name"` // 消息队列名称
}

// GetMetrics 获取监控指标
func GetMetrics(ctx context.Context, req MonitorReq) (res interface{}, err error) {
	// 空名称时返回空结果而不是nil，避免中断请求处理
	if req.Name == "" {
		return make(map[string]*core.Metrics), nil
	}

	pipeline, exists := core.GetGmq(req.Name)
	if !exists {
		return make(map[string]*core.Metrics), nil
	}

	metrics := pipeline.GetMetrics(ctx)
	result := map[string]*core.Metrics{
		req.Name: metrics,
	}
	return result, nil
}

// GetAllMetrics 获取所有消息队列的监控指标
func GetAllMetrics(ctx context.Context, req MonitorReq) (res interface{}, err error) {
	// req参数在此方法中未使用，但保留以符合中间件签名要求
	result := make(map[string]*core.Metrics)

	for name, pipeline := range core.GetAllGmq() {
		metrics := pipeline.GetMetrics(ctx)
		result[name] = metrics
	}

	return result, nil
}
