package controller

import (
	"context"

	"github.com/bjang03/gmq/core"
	"github.com/gin-gonic/gin"
)

type MonitorReq struct {
	Name string `json:"name" form:"name"` // 消息队列名称
}

// GetMetrics 获取监控指标
func GetMetrics(c *gin.Context, req *MonitorReq) (res interface{}, err error) {
	if req.Name == "" {
		return nil, nil
	}

	pipeline, exists := core.GetGmq(req.Name)
	if !exists {
		return nil, nil
	}

	metrics := pipeline.GetMetrics(c.Request.Context())
	return metrics, nil
}

// GetAllMetrics 获取所有消息队列的监控指标
func GetAllMetrics(ctx context.Context, req *MonitorReq) (res interface{}, err error) {
	result := make(map[string]*core.Metrics)

	for name, pipeline := range core.GetAllGmq() {
		metrics := pipeline.GetMetrics(ctx)
		result[name] = metrics
	}

	return result, nil
}
