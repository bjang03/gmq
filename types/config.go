package types

// GMQConfig 定义更灵活的通用结构，适配各种YAML格式
type GMQConfig struct {
	GMQ map[string]map[string]interface{} `yaml:"gmq"` // 第一层：gmq -> 第二层(redis/nats) -> 第三层/配置项
}
