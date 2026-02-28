package types

// MQItem 单个MQ配置项（抽离name，其余字段用map存储，兼容所有MQ类型）
type MQItem struct {
	Name string                 `yaml:"name"`    // 配置项名称（作为key）
	Meta map[string]interface{} `yaml:",inline"` // 其余所有字段（自动内嵌）
}

// GMQRoot 配置文件根结构（第一层固定为gmq）
type GMQRoot struct {
	GMQ map[string][]MQItem `yaml:"gmq"` // key: MQ类型(redis/nats/...)，value: 该类型的配置列表
}
