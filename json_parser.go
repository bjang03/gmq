// Package gmq JSON数据解析器实现
package gmq

import "encoding/json"

// JsonParser JSON数据解析器
type JsonParser struct {
	DateFormat string // 日期格式化规则(待实现)
}

// GmqParseData 将数据解析为标准格式
// 支持将结构体、map等类型转换为map[string]interface{}格式
func (*JsonParser) GmqParseData(data any) (dt any, err error) {
	bt, err := json.Marshal(data)
	if bt != nil {
		err = json.Unmarshal(bt, &dt)
	}
	return
}
