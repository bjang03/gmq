package utils

import (
	"encoding/json"
	"fmt"
	"github.com/bjang03/gmq/types"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"reflect"
	"strings"
)

// MapToStruct 将map[string]interface{}转换为结构体（无反射）
func MapToStruct(target interface{}, dataMap map[string]interface{}) error {
	// 1. 检查入参是否为结构体指针
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target必须是结构体指针（如&natsConfig{}）")
	}
	// 解引用指针，获取结构体的可修改值
	structVal := val.Elem()
	// 获取结构体类型信息
	structType := structVal.Type()
	// 2. 遍历结构体的所有字段
	for i := 0; i < structVal.NumField(); i++ {
		// 获取字段值（可修改）
		field := structVal.Field(i)
		// 获取字段类型信息（名称、类型等）
		fieldInfo := structType.Field(i)
		// 结构体字段名（如Name、Url、Username）
		fieldName := fieldInfo.Name
		// 3. 从map中获取对应key的值（key=字段名，大小写敏感）
		// 如果map的key是小写（如"name"），可改为 strings.ToLower(fieldName) 匹配
		fieldName = strings.ToLower(fieldName)
		valFromMap, ok := dataMap[fieldName]
		if !ok {
			// map中无该key，跳过（也可返回错误，根据业务需求调整）
			continue
		}
		// 4. 用cast库将map中的值转换为字段类型，并赋值
		switch field.Kind() {
		case reflect.String:
			// 转换为字符串（cast.ToString支持任意类型转string）
			strVal := cast.ToString(valFromMap)
			field.SetString(strVal)
		case reflect.Int:
			// 如需支持int类型，可添加：
			intVal := cast.ToInt(valFromMap)
			field.SetInt(int64(intVal))
		case reflect.Bool:
			// 如需支持bool类型，可添加：
			boolVal := cast.ToBool(valFromMap)
			field.SetBool(boolVal)
		default:
			return fmt.Errorf("暂不支持字段%s的类型：%s", fieldName, field.Kind())
		}
	}
	return nil
}

// LoadGMQConfig 读取配置文件并返回 {name: 配置项} 的映射
// 参数: configPath - 配置文件路径（如config.yaml）
// 返回: 全局映射（key=配置项name，value=该配置项的所有信息）、错误信息
func LoadGMQConfig(configPath string) (*types.GMQRoot, map[string]types.MQItem, error) {
	// 1. 读取文件内容
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 2. 解析YAML到结构体
	var root *types.GMQRoot
	if err := yaml.Unmarshal(content, &root); err != nil {
		return nil, nil, fmt.Errorf("解析YAML失败: %w", err)
	}

	// 3. 转换为 {name: 配置项} 的映射（核心逻辑）
	nameToConfig := make(map[string]types.MQItem)
	for mqType, items := range root.GMQ {
		for _, item := range items {
			// 校验name不能为空
			if item.Name == "" {
				log.Printf("警告: %s类型存在空name的配置项，已跳过", mqType)
				continue
			}
			// 校验name是否重复（重复则覆盖，也可改为报错）
			if _, exists := nameToConfig[item.Name]; exists {
				log.Printf("警告: 配置项name=%s重复，已覆盖", item.Name)
			}
			// 存入映射（key=name，value=完整配置项）
			nameToConfig[item.Name] = item
		}
	}

	return root, nameToConfig, nil
}

// ConvertToMap 通用数据转map[string]interface{}（无反射+适配Redis）
// 核心改进：
// 1. 完全移除反射，仅用类型断言+cast包
// 2. 切片/数组直接序列化为JSON字符串（避免[]interface{}）
// 3. 所有value最终为string/int/float/bool，可直接存入Redis
func ConvertToMap(data interface{}) (map[string]interface{}, error) {
	if data == nil {
		return nil, fmt.Errorf("convert failed: data is nil (input type: nil)")
	}

	// 处理一级指针（无反射，仅支持常见指针类型）
	switch v := data.(type) {
	case *map[string]interface{}:
		if v == nil {
			return nil, fmt.Errorf("convert failed: nil *map[string]interface{}")
		}
		return *v, nil
	case *map[string]string:
		if v == nil {
			return nil, fmt.Errorf("convert failed: nil *map[string]string")
		}
		res := make(map[string]interface{}, len(*v))
		for k, val := range *v {
			res[k] = val
		}
		return res, nil
	case *string, *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64,
		*float32, *float64, *bool:
		// 用cast包安全解引用
		return ConvertToMap(cast.ToString(v))
	case *[]string, *[]int, *[]int64:
		// 切片指针：解引用后序列化为JSON
		if v == nil {
			return nil, fmt.Errorf("convert failed: nil slice pointer (type: %T)", data)
		}
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("convert failed: marshal slice pointer error (%v) (type: %T)", err, data)
		}
		return map[string]interface{}{"data": string(jsonBytes)}, nil
	}

	// 1. 优先处理原生map类型
	switch v := data.(type) {
	case map[string]interface{}:
		return v, nil
	case map[string]string:
		res := make(map[string]interface{}, len(v))
		for k, val := range v {
			res[k] = val
		}
		return res, nil
	case map[string]int, map[string]int64, map[string]float64, map[string]bool:
		res := cast.ToStringMap(v)
		return res, nil
	}

	// 2. 处理基础类型（直接封装）
	switch v := data.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return map[string]interface{}{"data": v}, nil
	}

	// 3. 处理切片/数组类型（核心修复：直接序列化为JSON字符串）
	switch v := data.(type) {
	case []string, []int, []int64, []float64, []bool, []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("convert failed: marshal slice error (%v) (type: %T)", err, data)
		}
		return map[string]interface{}{"data": string(jsonBytes)}, nil
	}

	// 4. 兜底：JSON序列化所有复杂类型（结构体/自定义类型）
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		res := cast.ToStringMap(data)
		if len(res) > 0 {
			return res, nil
		}
		return nil, fmt.Errorf("convert failed: json marshal error (%v), cast also return empty (type: %T)", err, data)
	}

	// 尝试反序列化为map（结构体/JSON对象）
	var res map[string]interface{}
	if err = json.Unmarshal(jsonBytes, &res); err == nil {
		return res, nil
	}

	// 最终兜底：序列化为JSON字符串封装
	return map[string]interface{}{"data": string(jsonBytes)}, nil
}
