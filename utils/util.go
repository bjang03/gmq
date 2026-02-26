package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/spf13/cast"
)

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

func IsEmpty(value any, traceSource ...bool) bool {
	if value == nil {
		return true
	}
	// It firstly checks the variable as common types using assertion to enhance the performance,
	// and then using reflection.
	switch result := value.(type) {
	case int:
		return result == 0
	case int8:
		return result == 0
	case int16:
		return result == 0
	case int32:
		return result == 0
	case int64:
		return result == 0
	case uint:
		return result == 0
	case uint8:
		return result == 0
	case uint16:
		return result == 0
	case uint32:
		return result == 0
	case uint64:
		return result == 0
	case float32:
		return result == 0
	case float64:
		return result == 0
	case bool:
		return !result
	case string:
		return result == ""
	case []byte:
		return len(result) == 0
	case []rune:
		return len(result) == 0
	case []int:
		return len(result) == 0
	case []string:
		return len(result) == 0
	case []float32:
		return len(result) == 0
	case []float64:
		return len(result) == 0
	case map[string]any:
		return len(result) == 0

	default:
		// Finally, using reflect.
		var rv reflect.Value
		if v, ok := value.(reflect.Value); ok {
			rv = v
		} else {
			rv = reflect.ValueOf(value)
			if IsNil(rv) {
				return true
			}

			// =========================
			// Common interfaces checks.
			// =========================
			if f, ok := value.(iTime); ok {
				if f == (*time.Time)(nil) {
					return true
				}
				return f.IsZero()
			}
			if f, ok := value.(iString); ok {
				if f == nil {
					return true
				}
				return f.String() == ""
			}
			if f, ok := value.(iInterfaces); ok {
				if f == nil {
					return true
				}
				return len(f.Interfaces()) == 0
			}
			if f, ok := value.(iMapStrAny); ok {
				if f == nil {
					return true
				}
				return len(f.MapStrAny()) == 0
			}
		}

		switch rv.Kind() {
		case reflect.Bool:
			return !rv.Bool()

		case
			reflect.Int,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64:
			return rv.Int() == 0

		case
			reflect.Uint,
			reflect.Uint8,
			reflect.Uint16,
			reflect.Uint32,
			reflect.Uint64,
			reflect.Uintptr:
			return rv.Uint() == 0

		case
			reflect.Float32,
			reflect.Float64:
			return rv.Float() == 0

		case reflect.String:
			return rv.Len() == 0

		case reflect.Struct:
			var fieldValueInterface any
			for i := 0; i < rv.NumField(); i++ {
				fieldValueInterface, _ = ValueToInterface(rv.Field(i))
				if !IsEmpty(fieldValueInterface) {
					return false
				}
			}
			return true

		case
			reflect.Chan,
			reflect.Map,
			reflect.Slice,
			reflect.Array:
			return rv.Len() == 0

		case reflect.Pointer:
			if len(traceSource) > 0 && traceSource[0] {
				return IsEmpty(rv.Elem())
			}
			return rv.IsNil()

		case
			reflect.Func,
			reflect.Interface,
			reflect.UnsafePointer:
			return rv.IsNil()

		case reflect.Invalid:
			return true

		default:
			return false
		}
	}
}

func IsNil(value any, traceSource ...bool) bool {
	if value == nil {
		return true
	}
	var rv reflect.Value
	if v, ok := value.(reflect.Value); ok {
		rv = v
	} else {
		rv = reflect.ValueOf(value)
	}
	switch rv.Kind() {
	case reflect.Chan,
		reflect.Map,
		reflect.Slice,
		reflect.Func,
		reflect.Interface,
		reflect.UnsafePointer:
		return !rv.IsValid() || rv.IsNil()

	case reflect.Pointer:
		if len(traceSource) > 0 && traceSource[0] {
			for rv.Kind() == reflect.Pointer {
				rv = rv.Elem()
			}
			if !rv.IsValid() {
				return true
			}
			if rv.Kind() == reflect.Pointer {
				return rv.IsNil()
			}
		} else {
			return !rv.IsValid() || rv.IsNil()
		}

	default:
		return false
	}
	return false
}

// ValueToInterface converts reflect value to its interface type.
func ValueToInterface(v reflect.Value) (value any, ok bool) {
	if v.IsValid() && v.CanInterface() {
		return v.Interface(), true
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint(), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	case reflect.Complex64, reflect.Complex128:
		return v.Complex(), true
	case reflect.String:
		return v.String(), true
	case reflect.Pointer:
		return ValueToInterface(v.Elem())
	case reflect.Interface:
		return ValueToInterface(v.Elem())
	default:
		return nil, false
	}
}

// iString is used for type assert api for String().
type iString interface {
	String() string
}

// iInterfaces is used for type assert api for Interfaces.
type iInterfaces interface {
	Interfaces() []any
}

// iMapStrAny is the interface support for converting struct parameter to map.
type iMapStrAny interface {
	MapStrAny() map[string]any
}

type iTime interface {
	Date() (year int, month time.Month, day int)
	IsZero() bool
}

// ToBool 公开的字符串转bool方法
func ToBool(s string) bool {
	return s == "true" || s == "1"
}

// ToInt 公开的字符串转int方法
func ToInt(s string) int {
	i := 0
	if s != "" {
		fmt.Sscanf(s, "%d", &i)
	}
	return i
}
