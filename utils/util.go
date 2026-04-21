// Package utils provides utility functions for the GMQ message queue system.
// It includes logging utilities, validation, configuration loading, and type conversion helpers.
package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/bjang03/gmq/types"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// MapToStruct converts map[string]interface{} to struct field values.
// Performs case-insensitive matching between map keys and struct field names.
// Supports field types: string, int, bool.
// Parameters:
//   - target: struct pointer to populate (e.g., &config{})
//   - dataMap: source map with key-value pairs
//
// Returns error if target is not a struct pointer or contains unsupported field types
func MapToStruct(target interface{}, dataMap map[string]interface{}) error {
	// 1. check if input is a struct pointer
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("%w (e.g. &config{})", types.ErrTargetMustBeStructPtr)
	}
	// dereference pointer to get modifiable struct value
	structVal := val.Elem()
	// get struct type information
	structType := structVal.Type()
	// 2. iterate through all fields of the struct
	for i := 0; i < structVal.NumField(); i++ {
		// get field value (modifiable)
		field := structVal.Field(i)
		// get field type information (name, type, etc.)
		fieldInfo := structType.Field(i)
		// struct field name (e.g. Name, Url, Username)
		fieldName := fieldInfo.Name
		// 3. get corresponding value from map (key=field name, case sensitive)
		// if map key is lowercase (e.g. "name"), can use strings.ToLower(fieldName) for matching
		fieldName = strings.ToLower(fieldName)
		valFromMap, ok := dataMap[fieldName]
		if !ok {
			// key not in map, skip (can also return error, adjust based on business needs)
			continue
		}
		// 4. use cast library to convert map value to field type and assign
		switch field.Kind() {
		case reflect.String:
			strVal := cast.ToString(valFromMap)
			field.SetString(strVal)
		case reflect.Int:
			intVal := cast.ToInt(valFromMap)
			field.SetInt(int64(intVal))
		case reflect.Bool:
			boolVal := cast.ToBool(valFromMap)
			field.SetBool(boolVal)
		default:
			return fmt.Errorf("%w: %s", types.ErrUnsupportedFieldType, fieldName)
		}
	}
	return nil
}

// LoadGMQConfig reads configuration from config.yml file and parses it.
// The function expects a YAML file with the gmq configuration structure.
// Returns the parsed GMQConfig or error if file doesn't exist or is malformed
func LoadGMQConfig(configPath string) (*types.GMQConfig, error) {
	// 1. read file content
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	// 2. parse YAML to struct
	config := new(types.GMQConfig)
	if err := yaml.Unmarshal(content, config); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}
	return config, nil
}

func ConvertToMap(data interface{}) (map[string]interface{}, error) {
	if data == nil {
		return map[string]interface{}{"data": ""}, nil
	}

	v := reflect.ValueOf(data)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return map[string]interface{}{"data": ""}, nil
		}
		v = v.Elem()
	}
	data = v.Interface()

	if v.Kind() == reflect.Struct {
		if timeStr, ok := getTimeStringFromStruct(v); ok {
			return map[string]interface{}{"data": timeStr}, nil
		}
	}

	switch m := data.(type) {
	case map[string]interface{}:
		cleanMap := make(map[string]interface{}, len(m))
		for k, val := range m {
			if val == nil {
				cleanMap[k] = ""
			} else {
				cleanMap[k] = val
			}
		}
		return cleanMap, nil
	case map[string]string:
		res := make(map[string]interface{}, len(m))
		for k, val := range m {
			res[k] = val
		}
		return res, nil
	case map[string]int, map[string]int64, map[string]float64, map[string]bool:
		return cast.ToStringMap(m), nil
	}

	switch data.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return map[string]interface{}{"data": data}, nil
	}

	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal slice failed: %v", err)
		}
		return map[string]interface{}{"data": string(jsonBytes)}, nil
	}

	if v.Kind() == reflect.Struct {
		structVal := v
		structType := structVal.Type()
		res := make(map[string]interface{}, structVal.NumField())
		for i := 0; i < structVal.NumField(); i++ {
			field := structType.Field(i)
			if field.PkgPath != "" {
				continue
			}
			fieldVal := structVal.Field(i).Interface()
			fieldMap, err := ConvertToMap(fieldVal)
			if err != nil {
				return nil, fmt.Errorf("convert field %s failed: %v", field.Name, err)
			}
			var finalVal interface{} = ""
			if dataVal, ok := fieldMap["data"]; ok {
				finalVal = dataVal
			} else if len(fieldMap) > 0 {
				jsonBytes, _ := json.Marshal(fieldMap)
				finalVal = string(jsonBytes)
			}
			res[field.Name] = finalVal
		}
		return res, nil
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %v (type: %T)", err, data)
	}
	return map[string]interface{}{"data": string(jsonBytes)}, nil
}

// 辅助函数：行为检测提取时间字符串
func getTimeStringFromStruct(v reflect.Value) (string, bool) {
	stringMethod := v.MethodByName("String")
	if stringMethod.IsValid() && stringMethod.Type().NumIn() == 0 && stringMethod.Type().NumOut() == 1 &&
		stringMethod.Type().Out(0).Kind() == reflect.String {
		results := stringMethod.Call(nil)
		if len(results) > 0 {
			return results[0].String(), true
		}
	}

	formatMethod := v.MethodByName("Format")
	if formatMethod.IsValid() && formatMethod.Type().NumIn() == 1 && formatMethod.Type().NumOut() == 1 &&
		formatMethod.Type().In(0).Kind() == reflect.String && formatMethod.Type().Out(0).Kind() == reflect.String {
		formats := []string{"Y-m-d H:i:s", time.RFC3339}
		for _, fmtStr := range formats {
			results := formatMethod.Call([]reflect.Value{reflect.ValueOf(fmtStr)})
			if len(results) > 0 && results[0].String() != "" {
				return results[0].String(), true
			}
		}
	}

	if timeVal, ok := v.Interface().(time.Time); ok {
		return timeVal.Format(time.RFC3339), true
	}

	return "", false
}
