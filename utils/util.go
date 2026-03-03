// Package utils provides utility functions for the GMQ message queue system.
// It includes logging utilities, validation, configuration loading, and type conversion helpers.
package utils

import (
	"encoding/json"
	"fmt"
	"github.com/bjang03/gmq/types"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
	"os"
	"reflect"
	"strings"
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

// ConvertToMap converts generic data to map[string]interface{} for Redis compatibility.
// Core improvements:
// 1. Uses type assertion and cast package for common types
// 2. Serializes slices/arrays directly to JSON strings (avoid []interface{})
// 3. All values end up as string/int/float/bool, can be directly stored in Redis
// Parameters:
//   - data: input data to convert (supports maps, basic types, slices, structs)
//
// Returns converted map or error if conversion fails
func ConvertToMap(data interface{}) (map[string]interface{}, error) {
	if data == nil {
		return nil, fmt.Errorf("convert failed: data is nil (input type: nil)")
	}

	// handle level-1 pointers (no reflection, only support common pointer types)
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
		// use cast package to safely dereference
		return ConvertToMap(cast.ToString(v))
	case *[]string, *[]int, *[]int64:
		// slice pointer: dereference then serialize to JSON
		if v == nil {
			return nil, fmt.Errorf("convert failed: nil slice pointer (type: %T)", data)
		}
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("convert failed: marshal slice pointer error (%v) (type: %T)", err, data)
		}
		return map[string]interface{}{"data": string(jsonBytes)}, nil
	}

	// 1. prioritize handling native map types
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

	// 2. handle basic types (direct wrapping)
	switch v := data.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return map[string]interface{}{"data": v}, nil
	}

	// 3. handle slice/array types (core fix: serialize directly to JSON string)
	switch v := data.(type) {
	case []string, []int, []int64, []float64, []bool, []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("convert failed: marshal slice error (%v) (type: %T)", err, data)
		}
		return map[string]interface{}{"data": string(jsonBytes)}, nil
	}

	// 4. fallback: JSON serialize all complex types (structs/custom types)
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		res := cast.ToStringMap(data)
		if len(res) > 0 {
			return res, nil
		}
		return nil, fmt.Errorf("convert failed: json marshal error (%v), cast also return empty (type: %T)", err, data)
	}

	// try to deserialize to map (structs/JSON objects)
	var res map[string]interface{}
	if err = json.Unmarshal(jsonBytes, &res); err == nil {
		return res, nil
	}

	// final fallback: serialize to JSON string wrapper
	return map[string]interface{}{"data": string(jsonBytes)}, nil
}
