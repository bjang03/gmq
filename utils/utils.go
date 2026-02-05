package utils

// GetInt64 从map中获取int64值
func GetInt64(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return int64(val)
		case int64:
			return val
		case int:
			return int64(val)
		}
	}
	return 0
}

// GetFloat64 从map中获取float64值
func GetFloat64(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok {
		if val, ok := v.(float64); ok {
			return val
		}
	}
	return 0
}

// GetString 从map中获取string值
func GetString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if val, ok := v.(string); ok {
			return val
		}
	}
	return ""
}

// GetInt 从map中获取int值
func GetInt(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		case int:
			return val
		case int64:
			return int(val)
		}
	}
	return 0
}
