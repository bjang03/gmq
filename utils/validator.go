package utils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

var (
	validate *validator.Validate
)

func init() {
	validate = validator.New()
	// 注册自定义标签名解析函数
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
}

// ValidateStruct 验证结构体
func ValidateStruct(s interface{}) error {
	if err := validate.Struct(s); err != nil {
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			return fmt.Errorf("参数校验失败: %s", FormatValidationError(validationErrors))
		}
		return fmt.Errorf("参数校验失败: %v", err)
	}
	return nil
}

// FormatValidationError 格式化验证错误
func FormatValidationError(errs validator.ValidationErrors) string {
	var errMsgs []string
	for _, err := range errs {
		field := err.Field()
		tag := err.Tag()
		param := err.Param()

		switch tag {
		case "required":
			errMsgs = append(errMsgs, fmt.Sprintf("%s 不能为空", field))
		case "min":
			errMsgs = append(errMsgs, fmt.Sprintf("%s 长度不能小于 %s", field, param))
		case "max":
			errMsgs = append(errMsgs, fmt.Sprintf("%s 长度不能大于 %s", field, param))
		case "email":
			errMsgs = append(errMsgs, fmt.Sprintf("%s 邮箱格式不正确", field))
		case "url":
			errMsgs = append(errMsgs, fmt.Sprintf("%s URL格式不正确", field))
		case "oneof":
			errMsgs = append(errMsgs, fmt.Sprintf("%s 必须是以下值之一: %s", field, param))
		default:
			errMsgs = append(errMsgs, fmt.Sprintf("%s 校验失败: %s", field, tag))
		}
	}
	return strings.Join(errMsgs, "; ")
}
