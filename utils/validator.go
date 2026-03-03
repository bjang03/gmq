// Package utils provides utility functions for the GMQ message queue system.
// It includes logging utilities, validation, configuration loading, and type conversion helpers.
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

// init initializes the validator package.
// Creates a new validator instance and registers a custom tag name parsing function
// that uses json field names instead of struct field names for validation error messages.
func init() {
	validate = validator.New()
	// Register custom tag name parsing function
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
}

// ValidateStruct validates a struct using registered validation rules.
// Uses go-playground/validator for field validation.
// Parameters:
//   - s: the struct to validate (should be a pointer to struct)
// Returns formatted error message if validation fails
func ValidateStruct(s interface{}) error {
	if err := validate.Struct(s); err != nil {
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			return fmt.Errorf("validation failed: %s", FormatValidationError(validationErrors))
		}
		return fmt.Errorf("validation failed: %v", err)
	}
	return nil
}

// FormatValidationError formats validation errors into human-readable messages.
// Converts validator.ValidationErrors to a semicolon-separated string of field-level errors.
// Parameters:
//   - errs: validation errors from the validator
// Returns formatted error message string
func FormatValidationError(errs validator.ValidationErrors) string {
	var errMsgs []string
	for _, err := range errs {
		field := err.Field()
		tag := err.Tag()
		param := err.Param()

		switch tag {
		case "required":
			errMsgs = append(errMsgs, fmt.Sprintf("%s is required", field))
		case "min":
			errMsgs = append(errMsgs, fmt.Sprintf("%s length must be at least %s", field, param))
		case "max":
			errMsgs = append(errMsgs, fmt.Sprintf("%s length must be at most %s", field, param))
		case "email":
			errMsgs = append(errMsgs, fmt.Sprintf("%s must be a valid email", field))
		case "url":
			errMsgs = append(errMsgs, fmt.Sprintf("%s must be a valid URL", field))
		case "oneof":
			errMsgs = append(errMsgs, fmt.Sprintf("%s must be one of: %s", field, param))
		default:
			errMsgs = append(errMsgs, fmt.Sprintf("%s validation failed: %s", field, tag))
		}
	}
	return strings.Join(errMsgs, "; ")
}
