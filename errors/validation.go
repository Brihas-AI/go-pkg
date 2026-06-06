package errors

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

// ExtractFieldErrors converts a validator.ValidationErrors into FieldError slices.
// Falls back to a single "request" error for non-validation errors.
func ExtractFieldErrors(err error) []FieldError {
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		return []FieldError{{Field: "request", Message: err.Error()}}
	}
	out := make([]FieldError, 0, len(ve))
	for _, fe := range ve {
		out = append(out, FieldError{
			Field:   fe.Namespace(),
			Message: formatValidationMessage(fe),
		})
	}
	return out
}

func formatValidationMessage(fe validator.FieldError) string {
	field := toJSONFieldName(fe)
	switch fe.Tag() {
	case "required":
		return fmt.Sprintf("%s is required", field)
	case "min":
		return fmt.Sprintf("%s must have at least %s character(s)", field, fe.Param())
	case "max":
		return fmt.Sprintf("%s must not exceed %s", field, fe.Param())
	case "email":
		return fmt.Sprintf("%s must be a valid email", field)
	default:
		return fmt.Sprintf("%s is invalid", field)
	}
}

func toJSONFieldName(fe validator.FieldError) string {
	tag := fe.Field()
	jsonTag := fe.StructField()
	if tag != "" && tag != jsonTag {
		return tag
	}
	return strings.ToLower(jsonTag)
}
