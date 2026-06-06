package errors

// FieldError represents a validation failure on a specific input field.
type FieldError struct {
	Field   string `json:"field_path"`
	Message string `json:"message"`
}

// ErrorDetail carries the full error payload returned to the client.
type ErrorDetail struct {
	Type        string                 `json:"type,omitempty"`
	Code        string                 `json:"code"`
	Message     string                 `json:"message"`
	StatusCode  int                    `json:"status_code"`
	FieldErrors []FieldError           `json:"field_errors,omitempty"`
	Meta        map[string]interface{} `json:"meta,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
}

// ErrorResponse is the standard envelope for all API errors.
// Always has Success=false and a non-nil Error pointer.
type ErrorResponse struct {
	Success bool         `json:"success"`
	Error   *ErrorDetail `json:"errors"`
}
