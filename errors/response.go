package errors

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// HandleValidationError returns a 400 with per-field validation details.
func HandleValidationError(c *gin.Context, err error) {
	c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{
		Error: &ErrorDetail{
			Code:        CodeValidationFailed,
			Message:     MsgValidationFailed,
			StatusCode:  http.StatusBadRequest,
			FieldErrors: ExtractFieldErrors(err),
		},
	})
}

// HandleBadRequestError returns a 400 with a custom message.
func HandleBadRequestError(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeBadRequest,
			Message:    message,
			StatusCode: http.StatusBadRequest,
		},
	})
}

// HandleUnauthorizedError returns a 401 with the reason in Meta.
func HandleUnauthorizedError(c *gin.Context, reason string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeUnauthorized,
			Message:    MsgUnauthorized,
			StatusCode: http.StatusUnauthorized,
			Meta:       map[string]interface{}{"reason": reason},
		},
	})
}

// HandleForbiddenError returns a 403.
func HandleForbiddenError(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusForbidden, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeForbidden,
			Message:    MsgForbidden,
			StatusCode: http.StatusForbidden,
		},
	})
}

// HandleNotFoundError returns a 404.
func HandleNotFoundError(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusNotFound, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeNotFound,
			Message:    MsgNotFound,
			StatusCode: http.StatusNotFound,
		},
	})
}

// HandleConflictError returns a 409.
func HandleConflictError(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusConflict, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeConflict,
			Message:    MsgConflict,
			StatusCode: http.StatusConflict,
		},
	})
}

// HandleInternalError returns a 500 with optional debugging meta.
func HandleInternalError(c *gin.Context, meta map[string]interface{}) {
	c.AbortWithStatusJSON(http.StatusInternalServerError, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeInternalServerError,
			Message:    MsgInternalServerError,
			StatusCode: http.StatusInternalServerError,
			Meta:       meta,
		},
	})
}

// HandleServiceUnavailableError returns a 503 with the failing service in Meta.
func HandleServiceUnavailableError(c *gin.Context, service string) {
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeServiceUnavailable,
			Message:    MsgServiceUnavailable,
			StatusCode: http.StatusServiceUnavailable,
			Meta:       map[string]interface{}{"service": service},
		},
	})
}

// HandleRateLimitError returns a 429.
func HandleRateLimitError(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusTooManyRequests, ErrorResponse{
		Error: &ErrorDetail{
			Code:       CodeRateLimit,
			Message:    MsgRateLimit,
			StatusCode: http.StatusTooManyRequests,
		},
	})
}

// HandleError is the generic escape hatch. Prefer typed helpers above.
func HandleError(c *gin.Context, status int, code, message string, meta map[string]interface{}) {
	c.AbortWithStatusJSON(status, ErrorResponse{
		Error: &ErrorDetail{
			Code:       code,
			Message:    message,
			StatusCode: status,
			Meta:       meta,
		},
	})
}

// DecodeJSON reads + validates the request body into dst.
// Caps body at maxBytes to prevent OOM from a malicious client.
func DecodeJSON(c *gin.Context, dst any, maxBytes int64) error {
	c.Request.Body = http.MaxBytesReader(nil, c.Request.Body, maxBytes)
	return c.ShouldBindJSON(dst)
}

// JSON writes an arbitrary value as application/json with the given status.
func JSON(c *gin.Context, status int, v any) {
	c.JSON(status, v)
}

// Error maps a plain status+message to the structured ErrorResponse.
// Compatibility shim — prefer typed Handle* helpers for new code.
func Error(c *gin.Context, status int, message string) {
	HandleError(c, status, statusToCode(status), message, nil)
}

// ErrorWithCode maps the old code/message/meta style to HandleError.
// Compatibility shim — prefer typed Handle* helpers for new code.
func ErrorWithCode(c *gin.Context, status int, code, message string, meta map[string]any) {
	HandleError(c, status, code, message, meta)
}

func statusToCode(status int) string {
	switch status {
	case 400:
		return CodeBadRequest
	case 401:
		return CodeUnauthorized
	case 403:
		return CodeForbidden
	case 404:
		return CodeNotFound
	case 409:
		return CodeConflict
	case 429:
		return CodeRateLimit
	case 503:
		return CodeServiceUnavailable
	default:
		return CodeInternalServerError
	}
}
