package errors

const (
	CodeInternalServerError = "INTERNAL_SERVER_ERROR"
	CodeValidationFailed    = "JSON_SCHEMA_VALIDATION_ERROR"
	CodeUnauthorized        = "UNAUTHORIZED"
	CodeForbidden           = "FORBIDDEN"
	CodeNotFound            = "NOT_FOUND"
	CodeConflict            = "CONFLICT"
	CodeRateLimit           = "RATE_LIMIT_EXCEEDED"
	CodeExternalService     = "EXTERNAL_SERVICE_ERROR"
	CodeServiceUnavailable  = "SERVICE_UNAVAILABLE"
	CodeBadRequest          = "BAD_REQUEST"
	CodeGone                = "RESOURCE_GONE"
)
