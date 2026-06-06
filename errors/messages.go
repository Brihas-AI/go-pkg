package errors

const (
	MsgInternalServerError = "Something went wrong. Please try again later."
	MsgValidationFailed    = "Invalid request payload. Please fix the highlighted fields."
	MsgUnauthorized        = "Unauthorized access."
	MsgForbidden           = "Access to this resource is forbidden."
	MsgNotFound            = "The requested resource was not found."
	MsgConflict            = "This resource already exists."
	MsgRateLimit           = "Rate limit exceeded. Please try again later."
	MsgExternalService     = "An external service failed. Please try again later."
	MsgServiceUnavailable  = "Service temporarily unavailable. Please try again later."
	MsgBadRequest          = "The request was invalid."
	MsgGone                = "This resource is no longer available."
)
