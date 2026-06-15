package Http

import "net/http"

type ErrRequest interface {
	Error() string
	UserMessage() string
	StatusCode() int
}

// BaseError provides the foundation for custom request errors.
type BaseError struct {
	Code    string // Machine-readable error code
	Message string // Human-readable explanation
	Status  int    // HTTP status code
}

func (e BaseError) Error() string {
	return e.Code
}

func (e BaseError) UserMessage() string {
	return e.Message
}

func (e BaseError) StatusCode() int {
	if e.Status == 0 {
		return http.StatusInternalServerError
	}
	return e.Status
}

// Specific error types
type (
	ErrCloseBody     struct{ BaseError }
	ErrJSONDecode    struct{ BaseError }
	ErrJSONEncode    struct{ BaseError }
	ErrURLParse      struct{ BaseError }
	ErrRequestCreate struct{ BaseError }
	ErrRequestInit   struct{ BaseError }
	ErrTimeout       struct{ BaseError }
	ErrNon200        struct{ BaseError }
	ErrEmptyResponse struct{ BaseError }
	ErrRequestFailed struct{ BaseError }
	ErrCanceled      struct{ BaseError }
)

func NewErrJSONDecode() ErrRequest {
	return ErrJSONDecode{BaseError{
		Code:    "ERR_JSON_DECODE",
		Message: "Failed to decode JSON response",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrJSONEncode() ErrRequest {
	return ErrJSONEncode{BaseError{
		Code:    "ERR_JSON_ENCODE",
		Message: "Failed to encode JSON request",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrURLParse() ErrRequest {
	return ErrURLParse{BaseError{
		Code:    "ERR_URL_PARSE",
		Message: "Failed to parse URL",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrRequestCreate() ErrRequest {
	return ErrRequestCreate{BaseError{
		Code:    "ERR_CREATE_REQUEST",
		Message: "Failed to create HTTP request",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrRequestInit() ErrRequest {
	return ErrRequestInit{BaseError{
		Code:    "ERR_REQUEST_INIT",
		Message: "Failed to initialize HTTP request",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrTimeout() ErrRequest {
	return ErrTimeout{BaseError{
		Code:    "ERR_TIMEOUT",
		Message: "Request timed out",
		Status:  http.StatusGatewayTimeout, // Standard gateway timeout status code
	}}
}

func NewErrNon200(status int) ErrRequest {
	return ErrNon200{BaseError{
		Code:    "ERR_NON_200",
		Message: "Received non-200 HTTP status code",
		Status:  status,
	}}
}

func NewErrEmptyResponse() ErrRequest {
	return ErrEmptyResponse{BaseError{
		Code:    "ERR_EMPTY_RESPONSE",
		Message: "Response or body is nil",
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrRequestFailed(err error) ErrRequest {
	errMsg := "Request execution failed"
	if err != nil {
		errMsg = err.Error()
	}
	return ErrRequestFailed{BaseError{
		Code:    "ERR_REQUEST_FAILED",
		Message: errMsg,
		Status:  http.StatusInternalServerError,
	}}
}

func NewErrCanceled() ErrRequest {
	return ErrCanceled{BaseError{
		Code:    "ERR_CANCELED",
		Message: "Request was canceled",
		Status:  499, // Client Closed Request
	}}
}
