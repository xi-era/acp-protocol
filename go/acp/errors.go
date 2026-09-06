package acp

import "fmt"

// ACP error codes (spec §8.1) — 5-digit numbers whose first two digits align
// with HTTP semantic classes.
const (
	CodeParseError           = 40000
	CodeInvalidEnvelope      = 40001
	CodeUnknownOp            = 40002
	CodeUnsupportedVersion   = 40003
	CodeInvalidComponentID   = 40004
	CodeStreamRequired       = 40005
	CodeUnauthorized         = 40100
	CodeForbidden            = 40101
	CodeComponentNotFound    = 40400
	CodeComponentUnavailable = 40401
	CodeMethodNotAllowed     = 40500
	CodeUnsupportedMediaType = 41500
	CodeInvalidInput         = 42200
	CodeInvalidOutput        = 42201
	CodeRateLimited          = 42900
	CodeConcurrencyLimit     = 42901
	CodeSubscriptionLimit    = 42902
	CodeInternalError        = 50000
	CodeComponentError       = 50001
	CodeUpstreamError        = 50002
	CodeEventUnsupported     = 50100
	CodeShuttingDown         = 50300
	CodeTimeout              = 50400
	CodeStreamAborted        = 51000
	CodeChunkOverflow        = 51001
)

// ACPCodeToHTTPStatus maps an ACP error code to its HTTP status (spec §8.1;
// exceptions: 50002 -> 502, the 51xxx stream segment -> 500).
func ACPCodeToHTTPStatus(code int) int {
	class := code / 100
	if class >= 510 && class <= 519 {
		return 500
	}
	if code == CodeUpstreamError {
		return 502
	}
	if class >= 400 && class <= 599 {
		return class
	}
	return 500
}

// IsPrivateErrorCode reports whether code lies in the private/experimental
// range reserved for extensions (spec §8.2).
func IsPrivateErrorCode(code int) bool {
	return code >= 59000 && code <= 59999
}

// ACPError carries an ACP error code. Servers turn a handler-returned
// *ACPError into a failure envelope that passes the code through verbatim;
// clients surface transport error frames as *ACPError.
type ACPError struct {
	Code    int
	Message string
	Data    any
}

// Error implements the error interface.
func (e *ACPError) Error() string {
	return fmt.Sprintf("acp error %d: %s", e.Code, e.Message)
}

// NewACPError builds an *ACPError.
func NewACPError(code int, message string, data any) *ACPError {
	return &ACPError{Code: code, Message: message, Data: data}
}
