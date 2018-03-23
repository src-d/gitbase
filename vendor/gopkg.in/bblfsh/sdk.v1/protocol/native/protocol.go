package native

import (
	"time"

	"gopkg.in/bblfsh/sdk.v1/protocol"
)

// ParseRequest to use with the native parser. This is for internal use.
type ParseRequest protocol.ParseRequest

// ParseResponse is the reply to ParseRequest by the native parser.
type ParseResponse struct {
	// Status is the status of the parsing request.
	Status protocol.Status `json:"status"`
	// Status is the status of the parsing request.
	Errors []string `json:"errors"`
	// AST contains the AST from the parsed code.
	AST interface{} `json:"ast"`
	// Elapsed is the amount of time consume processing the request.
	Elapsed time.Duration `json:"elapsed"`
	// Language is the parsed language
	Language string `json:"language"`
}
