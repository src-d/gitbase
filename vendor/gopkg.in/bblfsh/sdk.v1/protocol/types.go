// Copyright 2017 Sourced Technologies SL
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//go:generate proteus  -f $GOPATH/src -p gopkg.in/bblfsh/sdk.v1/protocol -p gopkg.in/bblfsh/sdk.v1/uast
//go:generate stringer -type=Status,Encoding -output stringer.go

package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/bblfsh/sdk.v1/manifest"
	"gopkg.in/bblfsh/sdk.v1/uast"
)

// DefaultService is the default service used to process requests.
var DefaultService Service

// Service can parse code to UAST or AST.
type Service interface {
	Parse(*ParseRequest) *ParseResponse
	NativeParse(*NativeParseRequest) *NativeParseResponse
	Version(*VersionRequest) *VersionResponse
	SupportedLanguages(*SupportedLanguagesRequest) *SupportedLanguagesResponse
}

// Status is the status of a response.
//proteus:generate
type Status byte

const (
	// Ok status code.
	Ok Status = iota
	// Error status code. It is replied when the driver has got the AST with errors.
	Error
	// Fatal status code. It is replied when the driver hasn't could get the AST.
	Fatal
)

// Encoding is the encoding used for the content string. Currently only
// UTF-8 or Base64 encodings are supported. You should use UTF-8 if you can
// and Base64 as a fallback.
//proteus:generate
type Encoding byte

const (
	// UTF8 encoding
	UTF8 Encoding = iota
	// Base64 encoding
	Base64
)

// Response is a basic response, never used directly. The Response.Status field should always be
// checked to be protocol.Ok before further processing.
type Response struct {
	// Status is the status of the parsing request.
	Status Status `json:"status"`
	// Status is the status of the parsing request.
	Errors []string `json:"errors"`
	// Elapsed is the amount of time consume processing the request.
	Elapsed time.Duration `json:"elapsed"`
}

// ParseRequest is a request to parse a file and get its UAST.
//proteus:generate
type ParseRequest struct {
	// Filename is the name of the file containing the source code. Used for
	// language detection. Only filename is required, path might be used but
	// ignored. This is optional.
	Filename string `json:"filename"`
	// Language. If specified, it will override language detection. This is
	// optional.
	Language string `json:"language"`
	// Content is the source code to be parsed.
	Content string `json:"content"`
	// Encoding is the encoding that the Content uses. Currently only UTF-8 and
	// Base64 are supported.
	Encoding Encoding `json:"encoding"`
	// Timeout amount of time for wait until the request is proccessed.
	Timeout time.Duration `json:"timeout"`
}

// ParseResponse is the reply to ParseRequest. The Response.Status field should always
// be checked to be protocol.Ok before further processing.
//proteus:generate
type ParseResponse struct {
	Response
	// UAST contains the UAST from the parsed code.
	UAST *uast.Node `json:"uast"`
	// Language. The language that was parsed. Usedful if you used language
	// autodetection for the request.
	Language string `json:"language"`
	// Filename is the name of the file containing the source code. Used for
	// language detection. Only filename is required, path might be used but
	// ignored. This is optional.
	Filename string `json:"filename"`
}

func (r *ParseResponse) String() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Status: ", strings.ToLower(r.Status.String()))
	fmt.Fprintln(buf, "Language: ", strings.ToLower(r.Language))
	if len(r.Filename) > 0 {
		fmt.Fprintln(buf, "Filename:: ", strings.ToLower(r.Filename))
	}
	fmt.Fprintln(buf, "Errors: ")
	for _, err := range r.Errors {
		fmt.Fprintln(buf, " . ", err)
	}

	fmt.Fprintln(buf, "UAST: ")
	fmt.Fprintln(buf, r.UAST.String())

	return buf.String()
}

// NativeParseRequest is a request to parse a file and get its native AST.
//proteus:generate
type NativeParseRequest ParseRequest

// NativeParseResponse is the reply to NativeParseRequest by the native parser.
//proteus:generate
type NativeParseResponse struct {
	Response
	// AST contains the AST from the parsed code in json format.
	AST string `json:"ast"`
	// Language. The language that was parsed. Usedful if you used language
	// autodetection for the request.
	Language string `json:"language"`
}

func (r *NativeParseResponse) String() string {
	var s struct {
		Status   string      `json:"status"`
		Language string      `json:"language"`
		Errors   []string    `json:"errors"`
		AST      interface{} `json:"ast"`
	}

	s.Status = strings.ToLower(r.Status.String())
	s.Language = strings.ToLower(r.Language)
	s.Errors = r.Errors
	if len(s.Errors) == 0 {
		s.Errors = make([]string, 0)
	}

	if len(r.AST) > 0 {
		err := json.Unmarshal([]byte(r.AST), &s.AST)
		if err != nil {
			return err.Error()
		}
	}

	buf := bytes.NewBuffer(nil)
	e := json.NewEncoder(buf)
	e.SetIndent("", "    ")
	e.SetEscapeHTML(false)

	err := e.Encode(s)
	if err != nil {
		return err.Error()
	}

	return buf.String()
}

// VersionRequest is a request to get server version
//proteus:generate
type VersionRequest struct{}

// VersionResponse is the reply to VersionRequest
//proteus:generate
type VersionResponse struct {
	Response
	// Version is the server version. If is a local compilation the version
	// follows the pattern dev-<short-commit>[-dirty], dirty means that was
	// compile from a repository with un-committed changes.
	Version string `json:"version"`
	// Build contains the timestamp at the time of the build.
	Build time.Time `json:"build"`
}

// SupportedLanguagesRequest is a request to get the supported languages
//proteus:generate
type SupportedLanguagesRequest struct{}

// SupportedLanguagesResponse is the reply to SupportedLanguagesRequest
//proteus:generate
type SupportedLanguagesResponse struct {
	Response
	// Languages contains the details of the supported languages
	Languages []DriverManifest `json:"drivers"`
}

// DriverManifest is the installed driver exported data
//proteus:generate
type DriverManifest struct {
	Name     string   `json:"name"`
	Language string   `json:"language"`
	Version  string   `json:"version"`
	Status   string   `json:"status"`
	Features []string `json:"features"`
}

// NewDriverManifest returns a DriverManifest from a Manifest
func NewDriverManifest(manifest *manifest.Manifest) DriverManifest {
	features := make([]string, len(manifest.Features))
	for i, feature := range manifest.Features {
		features[i] = string(feature)
	}

	return DriverManifest{
		Name:     manifest.Name,
		Language: manifest.Language,
		Version:  manifest.Version,
		Status:   string(manifest.Status),
		Features: features,
	}
}
