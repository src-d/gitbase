package bblfsh

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v2/driver"
	protocol2 "gopkg.in/bblfsh/sdk.v2/protocol"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

// FatalError is returned when response is returned with Fatal status code.
type FatalError []string

func (e FatalError) Error() string {
	if n := len(e); n == 0 {
		return "fatal error"
	}
	return strings.Join([]string(e), "\n")
}

// ErrPartialParse is returned when driver was not able to parse the whole source file.
type ErrPartialParse = driver.ErrPartialParse

// Mode controls the level of transformation applied to UAST.
type Mode = protocol2.Mode

const (
	Native    = protocol2.Mode_Native
	Annotated = protocol2.Mode_Annotated
	Semantic  = protocol2.Mode_Semantic
)

// Parse mode parses a UAST mode string to an enum value.
func ParseMode(mode string) (Mode, error) {
	// TODO: define this function in SDK
	switch mode {
	case "native":
		return Native, nil
	case "annotated":
		return Annotated, nil
	case "semantic":
		return Semantic, nil
	}
	return 0, fmt.Errorf("unsupported mode: %q", mode)
}

// ParseRequest is a parsing request to get the UAST.
type ParseRequest struct {
	ctx      context.Context
	internal protocol2.ParseRequest
	client   *Client
	err      error
}

// Language sets the language of the given source file to parse. if missing
// will be guess from the filename and the content.
func (r *ParseRequest) Language(language string) *ParseRequest {
	r.internal.Language = language
	return r
}

// ReadFile loads a file given a local path and sets the content and the
// filename of the request.
func (r *ParseRequest) ReadFile(fp string) *ParseRequest {
	data, err := ioutil.ReadFile(fp)
	if err != nil {
		r.err = err
	} else {
		r.internal.Content = string(data)
		r.internal.Filename = filepath.Base(fp)
	}

	return r
}

// Content sets the content of the parse request. It should be the source code
// that wants to be parsed.
func (r *ParseRequest) Content(content string) *ParseRequest {
	r.internal.Content = content
	return r
}

// Filename sets the filename of the content.
func (r *ParseRequest) Filename(filename string) *ParseRequest {
	r.internal.Filename = filename
	return r
}

// Mode controls the level of transformation applied to UAST.
func (r *ParseRequest) Mode(mode Mode) *ParseRequest {
	r.internal.Mode = mode
	return r
}

// Context sets a cancellation context for this request.
func (r *ParseRequest) Context(ctx context.Context) *ParseRequest {
	r.ctx = ctx
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfshd and waiting for the response.
func (r *ParseRequest) Do() (*protocol2.ParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.client.service2.Parse(r.ctx, &r.internal)
}

// Node is a generic UAST node.
type Node = nodes.Node

// UAST send the request and returns decoded UAST and the language.
// If a file contains syntax error, the ErrPartialParse is returned and will contain a partial AST.
func (r *ParseRequest) UAST() (Node, string, error) {
	resp, err := r.Do()
	if err != nil {
		return nil, "", err
	}
	ast, err := resp.Nodes()
	return ast, resp.Language, err
}

// VersionRequest is a request to retrieve the version of the server.
type VersionRequest struct {
	ctx    context.Context
	client *Client
	err    error
}

// Context sets a cancellation context for this request.
func (r *VersionRequest) Context(ctx context.Context) *VersionRequest {
	r.ctx = ctx
	return r
}

// VersionResponse contains information about Babelfish version.
type VersionResponse struct {
	// Version is the server version. If is a local compilation the version
	// follows the pattern dev-<short-commit>[-dirty], dirty means that was
	// compile from a repository with un-committed changes.
	Version string `json:"version"`
	// Build contains the timestamp at the time of the build.
	Build time.Time `json:"build"`
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *VersionRequest) Do() (*VersionResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service1.Version(r.ctx, &protocol1.VersionRequest{})
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return nil, FatalError(resp.Errors)
	}
	return &VersionResponse{
		Version: resp.Version,
		Build:   resp.Build,
	}, nil
}

// SupportedLanguagesRequest is a request to retrieve the supported languages.
type SupportedLanguagesRequest struct {
	ctx    context.Context
	client *Client
	err    error
}

// Context sets a cancellation context for this request.
func (r *SupportedLanguagesRequest) Context(ctx context.Context) *SupportedLanguagesRequest {
	r.ctx = ctx
	return r
}

// DriverManifest contains an information about a single Babelfish driver.
type DriverManifest = protocol1.DriverManifest

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *SupportedLanguagesRequest) Do() ([]DriverManifest, error) {
	if r.err != nil {
		return nil, r.err
	}
	resp, err := r.client.service1.SupportedLanguages(r.ctx, &protocol1.SupportedLanguagesRequest{})
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return nil, FatalError(resp.Errors)
	}
	return resp.Languages, nil
}
