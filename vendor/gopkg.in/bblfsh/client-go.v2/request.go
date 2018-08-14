package bblfsh

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"

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

// ParseRequestV2 is a parsing request to get the UAST.
type ParseRequestV2 struct {
	internal protocol2.ParseRequest
	client   *Client
	err      error
}

// Language sets the language of the given source file to parse. if missing
// will be guess from the filename and the content.
func (r *ParseRequestV2) Language(language string) *ParseRequestV2 {
	r.internal.Language = language
	return r
}

// ReadFile loads a file given a local path and sets the content and the
// filename of the request.
func (r *ParseRequestV2) ReadFile(fp string) *ParseRequestV2 {
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
func (r *ParseRequestV2) Content(content string) *ParseRequestV2 {
	r.internal.Content = content
	return r
}

// Filename sets the filename of the content.
func (r *ParseRequestV2) Filename(filename string) *ParseRequestV2 {
	r.internal.Filename = filename
	return r
}

// Mode controls the level of transformation applied to UAST.
type Mode = protocol2.Mode

const (
	Native    = protocol2.Mode_Native
	Annotated = protocol2.Mode_Annotated
	Semantic  = protocol2.Mode_Semantic
)

// Mode controls the level of transformation applied to UAST.
func (r *ParseRequestV2) Mode(mode Mode) *ParseRequestV2 {
	r.internal.Mode = mode
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfshd and waiting for the response.
func (r *ParseRequestV2) Do() (*protocol2.ParseResponse, error) {
	return r.DoContext(context.Background())
}

// DoContext does the same as Do(), but supports cancellation by the use of Go contexts.
func (r *ParseRequestV2) DoContext(ctx context.Context) (*protocol2.ParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service2.Parse(ctx, &r.internal)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// UAST is the same as UASTContext, but uses context.Background as a context.
func (r *ParseRequestV2) UAST() (nodes.Node, string, error) {
	return r.UASTContext(context.Background())
}

// UASTContext send the request and returns decoded UAST and the language.
// If a file contains syntax error, the
func (r *ParseRequestV2) UASTContext(ctx context.Context) (nodes.Node, string, error) {
	if r.err != nil {
		return nil, "", r.err
	}
	resp, err := r.client.service2.Parse(ctx, &r.internal)
	if err != nil {
		return nil, "", err
	}
	ast, err := resp.Nodes()
	if err != nil {
		return nil, resp.Language, err
	}
	return ast, resp.Language, nil
}

// ParseRequest is a parsing request to get the UAST.
type ParseRequest struct {
	internal protocol1.ParseRequest
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

// Encoding sets the text encoding of the content.
func (r *ParseRequest) Encoding(encoding protocol1.Encoding) *ParseRequest {
	r.internal.Encoding = encoding
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfshd and waiting for the response.
func (r *ParseRequest) Do() (*protocol1.ParseResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *ParseRequest) DoWithContext(ctx context.Context) (*protocol1.ParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service1.Parse(ctx, &r.internal)
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return resp, FatalError(resp.Errors)
	}
	return resp, nil
}

// NativeParseRequest is a parsing request to get the AST.
type NativeParseRequest struct {
	internal protocol1.NativeParseRequest
	client   *Client
	err      error
}

// Language sets the language of the given source file to parse. if missing
// will be guess from the filename and the content.
func (r *NativeParseRequest) Language(language string) *NativeParseRequest {
	r.internal.Language = language
	return r
}

// ReadFile loads a file given a local path and sets the content and the
// filename of the request.
func (r *NativeParseRequest) ReadFile(fp string) *NativeParseRequest {
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
func (r *NativeParseRequest) Content(content string) *NativeParseRequest {
	r.internal.Content = content
	return r
}

// Filename sets the filename of the content.
func (r *NativeParseRequest) Filename(filename string) *NativeParseRequest {
	r.internal.Filename = filename
	return r
}

// Encoding sets the text encoding of the content.
func (r *NativeParseRequest) Encoding(encoding protocol1.Encoding) *NativeParseRequest {
	r.internal.Encoding = encoding
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *NativeParseRequest) Do() (*protocol1.NativeParseResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *NativeParseRequest) DoWithContext(ctx context.Context) (*protocol1.NativeParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service1.NativeParse(ctx, &r.internal)
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return resp, FatalError(resp.Errors)
	}
	return resp, nil
}

// VersionRequest is a request to retrieve the version of the server.
type VersionRequest struct {
	client *Client
	err    error
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *VersionRequest) Do() (*protocol1.VersionResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *VersionRequest) DoWithContext(ctx context.Context) (*protocol1.VersionResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service1.Version(ctx, &protocol1.VersionRequest{})
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return resp, FatalError(resp.Errors)
	}
	return resp, nil
}

// SupportedLanguagesRequest is a request to retrieve the supported languages.
type SupportedLanguagesRequest struct {
	client *Client
	err    error
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *SupportedLanguagesRequest) Do() (*protocol1.SupportedLanguagesResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *SupportedLanguagesRequest) DoWithContext(ctx context.Context) (*protocol1.SupportedLanguagesResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	resp, err := r.client.service1.SupportedLanguages(ctx, &protocol1.SupportedLanguagesRequest{})
	if err != nil {
		return nil, err
	} else if resp.Status == protocol1.Fatal {
		return resp, FatalError(resp.Errors)
	}
	return resp, nil
}
