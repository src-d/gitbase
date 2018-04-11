package bblfsh

import (
	"context"
	"io/ioutil"
	"path"

	"gopkg.in/bblfsh/sdk.v1/protocol"
)

// ParseRequest is a parsing request to get the UAST.
type ParseRequest struct {
	internal protocol.ParseRequest
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
func (r *ParseRequest) ReadFile(filepath string) *ParseRequest {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		r.err = err
	} else {
		r.internal.Content = string(data)
		r.internal.Filename = path.Base(filepath)
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
func (r *ParseRequest) Encoding(encoding protocol.Encoding) *ParseRequest {
	r.internal.Encoding = encoding
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfshd and waiting for the response.
func (r *ParseRequest) Do() (*protocol.ParseResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *ParseRequest) DoWithContext(ctx context.Context) (*protocol.ParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	return r.client.service.Parse(ctx, &r.internal)
}

// NativeParseRequest is a parsing request to get the AST.
type NativeParseRequest struct {
	internal protocol.NativeParseRequest
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
func (r *NativeParseRequest) ReadFile(filepath string) *NativeParseRequest {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		r.err = err
	} else {
		r.internal.Content = string(data)
		r.internal.Filename = path.Base(filepath)
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
func (r *NativeParseRequest) Encoding(encoding protocol.Encoding) *NativeParseRequest {
	r.internal.Encoding = encoding
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *NativeParseRequest) Do() (*protocol.NativeParseResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *NativeParseRequest) DoWithContext(ctx context.Context) (*protocol.NativeParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	return r.client.service.NativeParse(ctx, &r.internal)
}

// VersionRequest is a request to retrieve the version of the server.
type VersionRequest struct {
	client *Client
	err    error
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfsd and waiting for the response.
func (r *VersionRequest) Do() (*protocol.VersionResponse, error) {
	return r.DoWithContext(context.Background())
}

// DoWithContext does the same as Do(), but sopporting cancellation by the use
// of Go contexts.
func (r *VersionRequest) DoWithContext(ctx context.Context) (*protocol.VersionResponse, error) {
	if r.err != nil {
		return nil, r.err
	}

	return r.client.service.Version(ctx, &protocol.VersionRequest{})
}
