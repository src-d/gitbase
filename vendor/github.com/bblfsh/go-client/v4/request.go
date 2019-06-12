package bblfsh

import (
	"context"
	"errors"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/bblfsh/sdk/v3/driver"
	derrors "github.com/bblfsh/sdk/v3/driver/errors"
	"github.com/bblfsh/sdk/v3/driver/manifest"
	protocol2 "github.com/bblfsh/sdk/v3/protocol"
	"github.com/bblfsh/sdk/v3/uast/nodes"
	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
)

var (
	// ErrDriverFailure is returned when the driver is malfunctioning.
	ErrDriverFailure = derrors.ErrDriverFailure

	// ErrSyntax is returned when driver cannot parse the source file.
	// Can be omitted for native driver implementations.
	ErrSyntax = derrors.ErrSyntax
)

func errorStrings(str []string) error {
	errs := make([]error, 0, len(str))
	for _, e := range str {
		errs = append(errs, errors.New(e))
	}
	return derrors.Join(errs)
}

// Mode controls the level of transformation applied to UAST.
type Mode = protocol2.Mode

const (
	Native    = protocol2.Mode_Native
	Annotated = protocol2.Mode_Annotated
	Semantic  = protocol2.Mode_Semantic
)

// Parse mode parses a UAST mode string to an enum value.
func ParseMode(mode string) (Mode, error) {
	m, err := driver.ParseMode(mode)
	if err != nil {
		return 0, err
	}
	return Mode(m), nil
}

// ParseRequest is a parsing request to get the UAST.
type ParseRequest struct {
	ctx     context.Context
	content string
	options driver.ParseOptions
	client  *Client
	err     error
}

// Language sets the language of the given source file to parse. if missing
// will be guess from the filename and the content.
func (r *ParseRequest) Language(language string) *ParseRequest {
	r.options.Language = language
	return r
}

// ReadFile loads a file given a local path and sets the content and the
// filename of the request.
func (r *ParseRequest) ReadFile(fp string) *ParseRequest {
	data, err := ioutil.ReadFile(fp)
	if err != nil {
		r.err = err
	} else {
		r.content = string(data)
		r.options.Filename = filepath.Base(fp)
	}

	return r
}

// Content sets the content of the parse request. It should be the source code
// that wants to be parsed.
func (r *ParseRequest) Content(content string) *ParseRequest {
	r.content = content
	return r
}

// Filename sets the filename of the content.
func (r *ParseRequest) Filename(filename string) *ParseRequest {
	r.options.Filename = filename
	return r
}

// Mode controls the level of transformation applied to UAST.
func (r *ParseRequest) Mode(mode Mode) *ParseRequest {
	r.options.Mode = driver.Mode(mode)
	return r
}

// Context sets a cancellation context for this request.
func (r *ParseRequest) Context(ctx context.Context) *ParseRequest {
	r.ctx = ctx
	return r
}

// Do performs the actual parsing by serializing the request, sending it to
// bblfshd and waiting for the response.
//
// It's the caller's responsibility to interpret errors properly.
//
// Deprecated: use UAST() instead
func (r *ParseRequest) Do() (*protocol2.ParseResponse, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.client.driver2.Parse(r.ctx, &protocol2.ParseRequest{
		Content:  r.content,
		Mode:     protocol2.Mode(r.options.Mode),
		Language: r.options.Language,
		Filename: r.options.Filename,
	})
}

// Node is a generic UAST node.
type Node = nodes.Node

// UAST send the request and returns decoded UAST and the language.
//
// If a file contains syntax error, the ErrSyntax is returned and the UAST may be nil or partial in this case.
//
// ErrDriverFailure is returned if the native driver is malfunctioning.
func (r *ParseRequest) UAST() (Node, string, error) {
	ast, err := r.client.driver.Parse(r.ctx, r.content, &r.options)
	return ast, r.options.Language, err
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
	resp, err := r.client.driver.Version(r.ctx)
	if err != nil {
		return nil, err
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
//
// Deprecated: see DoV2.
type DriverManifest = protocol1.DriverManifest

// Do performs the supported languages request and return information about available drivers.
//
// Deprecated: use DoV2 instead.
func (r *SupportedLanguagesRequest) Do() ([]DriverManifest, error) {
	if r.err != nil {
		return nil, r.err
	}
	list, err := r.client.driver.Languages(r.ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DriverManifest, 0, len(list))
	for _, m := range list {
		dm := DriverManifest{
			Name:     m.Name,
			Language: m.Language,
			Version:  m.Version,
			Status:   string(m.Status),
			Features: make([]string, 0, len(m.Features)),
		}
		for _, f := range m.Features {
			dm.Features = append(dm.Features, string(f))
		}
		out = append(out, dm)
	}
	return out, nil
}

// DriverManifestV2 contains an information about a single Babelfish driver.
type DriverManifestV2 = manifest.Manifest

// DoV2 performs the supported languages request and return information about available drivers.
func (r *SupportedLanguagesRequest) DoV2() ([]DriverManifestV2, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.client.driver.Languages(r.ctx)
}
