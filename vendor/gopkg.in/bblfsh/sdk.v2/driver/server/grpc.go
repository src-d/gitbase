package server

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc"
	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
	protocol2 "gopkg.in/bblfsh/sdk.v2/protocol"
	"gopkg.in/bblfsh/sdk.v2/protocol/v1"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

// NewGRPCServer creates a gRPC server instance that dispatches requests to a provided driver.
//
// It will automatically include default server options for bblfsh protocol.
func NewGRPCServer(drv driver.DriverModule, opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts, protocol2.ServerOptions()...)
	return NewGRPCServerCustom(drv, opts...)
}

// NewGRPCServerCustom is the same as NewGRPCServer, but it won't include any options except the ones that were passed.
func NewGRPCServerCustom(drv driver.DriverModule, opts ...grpc.ServerOption) *grpc.Server {
	srv := grpc.NewServer(opts...)

	protocol1.DefaultService = service{drv}
	protocol1.RegisterProtocolServiceServer(
		srv,
		protocol1.NewProtocolServiceServer(),
	)
	protocol2.RegisterDriver(srv, drv)

	return srv
}

type service struct {
	d driver.DriverModule
}

func errResp(err error) protocol1.Response {
	return protocol1.Response{Status: protocol1.Fatal, Errors: []string{err.Error()}}
}

func newDriverManifest(manifest *manifest.Manifest) protocol1.DriverManifest {
	features := make([]string, len(manifest.Features))
	for i, feature := range manifest.Features {
		features[i] = string(feature)
	}
	return protocol1.DriverManifest{
		Name:     manifest.Name,
		Language: manifest.Language,
		Version:  manifest.Version,
		Status:   string(manifest.Status),
		Features: features,
	}
}

func (s service) SupportedLanguages(_ *protocol1.SupportedLanguagesRequest) *protocol1.SupportedLanguagesResponse {
	m, _ := s.d.Manifest()
	return &protocol1.SupportedLanguagesResponse{Languages: []protocol1.DriverManifest{
		newDriverManifest(&m),
	}}
}

func (s service) parse(mode driver.Mode, req *protocol1.ParseRequest) (nodes.Node, protocol1.Response) {
	start := time.Now()
	m, err := s.d.Manifest()
	if err != nil {
		r := errResp(err)
		r.Elapsed = time.Since(start)
		return nil, r
	}
	if req.Language != m.Language {
		r := errResp(ErrUnsupportedLanguage.New(req.Language))
		r.Elapsed = time.Since(start)
		return nil, r
	}
	ctx := context.Background()
	if req.Timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	ast, err := s.d.Parse(ctx, req.Content, &driver.ParseOptions{
		Mode:     mode,
		Language: req.Language,
		Filename: req.Filename,
	})
	dt := time.Since(start)
	var r protocol1.Response
	if err != nil {
		r = errResp(err)
	} else {
		r = protocol1.Response{Status: protocol1.Ok}
	}
	r.Elapsed = dt
	return ast, r
}

func (s service) Parse(req *protocol1.ParseRequest) *protocol1.ParseResponse {
	ast, resp := s.parse(driver.ModeAnnotated, req)
	if resp.Status != protocol1.Ok {
		return &protocol1.ParseResponse{Response: resp}
	}
	nd, err := uast1.ToNode(ast)
	if err != nil {
		r := errResp(err)
		r.Elapsed = resp.Elapsed
		return &protocol1.ParseResponse{Response: r}
	}
	return &protocol1.ParseResponse{
		Response: resp,
		Language: req.Language,
		Filename: req.Filename,
		UAST:     nd,
	}
}

func (s service) NativeParse(req *protocol1.NativeParseRequest) *protocol1.NativeParseResponse {
	ast, resp := s.parse(driver.ModeNative, (*protocol1.ParseRequest)(req))
	if resp.Status != protocol1.Ok {
		return &protocol1.NativeParseResponse{Response: resp}
	}
	data, err := json.Marshal(ast)
	if err != nil {
		r := errResp(err)
		r.Elapsed = resp.Elapsed
		return &protocol1.NativeParseResponse{Response: r}
	}
	return &protocol1.NativeParseResponse{
		Response: resp,
		Language: req.Language,
		AST:      string(data),
	}
}

func (s service) Version(req *protocol1.VersionRequest) *protocol1.VersionResponse {
	m, _ := s.d.Manifest()

	r := &protocol1.VersionResponse{
		Version: m.Version,
	}
	if m.Build != nil {
		r.Build = *m.Build
	}
	return r
}
