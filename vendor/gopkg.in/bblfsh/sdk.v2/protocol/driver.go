package protocol

import (
	"bytes"
	"context"

	xcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
)

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=plugins=grpc:. ./driver.proto

func RegisterDriver(srv *grpc.Server, d driver.Driver) {
	RegisterDriverServer(srv, &driverServer{d: d})
}

func AsDriver(cc *grpc.ClientConn, lang string) driver.Driver {
	return &client{c: NewDriverClient(cc), lang: lang}
}

type driverServer struct {
	d driver.Driver
}

func (s *driverServer) Parse(ctx xcontext.Context, req *ParseRequest) (*ParseResponse, error) {
	var resp ParseResponse
	n, err := s.d.Parse(ctx, driver.Mode(req.Mode), req.Content)
	if e, ok := err.(*driver.ErrPartialParse); ok {
		n = e.AST
		for _, txt := range e.Errors {
			resp.Errors = append(resp.Errors, &ParseError{Text: txt})
		}
	} else if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	err = nodesproto.WriteTo(buf, n)
	if err != nil {
		return nil, err
	}
	resp.Uast = buf.Bytes()
	return &resp, nil
}

type client struct {
	c    DriverClient
	lang string
}

func (c *client) Parse(ctx context.Context, mode driver.Mode, src string) (nodes.Node, error) {
	resp, err := c.c.Parse(ctx, &ParseRequest{Content: src, Mode: Mode(mode), Language: c.lang})
	if err != nil {
		return nil, err
	}
	return resp.Nodes()
}

func (m *ParseResponse) Nodes() (nodes.Node, error) {
	ast, err := nodesproto.ReadTree(bytes.NewReader(m.Uast))
	if err != nil {
		return nil, err
	}
	if len(m.Errors) != 0 {
		var errs []string
		for _, e := range m.Errors {
			errs = append(errs, e.Text)
		}
		return nil, &driver.ErrPartialParse{AST: ast, ErrMulti: driver.ErrMulti{Errors: errs}}
	}
	return ast, nil
}
