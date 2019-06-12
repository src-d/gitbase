package bblfsh

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bblfsh/sdk/v3/driver"
	"github.com/bblfsh/sdk/v3/driver/manifest"
	protocol2 "github.com/bblfsh/sdk/v3/protocol"
	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
)

// Client holds the public client API to interact with the bblfsh daemon.
type Client struct {
	*grpc.ClientConn
	driver2 protocol2.DriverClient
	driver  driver.Driver
}

// NewClientContext returns a new bblfsh client given a bblfshd endpoint.
func NewClientContext(ctx context.Context, endpoint string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
	}
	opts = append(opts, protocol2.DialOptions()...)

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return NewClientWithConnectionContext(ctx, conn)
}

// NewClient is the same as NewClientContext, but assumes a default timeout for the connection.
//
// Deprecated: use NewClientContext instead
func NewClient(endpoint string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return NewClientContext(ctx, endpoint)
}

// NewClientWithConnection returns a new bblfsh client given a grpc connection.
func NewClientWithConnection(conn *grpc.ClientConn) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return NewClientWithConnectionContext(ctx, conn)
}

func isServiceNotSupported(err error) bool {
	if err == nil {
		return false
	}
	return status.Code(err) == codes.Unimplemented
}

// NewClientWithConnectionContext returns a new bblfsh client given a grpc connection.
func NewClientWithConnectionContext(ctx context.Context, conn *grpc.ClientConn) (*Client, error) {
	host := protocol2.NewDriverHostClient(conn)
	_, err := host.ServerVersion(ctx, &protocol2.VersionRequest{})
	if err == nil {
		// supports v2
		return &Client{
			ClientConn: conn,
			driver2:    protocol2.NewDriverClient(conn),
			driver:     protocol2.AsDriver(conn),
		}, nil
	} else if !isServiceNotSupported(err) {
		return nil, err
	}
	s1 := protocol1.NewProtocolServiceClient(conn)
	return &Client{
		ClientConn: conn,
		driver2:    protocol2.NewDriverClient(conn),
		driver: &driverPartialV2{
			// use only Parse from v2
			Driver: protocol2.AsDriver(conn),
			// use v1 for version and supported languages
			service1: s1,
		},
	}, nil
}

type driverPartialV2 struct {
	driver.Driver
	service1 protocol1.ProtocolServiceClient
}

// Version implements a driver.Host using v1 protocol.
func (d *driverPartialV2) Version(ctx context.Context) (driver.Version, error) {
	resp, err := d.service1.Version(ctx, &protocol1.VersionRequest{})
	if err != nil {
		return driver.Version{}, err
	} else if resp.Status != protocol1.Ok {
		return driver.Version{}, errorStrings(resp.Errors)
	}
	return driver.Version{
		Version: resp.Version,
		Build:   resp.Build,
	}, nil
}

// Languages implements a driver.Host using v1 protocol.
func (d *driverPartialV2) Languages(ctx context.Context) ([]manifest.Manifest, error) {
	resp, err := d.service1.SupportedLanguages(ctx, &protocol1.SupportedLanguagesRequest{})
	if err != nil {
		return nil, err
	} else if resp.Status != protocol1.Ok {
		return nil, errorStrings(resp.Errors)
	}
	out := make([]manifest.Manifest, 0, len(resp.Languages))
	for _, m := range resp.Languages {
		dm := manifest.Manifest{
			Name:     m.Name,
			Language: m.Language,
			Version:  m.Version,
			Status:   manifest.DevelopmentStatus(m.Status),
			Features: make([]manifest.Feature, 0, len(m.Features)),
		}
		for _, f := range m.Features {
			dm.Features = append(dm.Features, manifest.Feature(f))
		}
		out = append(out, dm)
	}
	return out, nil
}

// NewParseRequest is a parsing request to get the UAST.
func (c *Client) NewParseRequest() *ParseRequest {
	return &ParseRequest{ctx: context.Background(), client: c}
}

// NewVersionRequest is a parsing request to get the version of the server.
func (c *Client) NewVersionRequest() *VersionRequest {
	return &VersionRequest{ctx: context.Background(), client: c}
}

// NewSupportedLanguagesRequest is a parsing request to get the supported languages.
func (c *Client) NewSupportedLanguagesRequest() *SupportedLanguagesRequest {
	return &SupportedLanguagesRequest{ctx: context.Background(), client: c}
}
