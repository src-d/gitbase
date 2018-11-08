package bblfsh

import (
	"context"
	"time"

	"gopkg.in/bblfsh/sdk.v2/driver"

	"google.golang.org/grpc"
	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
	protocol2 "gopkg.in/bblfsh/sdk.v2/protocol"
)

// Client holds the public client API to interact with the bblfsh daemon.
type Client struct {
	*grpc.ClientConn
	service1 protocol1.ProtocolServiceClient
	service2 protocol2.DriverClient
	driver2  driver.Driver
}

// NewClientContext returns a new bblfsh client given a bblfshd endpoint.
func NewClientContext(ctx context.Context, endpoint string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
	}

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return NewClientWithConnection(conn)
}

// NewClient is the same as NewClientContext, but assumes a default timeout for the connection.
func NewClient(endpoint string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return NewClientContext(ctx, endpoint)
}

// NewClientWithConnection returns a new bblfsh client given a grpc connection.
func NewClientWithConnection(conn *grpc.ClientConn) (*Client, error) {
	return &Client{
		ClientConn: conn,
		service1:   protocol1.NewProtocolServiceClient(conn),
		service2:   protocol2.NewDriverClient(conn),
		driver2:    protocol2.AsDriver(conn),
	}, nil
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
