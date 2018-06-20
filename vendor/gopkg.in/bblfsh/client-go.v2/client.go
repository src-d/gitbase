package bblfsh

import (
	"time"

	"google.golang.org/grpc"
	"gopkg.in/bblfsh/sdk.v1/protocol"
)

// Client holds the public client API to interact with the bblfsh daemon.
type Client struct {
	*grpc.ClientConn
	service protocol.ProtocolServiceClient
}

// NewClient returns a new bblfsh client given a bblfshd endpoint.
func NewClient(endpoint string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTimeout(5 * time.Second),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		ClientConn: conn,
		service:    protocol.NewProtocolServiceClient(conn),
	}, nil
}

// NewClientWithConnection returns a new bblfsh client given a grpc connection.
func NewClientWithConnection(conn *grpc.ClientConn) (*Client, error) {
	return &Client{
		ClientConn: conn,
		service:    protocol.NewProtocolServiceClient(conn),
	}, nil
}

// NewParseRequest is a parsing request to get the UAST.
func (c *Client) NewParseRequest() *ParseRequest {
	return &ParseRequest{client: c}
}

// NewNativeParseRequest is a parsing request to get the AST.
func (c *Client) NewNativeParseRequest() *NativeParseRequest {
	return &NativeParseRequest{client: c}
}

// NewVersionRequest is a parsing request to get the version of the server.
func (c *Client) NewVersionRequest() *VersionRequest {
	return &VersionRequest{client: c}
}

// NewSupportedLanguagesRequest is a parsing request to get the supported languages.
func (c *Client) NewSupportedLanguagesRequest() *SupportedLanguagesRequest {
	return &SupportedLanguagesRequest{client: c}
}
