package protocol_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"gopkg.in/bblfsh/sdk.v1/protocol"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestInvalidParser(t *testing.T) {
	require := require.New(t)

	protocol.DefaultService = nil
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)

	server := grpc.NewServer()
	protocol.RegisterProtocolServiceServer(
		server,
		protocol.NewProtocolServiceServer(),
	)

	go server.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTimeout(time.Second*2), grpc.WithInsecure())
	require.NoError(err)

	client := protocol.NewProtocolServiceClient(conn)

	ureq := &protocol.ParseRequest{
		Content: "my source code",
	}
	uresp, err := client.Parse(context.TODO(), ureq)
	require.NoError(err)
	require.Equal(protocol.Fatal, uresp.Status)

	server.GracefulStop()
}

func Example() {
	protocol.DefaultService = NewServiceMock()

	lis, err := net.Listen("tcp", "localhost:0")
	checkError(err)

	server := grpc.NewServer()
	protocol.RegisterProtocolServiceServer(
		server,
		protocol.NewProtocolServiceServer(),
	)

	go server.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTimeout(time.Second*2), grpc.WithInsecure())
	checkError(err)

	client := protocol.NewProtocolServiceClient(conn)

	req := &protocol.ParseRequest{Content: "my source code"}
	fmt.Println("Sending Parse for:", req.Content)

	resp, err := client.Parse(context.TODO(), req)
	checkError(err)
	fmt.Println("Got response with status:", resp.Status)

	server.GracefulStop()

	//Output: Sending Parse for: my source code
	// Got response with status: Ok
}

// do a reply to a mock server that except the encoding to be protocol.Base64 or
// returns error. If the encoding is -1, no Encoding field will the added to the
// request.
func doEncodingTesterRequest(intEncoding int) (resp *protocol.ParseResponse,
	server *grpc.Server, err error) {

	// Use a mock parser on the server.
	protocol.DefaultService = &ServiceMock{
		P: func(req *protocol.ParseRequest) *protocol.ParseResponse {
			r := &protocol.ParseResponse{}
			r.Status = protocol.Ok

			if req.Encoding != protocol.Base64 {
				r.Status = protocol.Error
				return r
			}

			return r
		},
	}

	server = grpc.NewServer()
	protocol.RegisterProtocolServiceServer(
		server,
		protocol.NewProtocolServiceServer(),
	)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		checkError(err)
	}

	go server.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTimeout(time.Second*2), grpc.WithInsecure())
	if err != nil {
		return
	}
	client := protocol.NewProtocolServiceClient(conn)

	var typedEncoding protocol.Encoding
	var req *protocol.ParseRequest

	if intEncoding >= 0 {
		// mockCheckBase64EncodingParser will return Ok if the Encoding is Base64
		typedEncoding = protocol.Encoding(intEncoding)

		req = &protocol.ParseRequest{
			Content:  "some source code",
			Encoding: typedEncoding,
		}
	} else {
		// Request without encoding
		req = &protocol.ParseRequest{
			Content: "some source code",
		}
	}

	resp, err = client.Parse(context.TODO(), req)
	if err != nil {
		return
	}
	return
}

func TestEncodingFieldBase64(t *testing.T) {
	require := require.New(t)

	resp, server, err := doEncodingTesterRequest(int(protocol.Base64))
	checkError(err)
	require.Equal(resp.Status, protocol.Ok)

	server.GracefulStop()
}

func TestEncodingFieldUTF8(t *testing.T) {
	require := require.New(t)

	resp, server, err := doEncodingTesterRequest(int(protocol.UTF8))
	checkError(err)
	// the mock encoder test server returns error on encoding == UTF8
	require.Equal(resp.Status, protocol.Error)

	server.GracefulStop()
}

func TestEncodingFieldEmptyIsUTF8(t *testing.T) {
	require := require.New(t)

	resp, server, err := doEncodingTesterRequest(-1)
	checkError(err)
	// the mock encoder test server returns error on != Base64, default should be UTF8
	require.Equal(resp.Status, protocol.Error)

	server.GracefulStop()
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

// ServiceMock implements the protocol.Servce interface and the methods to be
// used in the test.
type ServiceMock struct {
	P func(req *protocol.ParseRequest) *protocol.ParseResponse
	N func(req *protocol.NativeParseRequest) *protocol.NativeParseResponse
	V func(*protocol.VersionRequest) *protocol.VersionResponse
}

func NewServiceMock() *ServiceMock {
	return &ServiceMock{
		P: func(req *protocol.ParseRequest) *protocol.ParseResponse {
			return &protocol.ParseResponse{
				Response: protocol.Response{Status: protocol.Ok},
			}
		},

		N: func(req *protocol.NativeParseRequest) *protocol.NativeParseResponse {
			return &protocol.NativeParseResponse{
				Response: protocol.Response{Status: protocol.Ok},
			}
		},
	}
}

func (m *ServiceMock) Parse(req *protocol.ParseRequest) *protocol.ParseResponse {
	return m.P(req)
}

func (m *ServiceMock) NativeParse(req *protocol.NativeParseRequest) *protocol.NativeParseResponse {
	return m.N(req)
}

func (m *ServiceMock) Version(req *protocol.VersionRequest) *protocol.VersionResponse {
	return m.V(req)
}
