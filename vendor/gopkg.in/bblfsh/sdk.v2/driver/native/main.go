package native

import (
	"context"
	"fmt"
	"io"
	"os"

	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/bblfsh/sdk.v2/driver/native/jsonlines"
)

// Main is a main function for running a native Go driver as an Exec-based module that uses internal json protocol.
func Main(d driver.Native) {
	if err := d.Start(); err != nil {
		panic(err)
	}
	defer d.Close()
	srv := &nativeServer{d: d}
	c := struct {
		io.Reader
		io.Writer
	}{
		os.Stdin,
		os.Stdout,
	}
	if err := srv.Serve(c); err != nil {
		panic(err)
	}
}

type nativeServer struct {
	d driver.Native
}

func errResp(err error) *parseResponse {
	if e, ok := err.(*driver.ErrPartialParse); ok {
		return &parseResponse{
			Status: statusError,
			AST:    e.AST, Errors: e.Errors,
		}
	}
	return &parseResponse{
		Status: statusFatal,
		Errors: []string{err.Error()},
	}
}

func errRespf(format string, args ...interface{}) *parseResponse {
	return errResp(fmt.Errorf(format, args...))
}

func (s *nativeServer) parse(ctx context.Context, req *parseRequest) *parseResponse {
	src, err := req.Encoding.Decode(req.Content)
	if err != nil {
		return errResp(err)
	}
	ast, err := s.d.Parse(ctx, src)
	if err != nil {
		return errResp(err)
	}
	return &parseResponse{Status: statusOK, AST: ast}
}

func (s *nativeServer) Serve(c io.ReadWriter) error {
	ctx := context.Background()
	enc := jsonlines.NewEncoder(c)
	dec := jsonlines.NewDecoder(c)
	for {
		var req parseRequest
		err := dec.Decode(&req)
		if err == io.EOF {
			return nil
		} else if err != nil {
			if err = enc.Encode(errRespf("failed to decode request: %v", err)); err != nil {
				return err
			}
			continue
		}
		resp := s.parse(ctx, &req)
		if err = enc.Encode(resp); err != nil {
			return err
		}
	}
}
