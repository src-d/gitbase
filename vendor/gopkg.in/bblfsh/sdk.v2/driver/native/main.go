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

func errToStrings(err error) []string {
	if e, ok := err.(*driver.ErrMulti); ok {
		str := make([]string, 0, len(e.Errors))
		for _, e := range e.Errors {
			str = append(str, e.Error())
		}
		return str
	}
	return []string{err.Error()}
}

type nativeServer struct {
	d driver.Native
}

func (s *nativeServer) parse(ctx context.Context, req *parseRequest) *parseResponse {
	src, err := req.Encoding.Decode(req.Content)
	if err != nil {
		return &parseResponse{
			Status: statusFatal,
			Errors: errToStrings(err),
		}
	}
	ast, err := s.d.Parse(ctx, src)
	if driver.ErrDriverFailure.Is(err) {
		return &parseResponse{
			Status: statusFatal,
			Errors: errToStrings(err),
		}
	}
	if err != nil {
		return &parseResponse{
			Status: statusError,
			AST:    ast, Errors: errToStrings(err),
		}
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
			resp := &parseResponse{
				Status: statusFatal,
				Errors: []string{fmt.Sprintf("failed to decode request: %v", err)},
			}
			if err = enc.Encode(resp); err != nil {
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
