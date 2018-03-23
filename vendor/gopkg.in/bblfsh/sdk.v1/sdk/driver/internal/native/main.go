package main

import (
	"io"
	"os"

	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/sdk/driver"
	"gopkg.in/bblfsh/sdk.v1/sdk/jsonlines"
)

func main() {
	dec := jsonlines.NewDecoder(os.Stdin)
	enc := jsonlines.NewEncoder(os.Stdout)
	for {
		req := &driver.InternalParseRequest{}
		if err := dec.Decode(req); err != nil {
			if err == io.EOF {
				os.Exit(0)
			}

			if err := enc.Encode(&driver.InternalParseResponse{
				Status: driver.Status(protocol.Fatal),
				Errors: []string{err.Error()},
			}); err != nil {
				os.Exit(-1)
			}

			continue
		}

		resp := &driver.InternalParseResponse{
			Status: driver.Status(protocol.Ok),
			AST: map[string]interface{}{
				"root": map[string]interface{}{
					"key": "val",
				},
			},
		}

		if err := enc.Encode(resp); err != nil {
			os.Exit(-1)
		}
	}
}
