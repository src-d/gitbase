// Copyright 2018 Sourced Technologies SL
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"

	"gopkg.in/bblfsh/client-go.v3"
	"gopkg.in/bblfsh/client-go.v3/tools"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
	"gopkg.in/bblfsh/sdk.v2/uast/yaml"
)

func main() {
	var opts struct {
		Host     string `short:"a" long:"host" description:"Babelfish endpoint address" default:"localhost:9432"`
		Language string `short:"l" long:"language" description:"language to parse (default: auto)"`
		Query    string `short:"q" long:"query" description:"XPath query applied to the resulting UAST"`
		Mode     string `short:"m" long:"mode" description:"UAST transformation mode: semantic, annotated, native"`
		Out      string `short:"o" long:"out" description:"Output format: yaml, json, bin" default:"yaml"`
	}
	args, err := flags.Parse(&opts)
	if err != nil {
		fatalf("couldn't parse flags: %v", err)
	}

	if len(args) == 0 {
		fatalf("missing file to parse")
	} else if len(args) > 1 {
		fatalf("couldn't parse more than a file at a time")
	}
	filename := args[0]

	client, err := bblfsh.NewClient(opts.Host)
	if err != nil {
		fatalf("couldn't create client: %v", err)
	}

	req := client.NewParseRequest().
		Language(opts.Language).
		Filename(filename).
		ReadFile(filename)
	if opts.Mode != "" {
		m, err := bblfsh.ParseMode(opts.Mode)
		if err != nil {
			fatalf("%v", err)
		}
		req = req.Mode(m)
	}
	ast, _, err := req.UAST()
	if err != nil {
		fatalf("couldn't parse %s: %v", args[0], err)
	}
	if opts.Query != "" {
		it, err := tools.Filter(ast, opts.Query)
		if err != nil {
			fatalf("%v", err)
		}
		var arr nodes.Array
		for it.Next() {
			arr = append(arr, it.Node().(nodes.Node))
		}
		ast = arr
	}
	var (
		data []byte
	)
	switch opts.Out {
	case "", "yaml", "yml":
		data, err = uastyml.Marshal(ast)
	case "json":
		data, err = json.MarshalIndent(ast, "", "  ")
	case "bin", "binary":
		buf := new(bytes.Buffer)
		err = nodesproto.WriteTo(buf, ast)
		data = buf.Bytes()
	default:
		err = fmt.Errorf("unsupported output format: %q", opts.Out)
	}
	if err == nil {
		_, err = os.Stdout.Write(data)
	}
	if err != nil {
		fatalf("couldn't encode UAST: %v", err)
	}
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
