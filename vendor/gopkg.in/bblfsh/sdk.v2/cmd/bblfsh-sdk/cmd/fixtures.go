package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"google.golang.org/grpc"
	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
	protocol2 "gopkg.in/bblfsh/sdk.v2/protocol"

	derrors "gopkg.in/bblfsh/sdk.v2/driver/errors"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/yaml"
)

const FixturesCommandDescription = "" +
	"Generate integration tests' '.native', '.sem.uast', '.legacy' and '.proto' fixtures from source files"

type FixturesCommand struct {
	Args struct {
		SourceFiles []string `positional-arg-name:"sourcefile(s)" required:"true" description:"File(s) with the source code"`
	} `positional-args:"yes"`
	Language  string `long:"language" short:"l" default:"" description:"Language to parse"`
	Endpoint  string `long:"endpoint" short:"e" default:"localhost:9432" description:"Endpoint of the gRPC server to use"`
	ExtNative string `long:"extnative" short:"n" default:"native" description:"File extension for native files"`
	ExtUast   string `long:"extuast" short:"u" default:"sem.uast" description:"File extension for uast files"`
	ExtLegacy string `long:"extlegacy" short:"g" default:"legacy" description:"File extenstion for leagacy UASTv1 files"`
	ExtProto  string `long:"extproto" short:"p" description:"File extenstion for proto message files"`
	Quiet     bool   `long:"quiet" short:"q" description:"Don't print any output"`

	cli1 protocol1.ProtocolServiceClient
	cli2 protocol2.DriverClient
}

func (c *FixturesCommand) Execute(args []string) error {
	conn, err := grpc.Dial(c.Endpoint, grpc.WithTimeout(time.Second*2), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Endpoint connection error, is a bblfshd server running?")
		return err
	}

	c.cli1 = protocol1.NewProtocolServiceClient(conn)
	c.cli2 = protocol2.NewDriverClient(conn)

	for _, f := range c.Args.SourceFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			fmt.Println("Error: File", f, "doesn't exists")
			os.Exit(1)
		}

		err := c.generateFixtures(f)
		if err != nil {
			fmt.Println("While generating fixtures for ", f)
			return err
		}
	}
	return nil
}

//generateFixtures writes v2 '.sem.uast', '.native' and v1 .legacy and .proto (optional) files.
//All of them contain plain-text represenation of the same UAST in differetn formats.
func (c *FixturesCommand) generateFixtures(filename string) error {
	if !c.Quiet {
		fmt.Println("Processing", filename, "...")
	}

	source, err := getSourceFile(filename)
	if err != nil {
		return err
	}

	//UASTv2 sematic, native
	err = c.writeUASTv2(source, filename, c.ExtNative, protocol2.Mode_Native)
	if err != nil {
		return err
	}

	err = c.writeUASTv2(source, filename, c.ExtUast, protocol2.Mode_Semantic)
	if err != nil {
		return err
	}

	//UASTv1 legacy, proto
	resp, err := c.writeLegacyUASTv1(source, filename)
	if err != nil {
		return err
	}

	if c.ExtProto != "" {
		return c.writeProto(resp, filename, c.ExtProto)
	}

	return nil
}

func (c *FixturesCommand) writeUASTv2(source, filename, ext string, mode protocol2.Mode) error {
	ast, err := c.getUast(source, filename, mode)
	if err != nil {
		return err
	}

	data, err := uastyml.Marshal(ast)
	if err != nil {
		return err
	}

	return c.writeResult(filename, ext, data)
}

func (c *FixturesCommand) getUast(source, filename string, mode protocol2.Mode) (nodes.Node, error) {
	req := &protocol2.ParseRequest{
		Language: c.Language,
		Content:  source,
		Filename: filename,
		Mode:     mode,
	}

	res, err := c.cli2.Parse(context.Background(), req)
	if err != nil {
		return nil, err
	}

	ast, err := res.Nodes()
	if derrors.ErrSyntax.Is(err) {
		if !c.Quiet {
			fmt.Println("Warning: parsing native AST for ", filename, "returned errors:")
			fmt.Println(err)
		}
	} else if err != nil {
		return nil, err
	}

	return ast, nil
}

func (c *FixturesCommand) writeLegacyUASTv1(source string, filename string) (*protocol1.ParseResponse, error) {
	resp, err := c.getLegacyUASTv1(source, filename)
	if err != nil {
		return nil, err
	}

	err = c.writeResult(filename, c.ExtLegacy, []byte(resp.UAST.String()))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *FixturesCommand) getLegacyUASTv1(source string, filename string) (*protocol1.ParseResponse, error) {
	req := &protocol1.ParseRequest{
		Language: c.Language,
		Content:  source,
		Filename: filename,
	}

	res, err := c.cli1.Parse(context.Background(), req)
	if err != nil {
		return nil, err
	}

	if res.Status != protocol1.Ok {
		if !c.Quiet {
			fmt.Println("Warning: parse request for ", filename, "returned errors:")
			for _, e := range res.Errors {
				fmt.Println(e)
			}
		}
	}

	return res, nil
}

func (c *FixturesCommand) writeProto(resp *protocol1.ParseResponse, filename, ext string) error {
	protoUast, err := resp.UAST.Marshal()
	if err != nil {
		return err
	}

	err = c.writeResult(filename, ext, protoUast)
	if err != nil {
		return err
	}
	return nil
}

func (c *FixturesCommand) writeResult(origName, extension string, content []byte) error {
	outname := origName + "." + extension
	if !c.Quiet {
		fmt.Println("\tWriting", outname, "...")
	}

	err := ioutil.WriteFile(outname, []byte(content), 0644)
	if err != nil {
		return err
	}

	return nil
}

func getSourceFile(f string) (string, error) {
	content, err := ioutil.ReadFile(f)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
