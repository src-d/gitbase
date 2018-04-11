package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/bblfsh/sdk.v1/manifest"
	"gopkg.in/bblfsh/sdk.v1/protocol"

	"google.golang.org/grpc"
)

const FixturesCommandDescription = "" +
	"Generate integration tests' '.native' and '.uast' fixtures from source files"

type FixturesCommand struct {
	Args struct {
		SourceFiles []string `positional-arg-name:"sourcefile(s)" required:"true" description:"File(s) with the source code"`
	} `positional-args:"yes"`
	Language  string `long:"language" short:"l" default:"" description:"Language to parse"`
	Endpoint  string `long:"endpoint" short:"e" default:"localhost:9432" description:"Endpoint of the gRPC server to use"`
	ExtNative string `long:"extnative" short:"n" default:"native" description:"File extension for native files"`
	ExtUast   string `long:"extuast" short:"u" default:"uast" description:"File extension for uast files"`
	ExtProto  string `long:"extproto" short:"p" description:"File extenstion for proto message fiels"`
	Quiet     bool   `long:"quiet" short:"q" description:"Don't print any output"`

	manifestCommand
	cli protocol.ProtocolServiceClient
}

func (c *FixturesCommand) Execute(args []string) error {
	if err := c.manifestCommand.Execute(args); err != nil {
		return err
	}

	c.processManifest(c.Manifest)
	return nil
}

func (c *FixturesCommand) processManifest(m *manifest.Manifest) {
	conn, err := grpc.Dial(c.Endpoint, grpc.WithTimeout(time.Second*2), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Println("Endpoint connection error, is a bblfshd server running?")
		panic(err)
	}

	c.cli = protocol.NewProtocolServiceClient(conn)

	for _, f := range c.Args.SourceFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			fmt.Println("Error: File", f, "doesn't exists")
			os.Exit(1)
		}

		err := c.generateFixtures(f)
		if err != nil {
			fmt.Println("While generating fixtures for ", f)
			panic(err)
		}
	}
}

func (c *FixturesCommand) generateFixtures(filename string) error {
	if !c.Quiet {
		fmt.Println("Processing", filename, "...")
	}

	source, err := getSourceFile(filename)
	if err != nil {
		return err
	}

	native, err := c.getNative(source, filename)
	if err != nil {
		return err
	}

	err = c.writeResult(filename, native, c.ExtNative)
	if err != nil {
		return err
	}

	uast, err := c.getUast(source, filename)
	if err != nil {
		return err
	}

	err = c.writeResult(filename, uast.String(), c.ExtUast)
	if err != nil {
		return err
	}

	if c.ExtProto != "" {
		protoUast, err := uast.UAST.Marshal()
		if err != nil {
			return err
		}
		err = c.writeResult(filename, string(protoUast), c.ExtProto)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *FixturesCommand) getNative(source string, filename string) (string, error) {
	req := &protocol.NativeParseRequest{
		Language: c.Language,
		Content:  source,
		Filename: filename,
	}

	res, err := c.cli.NativeParse(context.Background(), req)
	if err != nil {
		return "", err
	}

	if res.Status != protocol.Ok {
		if !c.Quiet {
			fmt.Println("Warning: native request for ", filename, "returned errors:")
			for _, e := range res.Errors {
				fmt.Println(e)
			}
		}
	}

	return res.String(), nil
}

func (c *FixturesCommand) getUast(source string, filename string) (*protocol.ParseResponse, error) {
	req := &protocol.ParseRequest{
		Language: c.Language,
		Content:  source,
		Filename: filename,
	}

	res, err := c.cli.Parse(context.Background(), req)
	if err != nil {
		return nil, err
	}

	if res.Status != protocol.Ok {
		if !c.Quiet {
			fmt.Println("Warning: parse request for ", filename, "returned errors:")
			for _, e := range res.Errors {
				fmt.Println(e)
			}
		}
	}

	return res, nil
}

func (c *FixturesCommand) writeResult(origName, content, extension string) error {
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
