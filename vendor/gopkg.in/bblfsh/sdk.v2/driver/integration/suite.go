package integration

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol1 "gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v2/driver/integration/consts"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	Endpoint   = integration.Endpoint
	Language   = integration.Language
	DriverPath = integration.DriverPath
)
const (
	DefaultFixtureLocation = "fixtures"
)

var Suite *suite

func Setup(endpoint, language, driverPath string) {
	if endpoint == "" {
		endpoint = "localhost:9432"
	}
	if language == "" {
		m, err := manifest.Load(manifest.Filename)
		if err != nil {
			panic(err)
		}
		language = m.Language
	}
	if driverPath == "" {
		driverPath = "./"
	}
	setup(endpoint, language, driverPath)
}

func setup(endpoint, language, driverPath string) {
	Suite = &suite{
		Endpoint: endpoint,
		Language: language,
		Fixtures: filepath.Join(driverPath, DefaultFixtureLocation),
	}
}

func init() {
	setup(os.Getenv(Endpoint), os.Getenv(Language), os.Getenv(DriverPath))
}

type suite struct {
	// Language of the driver being test.
	Language string
	// Endpoint of the grpc server to test.
	Endpoint string
	// Fixture to use against the driver
	Fixtures string

	c protocol1.ProtocolServiceClient
}

func (s *suite) SetUpTest(t *testing.T) {
	if s.Endpoint == "" || s.Language == "" {
		t.SkipNow()
	}
	t.Logf("dialing %v", s.Endpoint)

	r := require.New(t)
	// TODO: use client-go as soon NativeParse request is availabe on it.
	conn, err := grpc.Dial(s.Endpoint, grpc.WithTimeout(time.Second*2), grpc.WithInsecure(), grpc.WithBlock())
	r.Nil(err)

	s.c = protocol1.NewProtocolServiceClient(conn)
}

func (s *suite) TestParse(t *testing.T) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*", s.Fixtures))
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if !isSource(f) {
			continue
		}

		t.Run(filepath.Base(f), func(t *testing.T) {
			s.doTestParse(t, f)
		})
	}
}

func (s *suite) TestNativeParse(t *testing.T) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*", s.Fixtures))
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if !isSource(f) {
			continue
		}

		t.Run(filepath.Base(f), func(t *testing.T) {
			s.doTestNativeParse(t, f)
		})
	}
}

func (s *suite) doTestParse(t *testing.T, filename string) {
	r := require.New(t)

	source := getSourceCode(r, filename)
	req := &protocol1.ParseRequest{
		Language: s.Language,
		Content:  source,
	}

	res, err := s.c.Parse(context.Background(), req)
	r.Nil(err)

	expected := getUAST(r, filename)
	EqualText(r, expected, res.String())
}

func (s *suite) doTestNativeParse(t *testing.T, filename string) {
	r := require.New(t)

	source := getSourceCode(r, filename)
	req := &protocol1.NativeParseRequest{
		Language: s.Language,
		Content:  source,
	}

	res, err := s.c.NativeParse(context.Background(), req)
	r.Nil(err)

	expected := getAST(r, filename)
	EqualText(r, expected, res.String())
}

func EqualText(r *require.Assertions, expected, actual string) {
	if expected == actual {
		return
	}

	diff := difflib.ContextDiff{
		A:        difflib.SplitLines(expected),
		B:        difflib.SplitLines(actual),
		FromFile: "expected",
		ToFile:   "actual",
		Context:  3,
		Eol:      "\n",
	}

	patch, err := difflib.GetContextDiffString(diff)
	r.Nil(err)

	if patch != "" {
		r.Fail("response doesn't match", patch)
	}
}

func getSourceCode(r *require.Assertions, filename string) string {
	return getFileContent(r, filename, "")
}

func getUAST(r *require.Assertions, filename string) string {
	return getFileContent(r, filename, "uast")
}

func getAST(r *require.Assertions, filename string) string {
	return getFileContent(r, filename, "native")
}

func getFileContent(r *require.Assertions, filename, extension string) string {
	if len(extension) > 0 {
		filename = fmt.Sprintf("%s.%s", filename, extension)
	}
	content, err := ioutil.ReadFile(filename)
	r.Nil(err)

	return string(content)
}

func isSource(f string) bool {
	ext := filepath.Ext(f)
	return ext != ".native" && ext != ".uast"
}
