package function

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/src-d/gitbase"
	bblfsh "gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/sdk.v1/uast"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	// ErrParseBlob is returned when the blob can't be parsed with bblfsh.
	ErrParseBlob = errors.NewKind("unable to parse the given blob using bblfsh: %s")

	// ErrUnmarshalUAST is returned when an error arises unmarshaling UASTs.
	ErrUnmarshalUAST = errors.NewKind("error unmarshaling UAST: %s")

	// ErrMarshalUAST is returned when an error arises marshaling UASTs.
	ErrMarshalUAST = errors.NewKind("error marshaling uast node: %s")
)

// MarshalUASTNodes takes in a list of UAST nodes and serializes it.
func MarshalUASTNodes(nodes []*uast.Node) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, r.(error)
		}
	}()

	buf := &bytes.Buffer{}
	for _, n := range nodes {
		if n != nil {
			data, err := n.Marshal()
			if err != nil {
				return nil, err
			}

			if err := binary.Write(
				buf, binary.BigEndian, int32(len(data)),
			); err != nil {
				return nil, err
			}

			n, _ := buf.Write(data)
			if n != len(data) {
				return nil, ErrMarshalUAST.New("couldn't write all the data")
			}
		}
	}

	return buf.Bytes(), nil
}

// UnmarshalUASTNodes takes in a sequence of bytes and deserializes it as a list of UAST nodes.
func UnmarshalUASTNodes(data []byte) ([]*uast.Node, error) {
	if len(data) == 0 {
		return nil, nil
	}

	nodes := []*uast.Node{}
	buf := bytes.NewBuffer(data)
	for {
		var nodeLen int32
		if err := binary.Read(
			buf, binary.BigEndian, &nodeLen,
		); err != nil {
			if err == io.EOF {
				break
			}

			return nil, ErrUnmarshalUAST.New(err)
		}

		node := uast.NewNode()
		if err := node.Unmarshal(buf.Next(int(nodeLen))); err != nil {
			return nil, ErrUnmarshalUAST.New(err)
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func exprToString(
	ctx *sql.Context,
	e sql.Expression,
	r sql.Row,
) (string, error) {
	if e == nil {
		return "", nil
	}

	x, err := e.Eval(ctx, r)
	if err != nil {
		return "", err
	}

	if x == nil {
		return "", nil
	}

	x, err = sql.Text.Convert(x)
	if err != nil {
		return "", err
	}

	return x.(string), nil
}

func computeKey(h hash.Hash, mode, lang string, blob []byte) (string, error) {
	h.Reset()
	if err := writeToHash(h, [][]byte{
		[]byte(mode),
		[]byte(lang),
		blob,
	}); err != nil {
		return "", err
	}

	return string(h.Sum(nil)), nil
}

func writeToHash(h hash.Hash, elements [][]byte) error {
	for _, e := range elements {
		n, err := h.Write(e)
		if err != nil {
			return err
		}

		if n != len(e) {
			return fmt.Errorf("cache key hash: " +
				"couldn't write all the content")
		}
	}

	return nil
}

func getUASTFromBblfsh(ctx *sql.Context,
	blob []byte,
	lang, xpath string,
	mode bblfsh.Mode,
) (*uast.Node, error) {
	session, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	client, err := session.BblfshClient()
	if err != nil {
		return nil, err
	}

	// If we have a language we must check if it's supported. If we don't, bblfsh
	// is the one that will have to identify the language.
	if lang != "" {
		ok, err = client.IsLanguageSupported(ctx, lang)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, nil
		}
	}

	resp, err := client.ParseWithMode(ctx, mode, lang, blob)
	if err != nil {
		err := ErrParseBlob.New(err)
		logrus.Warn(err)
		return nil, err
	}

	if len(resp.Errors) > 0 {
		err := ErrParseBlob.New(strings.Join(resp.Errors, "\n"))
		logrus.Warn(err)
		return nil, err
	}

	return resp.UAST, nil
}

func getNodes(data interface{}) (nodes []*uast.Node, err error) {
	if data == nil {
		return nil, nil
	}

	raw, ok := data.([]byte)
	if !ok {
		return nil, ErrUnmarshalUAST.New("wrong underlying UAST format")
	}

	return UnmarshalUASTNodes(raw)
}
