package function

import (
	"bytes"
	"fmt"
	"hash"

	"github.com/bblfsh/go-client/v4/tools"
	"github.com/bblfsh/sdk/v3/uast/nodes/nodesproto"

	"github.com/sirupsen/logrus"
	"github.com/src-d/gitbase"
	bblfsh "github.com/bblfsh/go-client/v4"
	"github.com/bblfsh/sdk/v3/uast/nodes"
	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
)

var (
	// ErrParseBlob is returned when the blob can't be parsed with bblfsh.
	ErrParseBlob = errors.NewKind("unable to parse the given blob using bblfsh: %s")

	// ErrUnmarshalUAST is returned when an error arises unmarshaling UASTs.
	ErrUnmarshalUAST = errors.NewKind("error unmarshaling UAST: %s")

	// ErrMarshalUAST is returned when an error arises marshaling UASTs.
	ErrMarshalUAST = errors.NewKind("error marshaling uast node: %s")
)

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
) (nodes.Node, error) {
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
			return nil, ErrParseBlob.New(
				fmt.Errorf("unsupported language %q", lang))
		}
	}

	node, _, err := client.ParseWithMode(ctx, mode, lang, blob)
	if err != nil {
		err := ErrParseBlob.New(err)
		logrus.Warn(err)
		ctx.Warn(0, err.Error())
		return nil, err
	}

	return node, nil
}

func applyXpath(n nodes.Node, query string) (nodes.Array, error) {
	var filtered nodes.Array
	it, err := tools.Filter(n, query)
	if err != nil {
		return nil, err
	}

	for n := range tools.Iterate(it) {
		filtered = append(filtered, n)
	}

	return filtered, nil
}

func marshalNodes(arr nodes.Array) (interface{}, error) {
	if len(arr) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	if err := nodesproto.WriteTo(&buf, arr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func getNodes(data interface{}) (nodes.Array, error) {
	if data == nil {
		return nil, nil
	}

	raw, ok := data.([]byte)
	if !ok {
		return nil, ErrUnmarshalUAST.New("wrong underlying UAST format")
	}

	return unmarshalNodes(raw)
}

func unmarshalNodes(data []byte) (nodes.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)
	n, err := nodesproto.ReadTree(buf)
	if err != nil {
		return nil, err
	}

	if n.Kind() != nodes.KindArray {
		return nil, fmt.Errorf("unmarshal: wrong kind of node found %q, expected %q",
			n.Kind(), nodes.KindArray.String())
	}

	return n.(nodes.Array), nil
}
