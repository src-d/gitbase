package transformer

import (
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"
)

type Tranformer interface {
	Do(code string, e protocol.Encoding, n *uast.Node) error
}
