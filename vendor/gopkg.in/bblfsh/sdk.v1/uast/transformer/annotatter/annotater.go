package annotatter

import (
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/bblfsh/sdk.v1/uast/ann"
)

type Annotatter struct {
	r *ann.Rule
}

func NewAnnotatter(r *ann.Rule) *Annotatter {
	return &Annotatter{r: r}
}

func (t *Annotatter) Do(code string, e protocol.Encoding, n *uast.Node) error {
	return t.r.Apply(n)
}
