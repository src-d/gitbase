package transformer

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/bblfsh/sdk/v3/uast"
	"github.com/bblfsh/sdk/v3/uast/nodes"
)

func uastType(uobj interface{}, op ObjectOp, part string) ObjectOp {
	if op == nil {
		op = Obj{}
	}
	utyp := uast.TypeOf(uobj)
	if utyp == "" {
		panic(fmt.Errorf("type is not registered: %T", uobj))
	}
	obj := Obj{uast.KeyType: String(utyp)}
	if part != "" {
		return JoinObj(obj, Part(part, op))
	}
	fields, ok := op.Fields()
	if !ok {
		return JoinObj(obj, op)
	}
	zero, opt := uast.NewObjectByTypeOpt(utyp)
	delete(zero, uast.KeyType)
	if len(zero) == 0 {
		return JoinObj(obj, op)
	}
	for _, f := range fields.fields {
		if f.name == uast.KeyType {
			continue
		}
		k := f.name
		_, ok := zero[k]
		_, ok2 := opt[k]
		if !ok && !ok2 {
			panic(ErrUndefinedField.New(utyp + "." + k))
		}
		delete(zero, k)
	}
	for k, v := range zero {
		obj[k] = Is(v)
	}
	return JoinObj(obj, op)
}

func UASTType(uobj interface{}, op ObjectOp) ObjectOp {
	return uastType(uobj, op, "")
}

func UASTTypePart(vr string, uobj interface{}, op ObjectOp) ObjectOp {
	return uastType(uobj, op, vr)
}

func remapPos(m ObjMapping, names map[string]string) ObjMapping {
	so, do := m.ObjMapping() // TODO: clone?

	sp := UASTType(uast.Positions{}, Fields{
		{Name: uast.KeyStart, Op: Var(uast.KeyStart), Optional: uast.KeyStart + "_exists"},
		{Name: uast.KeyEnd, Op: Var(uast.KeyEnd), Optional: uast.KeyEnd + "_exists"},
	})
	dp := UASTType(uast.Positions{}, Fields{
		{Name: uast.KeyStart, Op: Var(uast.KeyStart), Optional: uast.KeyStart + "_exists"},
		{Name: uast.KeyEnd, Op: Var(uast.KeyEnd), Optional: uast.KeyEnd + "_exists"},
	})
	if len(names) != 0 {
		sa, da := make(Obj), make(Obj)
		for k, v := range names {
			sa[k] = Var(v)
			if v != uast.KeyStart && v != uast.KeyEnd {
				da[k] = Var(v)
			}
		}
		sp, dp = JoinObj(sp, sa), JoinObj(dp, da)
	}
	return MapObj(
		JoinObj(so, Obj{uast.KeyPos: sp}),
		JoinObj(do, Obj{uast.KeyPos: dp}),
	)
}

func MapSemantic(nativeType string, semType interface{}, m ObjMapping) ObjMapping {
	return MapSemanticPos(nativeType, semType, nil, m)
}

func MapSemanticPos(nativeType string, semType interface{}, pos map[string]string, m ObjMapping) ObjMapping {
	so, do := m.ObjMapping() // TODO: clone?
	so = JoinObj(Obj{uast.KeyType: String(nativeType)}, so)
	so, do = remapPos(MapObj(so, do), pos).ObjMapping()
	return MapObj(so, UASTType(semType, do))
}

func CommentText(tokens [2]string, vr string) Op {
	return &commentUAST{
		startToken: tokens[0],
		endToken:   tokens[1],
		textVar:    vr + "_text",
		prefVar:    vr + "_pref",
		suffVar:    vr + "_suff",
		indentVar:  vr + "_tab",
		doTrim:     false,
	}
}

func CommentTextTrimmed(tokens [2]string, vr string) Op {
	return &commentUAST{
		startToken: tokens[0],
		endToken:   tokens[1],
		textVar:    vr + "_text",
		prefVar:    vr + "_pref",
		suffVar:    vr + "_suff",
		indentVar:  vr + "_tab",
		doTrim:     true,
	}
}

func CommentNode(block bool, vr string, pos Op) ObjectOp {
	obj := Obj{
		"Block":  Bool(block),
		"Text":   Var(vr + "_text"),
		"Prefix": Var(vr + "_pref"),
		"Suffix": Var(vr + "_suff"),
		"Tab":    Var(vr + "_tab"),
	}
	if pos != nil {
		obj[uast.KeyPos] = pos
	}
	return UASTType(uast.Comment{}, obj)
}

// commentElems contains individual comment elements.
// See uast.Comment for details.
type commentElems struct {
	StartToken string
	EndToken   string
	Text       string
	Prefix     string
	Suffix     string
	Indent     string
	DoTrim     bool
}

func (c *commentElems) isTab(r rune) bool {
	if unicode.IsSpace(r) {
		return true
	}
	for _, r2 := range c.StartToken {
		if r == r2 {
			return true
		}
	}
	for _, r2 := range c.EndToken {
		if r == r2 {
			return true
		}
	}
	return false
}

func (c *commentElems) Split(text string) bool {
	if c.DoTrim {
		text = strings.TrimLeftFunc(text, unicode.IsSpace)
	}

	if !strings.HasPrefix(text, c.StartToken) || !strings.HasSuffix(text, c.EndToken) {
		return false
	}
	text = strings.TrimPrefix(text, c.StartToken)
	text = strings.TrimSuffix(text, c.EndToken)

	// find prefix (if not already trimmed)
	i := strings.IndexFunc(text, func(r rune) bool {
		return !c.isTab(r)
	})

	c.Prefix = ""

	if i >= 0 {
		c.Prefix = text[:i]
		text = text[i:]
	} else {
		c.Prefix = text
		text = ""
	}

	// find suffix
	i = strings.LastIndexFunc(text, func(r rune) bool {
		return !c.isTab(r)
	})

	c.Suffix = ""
	if i >= 0 {
		c.Suffix = text[i+1:]
		text = text[:i+1]
	}
	c.Text = text

	sub := strings.Split(text, "\n")
	if len(sub) == 1 {
		// fast path, no tabs
		return true
	}

	// find minimal common prefix for other lines
	// first line is special, it won't contain tab
	for i, line := range sub[1:] {
		if i == 0 {
			j := strings.IndexFunc(line, func(r rune) bool {
				return !c.isTab(r)
			})

			c.Indent = ""
			if j >= 0 {
				c.Indent = line[:j]
			} else {
				return true // no tabs
			}
			continue
		}
		if strings.HasPrefix(line, c.Indent) {
			continue
		}
		j := strings.IndexFunc(line, func(r rune) bool {
			return !c.isTab(r)
		})

		tab := ""
		if j >= 0 {
			tab = line[:j]
		} else {
			return true // no tabs
		}

		for j := 0; j < len(c.Indent) && j < len(tab); j++ {
			if c.Indent[j] == tab[j] {
				continue
			}
			if j == 0 {
				return true // inconsistent, no tabs
			}
			tab = tab[:j]
			break
		}
		c.Indent = tab
	}
	for i, line := range sub {
		if i == 0 {
			continue
		}
		sub[i] = strings.TrimPrefix(line, c.Indent)
	}
	c.Text = strings.Join(sub, "\n")
	return true
}

func (c commentElems) Join() string {
	if c.Indent != "" {
		sub := strings.Split(c.Text, "\n")
		for i, line := range sub {
			if i == 0 {
				continue
			}
			sub[i] = c.Indent + line
		}
		c.Text = strings.Join(sub, "\n")
	}
	return strings.Join([]string{
		c.StartToken, c.Prefix,
		c.Text,
		c.Suffix, c.EndToken,
	}, "")
}

type commentUAST struct {
	startToken string
	endToken   string
	textVar    string
	prefVar    string
	suffVar    string
	indentVar  string
	doTrim     bool
}

func (*commentUAST) Kinds() nodes.Kind {
	return nodes.KindString
}

func (op *commentUAST) Check(st *State, n nodes.Node) (bool, error) {
	s, ok := n.(nodes.String)
	if !ok {
		return false, nil
	}

	c := commentElems{StartToken: op.startToken, EndToken: op.endToken, DoTrim: op.doTrim}
	if !c.Split(string(s)) {
		return false, nil
	}

	err := st.SetVars(Vars{
		op.textVar:   nodes.String(c.Text),
		op.prefVar:   nodes.String(c.Prefix),
		op.suffVar:   nodes.String(c.Suffix),
		op.indentVar: nodes.String(c.Indent),
	})
	return err == nil, err
}

func (op *commentUAST) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	var (
		text, pref, suff, tab nodes.String
	)

	err := st.MustGetVars(VarsPtrs{
		op.textVar: &text,
		op.prefVar: &pref, op.suffVar: &suff, op.indentVar: &tab,
	})
	if err != nil {
		return nil, err
	}

	c := commentElems{
		StartToken: op.startToken,
		EndToken:   op.endToken,
		Text:       string(text),
		Prefix:     string(pref),
		Suffix:     string(suff),
		Indent:     string(tab),
	}

	return nodes.String(c.Join()), nil
}
