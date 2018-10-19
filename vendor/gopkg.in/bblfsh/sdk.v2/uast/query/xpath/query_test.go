package xpath

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"gopkg.in/bblfsh/sdk.v2/uast/yaml"

	"github.com/stretchr/testify/require"

	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/query"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

func mustNode(o interface{}) nodes.Node {
	n, err := uast.ToNode(o)
	if err != nil {
		panic(err)
	}
	return n
}

func TestFilter(t *testing.T) {
	var root = nodes.Array{
		mustNode(uast.Identifier{
			GenNode: uast.GenNode{
				Positions: uast.Positions{
					uast.KeyStart: {Offset: 7, Line: 1, Col: 3},
					uast.KeyEnd:   {Offset: 11, Line: 4, Col: 1},
				},
			},
			Name: "Foo",
		}),
		nodes.Object{
			uast.KeyType:  nodes.String("Ident"),
			uast.KeyToken: nodes.String("A"),
			uast.KeyRoles: nodes.Array{
				nodes.Int(role.Identifier),
				nodes.Int(role.Name),
			},
		},
	}

	idx := New()

	it, err := idx.Execute(root, "//uast:Identifier[@Name='Foo']")
	require.NoError(t, err)
	expect(t, it, root[0])

	it, err = idx.Execute(root, "//uast:Identifier/Name[text() = 'Foo']/..")
	require.NoError(t, err)
	expect(t, it, root[0])

	it, err = idx.Execute(root, "//*[@start-col=3]")
	require.NoError(t, err)
	expect(t, it, root[0])

	it, err = idx.Execute(root, "//Identifier")
	require.NoError(t, err)
	expect(t, it)

	it, err = idx.Execute(root, "//*[name() = 'uast:Identifier']")
	require.NoError(t, err)
	expect(t, it, root[0])

	it, err = idx.Execute(root, "//*[local-name() = 'Identifier']")
	require.NoError(t, err)
	expect(t, it, root[0])

	it, err = idx.Execute(root, "//Ident")
	require.NoError(t, err)
	expect(t, it, root[1])

	it, err = idx.Execute(root, "//Ident[text() = 'A']")
	require.NoError(t, err)
	expect(t, it, root[1])

	it, err = idx.Execute(root, "//Ident[@role = 'Name']")
	require.NoError(t, err)
	expect(t, it, root[1])

	it, err = idx.Execute(root, "//Ident[@role = 'Invalid']")
	require.NoError(t, err)
	expect(t, it)
}

func TestFilterObject(t *testing.T) {
	b := nodes.Object{
		uast.KeyType: nodes.String("B"),
	}
	c := nodes.Object{
		uast.KeyType: nodes.String("C"),
	}
	d := nodes.Object{
		uast.KeyType: nodes.String("d:X"),
	}
	v := nodes.String("val")
	va, vb := nodes.String("a"), nodes.String("b")
	varr := nodes.Array{
		va,
		vb,
	}
	var root = nodes.Object{
		uast.KeyType: nodes.String("A"),
		"key":        v,
		"keys":       varr,
		"one":        b,
		"sub": nodes.Array{
			c,
			d,
		},
	}
	/*
		<A key='val' keys='a' keys='b'>
			<key>val</key>
			<keys>a</keys>
			<keys>b</keys>
			<one>
				<B></B>
			</one>
			<sub>
				<C></C>
				<d:X></d:X>
			</sub>
		</A>
	*/

	idx := New()

	queries := []struct {
		name string
		qu   string
		exp  []nodes.Node
	}{
		{
			name: "root", qu: "/",
			exp: []nodes.Node{root},
		},
		{
			name: "root tag", qu: "/A",
			exp: []nodes.Node{root},
		},
		{
			name: "field obj", qu: "/A/one",
			exp: []nodes.Node{b},
		},
		{
			name: "field obj tag", qu: "/A/one/B",
			exp: []nodes.Node{b},
		},
		{
			name: "field obj arr", qu: "/A/sub",
			exp: []nodes.Node{nodes.Array{c, d}},
		},
		{
			name: "field obj arr tag", qu: "/A/sub/C",
			exp: []nodes.Node{c},
		},
		{
			name: "inner field", qu: "//one",
			exp: []nodes.Node{b},
		},
		{
			name: "inner obj", qu: "//B",
			exp: []nodes.Node{b},
		},
		{
			name: "inner obj 2", qu: "//C",
			exp: []nodes.Node{c},
		},
		{
			name: "inner obj ns", qu: "//d:X",
			exp: []nodes.Node{d},
		},
		{
			name: "field value", qu: "/A/key",
			exp: []nodes.Node{v},
		},
		{
			name: "field value text", qu: "/A/key[text() = 'val']",
			exp: []nodes.Node{v},
		},
		{
			name: "field value arr", qu: "/A/keys",
			exp: []nodes.Node{varr},
		},
		{
			name: "text", qu: "//*[text() = 'a']",
			exp: []nodes.Node{varr},
		},
		{
			name: "attr value", qu: "//A[@key='val']",
			exp: []nodes.Node{root},
		},
		{
			name: "attr value arr", qu: "//A[@keys='a']",
			exp: []nodes.Node{root},
		},
		{
			name: "attr or", qu: "//A[@keys or @key]",
			exp: []nodes.Node{root},
		},
		{
			name: "boolean empty set", qu: "boolean(//*[@blah])",
			exp: []nodes.Node{nodes.Bool(false)},
		},
		{
			name: "boolean node set", qu: "boolean(/A)",
			exp: []nodes.Node{nodes.Bool(true)},
		},
		{
			name: "node name", qu: "name(//A)",
			exp: []nodes.Node{nodes.String("A")},
		},
		// TODO: fix in xpath library
		//{
		//	name: "field value arr elem", qu: "/A/keys/",
		//	exp: []nodes.Node{va, vb},
		//},
	}

	for _, c := range queries {
		c := c
		t.Run(c.name, func(t *testing.T) {
			it, err := idx.Execute(root, c.qu)
			require.NoError(t, err)
			expect(t, it, c.exp...)
		})
	}
}

func expect(t testing.TB, it query.Iterator, exp ...nodes.Node) {
	var out []nodes.Node
	for it.Next() {
		out = append(out, it.Node().(nodes.Node))
	}
	require.Equal(t, exp, out)
}

const dataDir = "../testdata"

func readUAST(t testing.TB, path string) nodes.Node {
	data, err := ioutil.ReadFile(path)
	require.NoError(t, err)
	nd, err := uastyml.Unmarshal(data)
	require.NoError(t, err)
	return nd
}

func BenchmarkXPath(b *testing.B) {
	root := readUAST(b, filepath.Join(dataDir, "large.go.sem.uast"))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := New()
		it, err := idx.Execute(root, "//uast:Identifier")
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for it.Next() {
			_ = it.Node()
			n++
		}
		if n != 2292 {
			b.Fatal("nodes:", n)
		}
	}
}

func BenchmarkXPathPrepare(b *testing.B) {
	root := readUAST(b, filepath.Join(dataDir, "large.go.sem.uast"))

	idx := New()
	q, err := idx.Prepare("//uast:Identifier")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		it, err := q.Execute(root)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for it.Next() {
			_ = it.Node()
			n++
		}
		if n != 2292 {
			b.Fatal("nodes:", n)
		}
	}
}
