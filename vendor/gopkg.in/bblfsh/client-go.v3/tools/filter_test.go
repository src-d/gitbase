package tools

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/uast/yaml"

	"github.com/stretchr/testify/assert"

	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

type Node = nodes.Node
type Arr = nodes.Array
type Obj = nodes.Object
type Str = nodes.String
type Int = nodes.Int

func toNode(o interface{}) Node {
	n, err := uast.ToNode(o)
	if err != nil {
		panic(err)
	}
	return n
}

func expectN(t testing.TB, it Iterator, exp int) {
	n := 0
	for it.Next() {
		_ = it.Node()
		n++
	}
	require.Equal(t, exp, n)
}

func TestFilter(t *testing.T) {
	var n Node

	r, err := Filter(n, "")
	require.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilterWrongType(t *testing.T) {
	var n Node

	_, err := FilterInt(n, "boolean(//*[@start-position or @end-position])")
	assert.NotNil(t, err)
}

func TestFilterBool(t *testing.T) {
	var n Node

	r, err := FilterBool(n, "boolean(0)")
	assert.Nil(t, err)
	assert.False(t, r)

	r, err = FilterBool(n, "boolean(1)")
	assert.Nil(t, err)
	assert.True(t, r)
}

func TestFilterNumber(t *testing.T) {
	var n Node = Obj{}

	r, err := FilterNumber(n, "count(//*)")
	assert.Nil(t, err)
	assert.Equal(t, 1, int(r))

	n = Arr{Obj{}, Obj{}}
	r, err = FilterNumber(n, "count(//*)")
	assert.Nil(t, err)
	assert.Equal(t, 3, int(r))
}

func TestFilterString(t *testing.T) {
	n := Obj{uast.KeyType: Str("TestType")}

	r, err := FilterString(n, "name(//*[1])")
	assert.Nil(t, err)
	assert.Equal(t, "TestType", r)
}

func TestFilter_All(t *testing.T) {
	var n Node

	_, err := Filter(n, "//*")
	assert.Nil(t, err)
}

func TestFilter_InternalType(t *testing.T) {
	n := Obj{uast.KeyType: Str("a")}

	r, err := Filter(n, "//a")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//b")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_Token(t *testing.T) {
	n := Obj{uast.KeyToken: Str("a")}

	r, err := Filter(n, "//*[@token='a']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@token='b']")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_Roles(t *testing.T) {
	n := Obj{uast.KeyRoles: Arr{Int(1)}}

	r, err := Filter(n, "//*[@role='Identifier']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@role='Qualified']")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_Properties(t *testing.T) {
	n := Obj{"k2": Str("v1"), "k1": Str("v2")}

	r, err := Filter(n, "//*[@k1='v2']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@k2='v1']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@k3='v1']")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_NoStartPosition(t *testing.T) {
	var n Node

	r, err := Filter(n, "//*[@start-offset='0']")
	assert.Nil(t, err)
	expectN(t, r, 0)

	r, err = Filter(n, "//*[@start-line='1']")
	assert.Nil(t, err)
	expectN(t, r, 0)

	r, err = Filter(n, "//*[@start-col='1']")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_StartPosition(t *testing.T) {
	n := toNode(uast.Identifier{
		GenNode: uast.GenNode{
			Positions: uast.Positions{
				uast.KeyStart: {
					Offset: 0,
					Line:   1, Col: 1,
				},
			},
		},
	})

	r, err := Filter(n, "//*[@start-offset='0']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@start-line='1']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@start-col='1']")
	assert.Nil(t, err)
	expectN(t, r, 1)
}

func TestFilter_NoEndPosition(t *testing.T) {
	var n Node

	r, err := Filter(n, "//*[@end-offset='0']")
	assert.Nil(t, err)
	expectN(t, r, 0)

	r, err = Filter(n, "//*[@end-line='1']")
	assert.Nil(t, err)
	expectN(t, r, 0)

	r, err = Filter(n, "//*[@end-col='1']")
	assert.Nil(t, err)
	expectN(t, r, 0)
}

func TestFilter_EndPosition(t *testing.T) {
	n := toNode(uast.Identifier{
		GenNode: uast.GenNode{
			Positions: uast.Positions{
				uast.KeyEnd: {
					Offset: 0,
					Line:   1, Col: 1,
				},
			},
		},
	})

	r, err := Filter(n, "//*[@end-offset='0']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@end-line='1']")
	assert.Nil(t, err)
	expectN(t, r, 1)

	r, err = Filter(n, "//*[@end-col='1']")
	assert.Nil(t, err)
	expectN(t, r, 1)
}

func TestFilter_InvalidExpression(t *testing.T) {
	var n Node

	r, err := Filter(n, ":")
	// FIXME
	//require.Equal(t, &ErrInvalidArgument{Message: "Invalid expression"}, err)
	require.NotNil(t, err)
	require.Nil(t, r)
}

const fixture = `./testdata/json.go.sem.uast`

func BenchmarkXPathV2(b *testing.B) {
	data, err := ioutil.ReadFile(fixture)
	require.NoError(b, err)
	node, err := uastyml.Unmarshal(data)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		it, err := Filter(node, `//uast:Identifier`)
		if err != nil {
			b.Fatal(err)
		}
		cnt := 0
		for it.Next() {
			cnt++
			_ = it.Node()
		}
		if cnt != 2292 {
			b.Fatal("wrong result:", cnt)
		}
	}
}
