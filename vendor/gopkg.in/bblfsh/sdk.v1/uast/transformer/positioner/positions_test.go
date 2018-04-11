package positioner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v1/uast"
)

func TestFillLineColFromOffset(t *testing.T) {
	require := require.New(t)

	data := "hello\n\nworld"

	input := &uast.Node{
		StartPosition: &uast.Position{Offset: 0},
		EndPosition:   &uast.Position{Offset: 4},
		Children: []*uast.Node{{
			StartPosition: &uast.Position{Offset: 7},
			EndPosition:   &uast.Position{Offset: 12},
		}},
	}

	expected := &uast.Node{
		StartPosition: &uast.Position{Offset: 0, Line: 1, Col: 1},
		EndPosition:   &uast.Position{Offset: 4, Line: 1, Col: 5},
		Children: []*uast.Node{{
			StartPosition: &uast.Position{Offset: 7, Line: 3, Col: 1},
			EndPosition:   &uast.Position{Offset: 12, Line: 3, Col: 6},
		}},
	}

	p := NewFillLineColFromOffset()
	err := p.Do(data, 0, input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestFillOffsetFromLineCol(t *testing.T) {
	require := require.New(t)

	data := "hello\n\nworld"
	input := &uast.Node{
		StartPosition: &uast.Position{Line: 1, Col: 1},
		EndPosition:   &uast.Position{Line: 1, Col: 5},
		Children: []*uast.Node{{
			StartPosition: &uast.Position{Line: 3, Col: 1},
			EndPosition:   &uast.Position{Line: 3, Col: 5},
		}},
	}

	expected := &uast.Node{
		StartPosition: &uast.Position{Offset: 0, Line: 1, Col: 1},
		EndPosition:   &uast.Position{Offset: 4, Line: 1, Col: 5},
		Children: []*uast.Node{{
			StartPosition: &uast.Position{Offset: 7, Line: 3, Col: 1},
			EndPosition:   &uast.Position{Offset: 11, Line: 3, Col: 5},
		}},
	}

	p := NewFillOffsetFromLineCol()
	err := p.Do(data, 0, input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestEmptyFile(t *testing.T) {
	require := require.New(t)

	data := ""

	input := &uast.Node{
		StartPosition: &uast.Position{Line: 1, Col: 1},
		EndPosition:   &uast.Position{Line: 1, Col: 1},
	}

	expected := &uast.Node{
		StartPosition: &uast.Position{Offset: 0, Line: 1, Col: 1},
		EndPosition:   &uast.Position{Offset: 0, Line: 1, Col: 1},
	}

	p := NewFillOffsetFromLineCol()
	err := p.Do(data, 0, input)
	require.NoError(err)
	require.Equal(expected, input)
}
