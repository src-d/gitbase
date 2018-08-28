package positioner

import (
	"fmt"
	"sort"

	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/transformer"
)

var _ transformer.CodeTransformer = Positioner{}

const cloneObj = false

// NewFillOffsetFromLineCol gets the original source code and its parsed form
// as *Node and fills every Offset field using its Line and Column.
func NewFillOffsetFromLineCol() Positioner {
	return Positioner{method: fillOffsetFromLineCol}
}

// NewFillLineColFromOffset gets the original source code and its parsed form
// as *Node and fills every Line and Column field using its Offset.
func NewFillLineColFromOffset() Positioner {
	return Positioner{method: fillLineColFromOffset}
}

// Positioner is a transformation that only changes positional information.
type Positioner struct {
	method func(*positionIndex, *uast.Position) error
}

// OnCode uses the source code to update positional information.
func (t Positioner) OnCode(code string) transformer.Transformer {
	idx := newPositionIndex([]byte(code))
	return transformer.TransformObjFunc(func(o nodes.Object) (nodes.Object, bool, error) {
		pos := uast.AsPosition(o)
		if pos == nil {
			return o, false, nil
		}
		if err := t.method(idx, pos); err != nil {
			return o, false, err
		}
		if cloneObj {
			o = o.CloneObject()
		}
		for k, v := range pos.ToObject() {
			o[k] = v
		}
		return o, cloneObj, nil
	})
}

func fillOffsetFromLineCol(idx *positionIndex, pos *uast.Position) error {
	offset, err := idx.Offset(int(pos.Line), int(pos.Col))
	if err != nil {
		return err
	}
	pos.Offset = uint32(offset)
	return nil
}

func fillLineColFromOffset(idx *positionIndex, pos *uast.Position) error {
	line, col, err := idx.LineCol(int(pos.Offset))
	if err != nil {
		return err
	}
	pos.Line = uint32(line)
	pos.Col = uint32(col)
	return nil
}

type positionIndex struct {
	offsetByLine []int
	size         int
}

func newPositionIndex(data []byte) *positionIndex {
	idx := &positionIndex{}
	idx.size = len(data)
	idx.addLineOffset(0)
	for offset, b := range data {
		if b == '\n' {
			idx.addLineOffset(offset + 1)
		}
	}

	return idx
}

func (idx *positionIndex) addLineOffset(offset int) {
	idx.offsetByLine = append(idx.offsetByLine, offset)

}

// LineCol returns a one-based line and col given a zero-based byte offset.
// It returns an error if the given offset is out of bounds.
func (idx *positionIndex) LineCol(offset int) (int, int, error) {
	var (
		minOffset = 0
		maxOffset = idx.size
	)

	if offset < minOffset || offset > maxOffset {
		return 0, 0, fmt.Errorf("offset out of bounds: %d [%d, %d]", offset, minOffset, maxOffset)
	}

	line := sort.Search(len(idx.offsetByLine), func(i int) bool {
		c := idx.offsetByLine[i] > offset
		return c
	})
	if line <= 0 || line > len(idx.offsetByLine) {
		return 0, 0, fmt.Errorf("offset not found in index: %d", offset)
	}

	lineOffset := idx.offsetByLine[line-1]
	col := offset - lineOffset + 1
	return line, col, nil
}

// Offset returns a zero-based byte offset given a one-based line and column.
// It returns an error if the given line and column are out of bounds.
func (idx *positionIndex) Offset(line, col int) (int, error) {
	var (
		minLine = 1
		maxLine = len(idx.offsetByLine)
		minCol  = 1
	)

	maxOffset := idx.size - 1

	if line < minLine || line > maxLine {
		return -1, fmt.Errorf("line out of bounds: %d [%d, %d]", line, minLine, maxLine)
	}

	nextLine := line
	line = line - 1
	if nextLine < len(idx.offsetByLine) {
		maxOffset = idx.offsetByLine[nextLine] - 1
	}

	maxCol := maxOffset - idx.offsetByLine[line] + 1

	// For empty files with 1-indexed drivers, set maxCol to 1
	if maxCol == 0 && col == 1 {
		maxCol = 1
	}

	if col < minCol || (maxCol > 0 && col - 1 > maxCol) {
		return 0, fmt.Errorf("column out of bounds: %d [%d, %d]", col, minCol, maxCol)
	}

	offset := idx.offsetByLine[line] + col - 1
	return offset, nil
}
