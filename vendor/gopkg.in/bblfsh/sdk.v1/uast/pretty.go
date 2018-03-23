package uast

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Pretty writes a pretty string representation of the *Node to a writer.
func Pretty(n *Node, w io.Writer, includes IncludeFlag) error {
	if n == nil {
		return nil
	}

	return printNode(w, 0, n, includes)
}

func printNode(w io.Writer, indent int, n *Node, includes IncludeFlag) error {
	nodeType := n.InternalType
	if !includes.Is(IncludeInternalType) {
		nodeType = "*"
	}

	if _, err := fmt.Fprintf(w, "%s {\n", nodeType); err != nil {
		return err
	}

	istr := strings.Repeat(".  ", indent+1)
	istrPrev := strings.Repeat(".  ", indent)

	if includes.Is(IncludeAnnotations) && len(n.Roles) > 0 {
		_, err := fmt.Fprintf(w, "%sRoles: %s\n",
			istr,
			rolesToString(n.Roles...),
		)
		if err != nil {
			return err
		}
	}

	if includes.Is(IncludeTokens) && n.Token != "" {
		if _, err := fmt.Fprintf(w, "%sTOKEN \"%s\"\n",
			istr, n.Token); err != nil {
			return err
		}
	}

	if includes.Is(IncludePositions) && n.StartPosition != nil {
		if _, err := fmt.Fprintf(w, "%sStartPosition: {\n", istr); err != nil {
			return err
		}

		if err := printPosition(w, indent+2, n.StartPosition); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s}\n", istr); err != nil {
			return err
		}
	}

	if includes.Is(IncludePositions) && n.EndPosition != nil {
		if _, err := fmt.Fprintf(w, "%sEndPosition: {\n", istr); err != nil {
			return err
		}

		if err := printPosition(w, indent+2, n.EndPosition); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s}\n", istr); err != nil {
			return err
		}
	}

	if includes.Is(IncludeProperties) && len(n.Properties) > 0 {
		if _, err := fmt.Fprintf(w, "%sProperties: {\n", istr); err != nil {
			return err
		}

		if err := printProperties(w, indent+2, n.Properties); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s}\n", istr); err != nil {
			return err
		}
	}

	if includes.Is(IncludeChildren) && len(n.Children) > 0 {
		if _, err := fmt.Fprintf(w, "%sChildren: {\n", istr); err != nil {
			return err
		}

		if err := printChildren(w, indent+2, n.Children, includes); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(w, "%s}\n", istr); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "%s}\n", istrPrev); err != nil {
		return err
	}

	return nil
}

func printChildren(w io.Writer, indent int, children []*Node, includes IncludeFlag) error {
	istr := strings.Repeat(".  ", indent)

	for idx, child := range children {
		_, err := fmt.Fprintf(w, "%s%d: ",
			istr,
			idx,
		)
		if err != nil {
			return err
		}

		if err := printNode(w, indent, child, includes); err != nil {
			return err
		}
	}

	return nil
}

func printProperties(w io.Writer, indent int, props map[string]string) error {
	istr := strings.Repeat(".  ", indent)
	keys := sortedKeys(props)

	for _, k := range keys {
		v := props[k]
		_, err := fmt.Fprintf(w, "%s%s: %s\n", istr, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}

func printPosition(w io.Writer, indent int, pos *Position) error {
	if pos == nil {
		return nil
	}

	istr := strings.Repeat(".  ", indent)

	if _, err := fmt.Fprintf(w, "%sOffset: %d\n", istr, pos.Offset); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "%sLine: %d\n", istr, pos.Line); err != nil {
		return err
	}

	_, err := fmt.Fprintf(w, "%sCol: %d\n", istr, pos.Col)
	return err
}

func rolesToString(roles ...Role) string {
	var strs []string
	for _, r := range roles {
		strs = append(strs, r.String())
	}

	return strings.Join(strs, ",")
}
