package cmd

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"

	"github.com/ghodss/yaml"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/yaml"
)

const Ast2GraphvizCommandDescription = "" +
	"Read '.native' files and generate graphviz diagrams"

type Ast2GraphvizCommand struct {
	Args struct {
		SourceFiles []string `positional-arg-name:"sourcefile(s)" required:"true" description:"File(s) with the native AST"`
	} `positional-args:"yes"`
	Output   string `long:"out" short:"o" default:"dot" description:"Output format (dot, svg, png)"`
	TypePred string `long:"type" short:"t" default:"@type" description:"Node type field in native AST"`
	Colors   string `long:"colors" short:"c" default:"colors.yml" description:"File with node color definitions"`
	NoPos    bool   `long:"no-pos" description:"Omit position info"`
	NoNils   bool   `long:"no-nils" description:"Omit nil fields"`

	nodeColors map[string]string
}

func (c *Ast2GraphvizCommand) Execute(args []string) error {
	if err := c.readColors(c.Colors); err != nil {
		return err
	}
	var last error
	for _, name := range c.Args.SourceFiles {
		if err := c.processFile(name); err != nil {
			log.Printf("error processing %v: %v", name, err)
			last = err
		}
	}
	return last
}

func (c *Ast2GraphvizCommand) readColors(name string) error {
	f, err := os.Open(name)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	var conf struct {
		Colors map[string]string `yaml:"colors"`
	}
	if err = yaml.Unmarshal(data, &conf); err != nil {
		return err
	}
	c.nodeColors = conf.Colors
	return nil
}
func (c *Ast2GraphvizCommand) processFile(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return err
	}
	ast, err := uastyml.Unmarshal(data)
	if err != nil {
		return fmt.Errorf("cannot unmarshal uast: %v", err)
	}
	ext := c.Output
	if ext == "" {
		ext = "dot"
	}
	outName := name + "." + ext
	out, err := os.Create(outName)
	if err != nil {
		return err
	}
	defer out.Close()

	if ext == "dot" || ext == "gv" {
		return c.writeGraphviz(out, ast)
	}

	buf := bytes.NewBuffer(nil)
	if err := c.writeGraphviz(buf, ast); err != nil {
		return err
	}

	cmd := exec.Command("dot", "-T"+ext)
	cmd.Stdin = buf
	cmd.Stdout = out
	return cmd.Run()
}

func (c *Ast2GraphvizCommand) writeGraphviz(w io.Writer, n nodes.Node) error {
	fmt.Fprintln(w, "digraph AST {")
	defer fmt.Fprintln(w, "}")

	var last int
	nextID := func() string {
		last++
		id := last
		return "n" + strconv.Itoa(id)
	}

	const (
		circle  = "ellipse"
		box     = "box"
		diamond = "diamond"
	)

	writeNode := func(id, label, shape, color string, small bool) {
		if shape == "" {
			shape = circle
		}
		opt := ""
		if small {
			const h = 0.4
			w := 0.8
			if label == "" {
				w = h
			}
			opt += fmt.Sprintf(" fontsize=10 margin=0 width=%.2f height=%.2f", w, h)
		}
		if color != "" {
			opt += fmt.Sprintf(" color=%q style=filled", color)
		}
		fmt.Fprintf(w, "\t%s [label=%q shape=%s%s]\n", id, label, shape, opt)
	}
	writePred := func(from, via, to string) {
		fmt.Fprintf(w, "\t%s -> %s [label=%q fontsize=10]\n", from, to, via)
	}
	writeLink := func(from, to string) {
		fmt.Fprintf(w, "\t%s -> %s\n", from, to)
	}
	_, _, _ = writeNode, writePred, writeLink

	var proc func(nodes.Node) string
	proc = func(n nodes.Node) string {
		id := nextID()
		switch n := n.(type) {
		case nodes.Array:
			writeNode(id, "", diamond, "", true)
			for _, s := range n {
				sid := proc(s)
				writeLink(id, sid)
			}
		case nodes.Object:
			n = n.CloneObject()
			tp, _ := n[c.TypePred].(nodes.String)
			delete(n, c.TypePred)
			if c.NoPos {
				delete(n, "@pos")
			}
			writeNode(id, string(tp), circle, c.nodeColors[string(tp)], tp == "")
			keys := n.Keys()
			for _, k := range keys {
				v := n[k]
				if c.NoNils && v == nil {
					continue
				}
				sid := proc(v)
				writePred(id, k, sid)
			}
		default:
			writeNode(id, fmt.Sprint(n), box, "", true)
		}
		return id
	}
	proc(n)
	return nil
}
