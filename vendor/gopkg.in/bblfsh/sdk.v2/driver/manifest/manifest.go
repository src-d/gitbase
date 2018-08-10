package manifest

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const Filename = "manifest.toml"

type DevelopmentStatus string

const (
	Planning DevelopmentStatus = "planning"
	PreAlpha DevelopmentStatus = "pre-alpha"
	Alpha    DevelopmentStatus = "alpha"
	Beta     DevelopmentStatus = "beta"
	Stable   DevelopmentStatus = "stable"
	Mature   DevelopmentStatus = "mature"
	Inactive DevelopmentStatus = "inactive"
)

// Rank is an integer indicating driver stability. Higher is better.
func (s DevelopmentStatus) Rank() int {
	// TODO: make DevelopmentStatus an int enum and provide text marshal/unmarshal methods
	return statusRanks[s]
}

var statusRanks = map[DevelopmentStatus]int{
	Inactive: 0,
	Planning: 1,
	PreAlpha: 2,
	Alpha:    3,
	Beta:     4,
	Stable:   5,
	Mature:   6,
}

// InformationLoss in terms of which kind of code generation would they allow.
type InformationLoss string

const (
	// Lossless no information loss converting code to AST and then back to code
	// would. code == codegen(AST(code)).
	Lossless InformationLoss = "lossless"
	// FormatingLoss only superfluous formatting information is lost (e.g.
	// whitespace, indentation). Code generated from the AST could be the same
	// as the original code after passing a code formatter.
	// fmt(code) == codegen(AST(code)).
	FormatingLoss InformationLoss = "formating-loss"
	// SyntacticSugarLoss there is information loss about syntactic sugar. Code
	// generated from the AST could be the same as the original code after
	// desugaring it. desugar(code) == codegen(AST(code)).
	SyntacticSugarLoss InformationLoss = "syntactic-sugar-loss"
	// CommentLoss comments are not present in the AST.
	CommentLoss InformationLoss = "formating-loss"
)

// Feature describes which level of information driver can produce.
type Feature string

const (
	// AST is a basic feature required for the driver. Driver can parse files and return native language AST.
	AST Feature = "ast"
	// UAST feature indicates that driver properly converts AST to UAST without further annotating it.
	UAST Feature = "uast"
	// Roles feature indicates that driver annotates UAST with roles. All node types are annotated.
	Roles Feature = "roles"
)

type OS string

const (
	Alpine OS = "alpine"
	Debian OS = "debian"
)

func (os OS) AsImage() string {
	switch os {
	case Alpine:
		return "alpine:3.7"
	case Debian:
		return "debian:jessie-slim"
	default:
		return ""
	}
}

type Manifest struct {
	Name            string            `toml:"name"` // human-readable name
	Language        string            `toml:"language"`
	Version         string            `toml:"version,omitempty"`
	Build           *time.Time        `toml:"build,omitempty"`
	Status          DevelopmentStatus `toml:"status"`
	InformationLoss []InformationLoss `toml:"loss"`
	Documentation   struct {
		Description string `toml:"description,omitempty"`
		Caveats     string `toml:"caveats,omitempty"`
	} `toml:"documentation,omitempty"`
	Runtime struct {
		OS             OS       `toml:"os"`
		NativeVersion  Versions `toml:"native_version"`
		NativeEncoding string   `toml:"native_encoding"`
		GoVersion      string   `toml:"go_version"`
	} `toml:"runtime"`
	Features []Feature `toml:"features"`
}

// Supports checks if driver supports specified feature.
func (m Manifest) Supports(f Feature) bool {
	for _, f2 := range m.Features {
		if f == f2 {
			return true
		}
	}
	return false
}

type Versions []string

func (v Versions) String() string {
	return strings.Join(v, ":")
}

// Load reads a manifest and decode the content into a new Manifest struct
func Load(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	m := &Manifest{}
	return m, m.Decode(f)
}

// Encode encodes m in toml format and writes the restult to w
func (m *Manifest) Encode(w io.Writer) error {
	e := toml.NewEncoder(w)
	return e.Encode(m)
}

// Decode decodes reads r and decodes it into m
func (m *Manifest) Decode(r io.Reader) error {
	if _, err := toml.DecodeReader(r, m); err != nil {
		return err
	}

	return nil
}
