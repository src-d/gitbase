package manifest

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/BurntSushi/toml"
)

// CurrentSDKMajor returns a major version of this SDK package.
func CurrentSDKMajor() int {
	type dummy struct{}
	p := reflect.TypeOf(dummy{}).PkgPath()
	const pref = "/sdk.v"
	i := strings.Index(p, pref)
	p = p[i+len(pref):]
	i = strings.Index(p, "/")
	if i > 0 {
		p = p[:i]
	}
	v, err := strconv.Atoi(p)
	if err != nil {
		panic(err)
	}
	return v
}

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

type Documentation struct {
	Description string `toml:"description,omitempty" json:",omitempty"`
	Caveats     string `toml:"caveats,omitempty" json:",omitempty"`
}

type Manifest struct {
	Name            string            `toml:"name"` // human-readable name
	Language        string            `toml:"language"`
	Version         string            `toml:"version,omitempty" json:",omitempty"`
	Build           *time.Time        `toml:"build,omitempty" json:",omitempty"`
	Status          DevelopmentStatus `toml:"status"`
	InformationLoss []InformationLoss `toml:"loss" json:",omitempty"`
	SDKVersion      string            `toml:"-"` // do not read it from manifest.toml
	Documentation   *Documentation    `toml:"documentation,omitempty" json:",omitempty"`
	Runtime         struct {
		OS             OS       `toml:"os" json:",omitempty"`
		NativeVersion  Versions `toml:"native_version" json:",omitempty"`
		NativeEncoding string   `toml:"native_encoding" json:",omitempty"`
		GoVersion      string   `toml:"go_version" json:",omitempty"`
	} `toml:"runtime"`
	Features    []Feature    `toml:"features" json:",omitempty"`
	Maintainers []Maintainer `toml:"-" json:",omitempty"`
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

// SDKMajor returns a major version of SDK this driver was built for.
func (m Manifest) SDKMajor() int {
	vers := m.SDKVersion
	if vers == "" {
		return 0
	}
	if i := strings.Index(vers, "."); i >= 0 {
		vers = vers[:i]
	}
	v, err := strconv.Atoi(vers)
	if err != nil {
		panic(err)
	}
	return v
}

// ForCurrentSDK indicates that driver is built for the same major version of SDK.
func (m Manifest) ForCurrentSDK() bool {
	return m.SDKMajor() == CurrentSDKMajor()
}

// InDevelopment indicates that driver is incomplete and should only be used for development purposes.
func (m Manifest) InDevelopment() bool {
	return m.Status.Rank() < Alpha.Rank()
}

// IsRecommended indicates that driver is stable enough to be used in production.
func (m Manifest) IsRecommended() bool {
	return m.ForCurrentSDK() && m.Status.Rank() >= Beta.Rank()
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
	if err := m.Decode(f); err != nil {
		return m, err
	}
	dir := filepath.Dir(path)
	open := InDir(dir)

	m.SDKVersion, err = SDKVersion(open)
	if err != nil {
		return m, err
	}
	m.Maintainers, err = Maintainers(open)
	if err != nil {
		return m, err
	}
	return m, nil
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

// Maintainer is an information about project maintainer.
type Maintainer struct {
	Name   string `json:",omitempty"`
	Email  string `json:",omitempty"`
	Github string `json:",omitempty"` // github handle
}

// GithubURL returns github profile URL.
func (m Maintainer) GithubURL() string {
	if m.Github != "" {
		return `https://github.com/` + m.Github
	}
	return ""
}

// URL returns a contact of the maintainer (either Github profile or email link).
func (m Maintainer) URL() string {
	if m.Github != "" {
		return m.GithubURL()
	} else if m.Email != "" {
		return `mailto:` + m.Email
	}
	return ""
}

// OpenFunc is a function for fetching a file using a relative file path.
// It returns an empty error an nil reader in case file does not exist.
type OpenFunc func(path string) (io.ReadCloser, error)

// InDir returns a function that read files in the specified directory.
func InDir(dir string) OpenFunc {
	return func(path string) (io.ReadCloser, error) {
		if filepath.IsAbs(path) {
			return nil, fmt.Errorf("expected relative path, got: %q", path)
		}
		f, err := os.Open(filepath.Join(dir, path))
		if os.IsNotExist(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return f, nil
	}
}

var currentDir = InDir("")

// reMaintainer is a regexp for one line of MAINTAINERS file (Github handle is optional):
//
//		John Doe <john@domain.com> (@john_at_github)
var reMaintainer = regexp.MustCompile(`^([^<(]+)\s<([^>]+)>(\s\(@([^\s]+)\))?`)

// parseMaintainers parses the MAINTAINERS file. It will ignore lines that does not match the reMaintainer regexp.
func parseMaintainers(r io.Reader) []Maintainer {
	var out []Maintainer
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		sub := reMaintainer.FindStringSubmatch(line)
		if len(sub) == 0 {
			continue
		}
		m := Maintainer{Name: sub[1], Email: sub[2]}
		if len(sub) >= 5 {
			m.Github = sub[4]
		}
		out = append(out, m)
	}
	return out
}

// Maintainers reads and parses the MAINTAINERS file using the provided function.
func Maintainers(open OpenFunc) ([]Maintainer, error) {
	if open == nil {
		open = currentDir
	}
	rc, err := open("MAINTAINERS")
	if err != nil {
		return nil, err
	} else if rc == nil {
		return nil, nil
	}
	defer rc.Close()

	list := parseMaintainers(rc)
	return list, nil
}

const sdkName = "gopkg.in/bblfsh/sdk"

// maxVersionToml finds a maximal version of a Babelfish SDK in the list of the projects.
func maxVersionToml(projs []map[string]interface{}) string {
	max := ""
	for _, p := range projs {
		name, _ := p["name"].(string)
		if !strings.HasPrefix(name, sdkName) {
			continue
		}
		vers, _ := p["version"].(string)
		vers = strings.TrimPrefix(vers, "v")
		vers = strings.TrimRight(vers, ".x")
		if vers == "" || !unicode.IsDigit(rune(vers[0])) {
			vers = strings.TrimPrefix(name, sdkName+".v")
			if max < vers {
				max = vers
			}
			continue
		}
		if max < vers {
			max = vers
		}
	}
	return max
}

// SDKVersion detects a Babelfish SDK version of a driver. Returned format is "x[.y[.z]]".
func SDKVersion(open OpenFunc) (string, error) {
	if f, err := open("Gopkg.lock"); err == nil && f != nil {
		defer f.Close()

		var m map[string]interface{}
		_, err := toml.DecodeReader(f, &m)
		if err != nil {
			return "", err
		}
		projs, _ := m["projects"].([]map[string]interface{})
		max := maxVersionToml(projs)
		if max != "" {
			return max, nil
		}
	}
	if f, err := open("Gopkg.toml"); err == nil && f != nil {
		defer f.Close()

		var m map[string]interface{}
		_, err := toml.DecodeReader(f, &m)
		if err != nil {
			return "", err
		}
		projs, _ := m["constraint"].([]map[string]interface{})
		max := maxVersionToml(projs)
		if max != "" {
			return max, nil
		}
	}
	return "1", nil
}
