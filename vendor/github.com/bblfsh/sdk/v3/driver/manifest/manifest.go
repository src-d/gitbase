package manifest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/bblfsh/sdk/v3/internal/buildmanifest"

	"github.com/BurntSushi/toml"
	"github.com/rogpeppe/go-internal/modfile"
)

// CurrentSDKMajor returns a major version of this SDK package.
func CurrentSDKMajor() int {
	type dummy struct{}
	p := reflect.TypeOf(dummy{}).PkgPath()
	const (
		pref    = "/sdk.v"
		prefMod = "/sdk/v"
	)
	if i := strings.Index(p, pref); i > 0 {
		p = p[i+len(pref):]
	} else if i = strings.Index(p, prefMod); i > 0 {
		p = p[i+len(prefMod):]
	}
	i := strings.Index(p, "/")
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

type Documentation struct {
	Description string `toml:"description,omitempty" json:",omitempty"`
	Caveats     string `toml:"caveats,omitempty" json:",omitempty"`
}

type Manifest struct {
	Name          string            `toml:"name"` // human-readable name
	Language      string            `toml:"language"`
	Aliases       []string          `toml:"aliases"` // language name aliases, see Enry/Linguist
	Version       string            `toml:"version,omitempty" json:",omitempty"`
	Build         time.Time         `toml:"build,omitempty" json:",omitempty"`
	Status        DevelopmentStatus `toml:"status"`
	SDKVersion    string            `toml:"-"` // read from go.mod
	Documentation *Documentation    `toml:"documentation,omitempty" json:",omitempty"`
	Runtime       struct {
		NativeVersion string `toml:"-" json:",omitempty"`
		GoVersion     string `toml:"-" json:",omitempty"`
	} `toml:"-" json:"Runtimes"` // read from build.yml
	Features    []Feature    `toml:"features" json:",omitempty"`
	Maintainers []Maintainer `toml:"-" json:",omitempty"` // read from MAINTAINERS
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

// Load reads a manifest and decode the content into a new Manifest struct.
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
	read := InDir(dir)

	m.SDKVersion, err = SDKVersion(read)
	if err != nil {
		return m, err
	}
	if err := LoadRuntimeInfo(m, read); err != nil {
		return m, err
	}
	m.Maintainers, err = Maintainers(read)
	if err != nil {
		return m, err
	}
	return m, nil
}

// extractImageVersion parses a semantic version from a Docker image name having
// the form base:label, where label is either a version or version-tag pair.
func extractImageVersion(s string) string {
	if i := strings.LastIndexByte(s, ':'); i > 0 {
		s = s[i+1:]
	}
	if i := strings.LastIndexByte(s, '-'); i > 0 {
		s = s[:i]
	}
	return s
}

// LoadRuntimeInfo reads a build manifest file with a given open function and sets
// runtime-related information to m.
func LoadRuntimeInfo(m *Manifest, read ReadFunc) error {
	data, err := read(buildmanifest.Filename)
	if err != nil || data == nil {
		return err
	}
	var b buildmanifest.Manifest
	if err := b.Decode(data); err != nil {
		return err
	} else if b.Format != buildmanifest.CurrentFormat {
		return fmt.Errorf("unknown format: %q", b.Format)
	}
	if b.Native.Build.Image != "" {
		// prefer image used to build the driver
		m.Runtime.NativeVersion = extractImageVersion(b.Native.Build.Image)
	} else if b.Native.Build.Gopath != "" {
		// for Go the image is the same as the server runtime
		m.Runtime.NativeVersion = extractImageVersion(b.Runtime.Version)
	} else if b.Native.Image != "" {
		m.Runtime.NativeVersion = extractImageVersion(b.Native.Image)
	}
	if b.Runtime.Version != "" {
		m.Runtime.GoVersion = extractImageVersion(b.Runtime.Version)
	}
	return nil
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

// ReadFunc is a function for fetching a file using a relative file path.
// It returns an empty error a nil slice in case file does not exist.
type ReadFunc func(path string) ([]byte, error)

// InDir returns a function that read files in the specified directory.
func InDir(dir string) ReadFunc {
	return func(path string) ([]byte, error) {
		if filepath.IsAbs(path) {
			return nil, fmt.Errorf("expected relative path, got: %q", path)
		}
		data, err := ioutil.ReadFile(filepath.Join(dir, path))
		if os.IsNotExist(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return data, nil
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
func Maintainers(read ReadFunc) ([]Maintainer, error) {
	if read == nil {
		read = currentDir
	}
	data, err := read("MAINTAINERS")
	if err != nil {
		return nil, err
	} else if data == nil {
		return nil, nil
	}

	list := parseMaintainers(bytes.NewReader(data))
	return list, nil
}

const (
	sdkNameLegacy = "gopkg.in/bblfsh/sdk"
	sdkName       = "github.com/bblfsh/sdk"
)

// maxVersionToml finds a maximal version of a Babelfish SDK in the list of the projects.
func maxVersionToml(projs []map[string]interface{}) string {
	max := ""
	for _, p := range projs {
		name, _ := p["name"].(string)
		if !strings.HasPrefix(name, sdkNameLegacy) {
			continue
		}
		vers, _ := p["version"].(string)
		vers = strings.TrimPrefix(vers, "v")
		vers = strings.TrimRight(vers, ".x")
		if vers == "" || !unicode.IsDigit(rune(vers[0])) {
			vers = strings.TrimPrefix(name, sdkNameLegacy+".v")
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

// maxVersionMod finds a maximal version of a Babelfish SDK in the list of the projects.
func maxVersionMod(req []*modfile.Require) string {
	max := ""
	for _, p := range req {
		if p.Indirect {
			continue
		}
		vers := strings.TrimPrefix(p.Mod.Version, "v")
		prefix := ""
		switch {
		case strings.HasPrefix(p.Mod.Path, sdkName):
			prefix = sdkName + "/v"
		case strings.HasPrefix(p.Mod.Path, sdkNameLegacy):
			prefix = sdkNameLegacy + ".v"
		default:
			continue
		}
		if i := strings.IndexByte(vers, '-'); i >= 0 {
			vers = vers[:i]
		}
		if i := strings.IndexByte(vers, '+'); i >= 0 {
			vers = vers[:i]
		}
		if vers == "" || vers == "0.0.0" {
			vers = strings.TrimPrefix(p.Mod.Path, prefix)
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
func SDKVersion(read ReadFunc) (string, error) {
	if data, err := read("go.mod"); err == nil && data != nil {
		mod, err := modfile.Parse("go.mod", data, nil)
		if err != nil {
			return "", err
		}
		max := maxVersionMod(mod.Require)
		if max != "" {
			return max, nil
		}
	}
	if data, err := read("Gopkg.lock"); err == nil && data != nil {
		var m map[string]interface{}
		_, err := toml.Decode(string(data), &m)
		if err != nil {
			return "", err
		}
		projs, _ := m["projects"].([]map[string]interface{})
		max := maxVersionToml(projs)
		if max != "" {
			return max, nil
		}
	}
	if data, err := read("Gopkg.toml"); err == nil && data != nil {
		var m map[string]interface{}
		_, err := toml.Decode(string(data), &m)
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
