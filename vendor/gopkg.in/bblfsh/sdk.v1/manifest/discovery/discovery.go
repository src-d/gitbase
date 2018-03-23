// Package discovery package implements helpers for clients to discover language drivers supported by Babelfish.
package discovery

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/google/go-github/github"
	"gopkg.in/bblfsh/sdk.v1/manifest"
)

const (
	GithubOrg = "bblfsh"
)

// topics that each driver repository on Github should be annotated with
var topics = []string{
	"babelfish", "driver",
}

// Driver is an object describing language driver and it's repository-related information.
type Driver struct {
	manifest.Manifest
	repo        *github.Repository
	Maintainers []Maintainer `json:",omitempty"`
}

type byStatusAndName []Driver

func (arr byStatusAndName) Len() int {
	return len(arr)
}

func (arr byStatusAndName) Less(i, j int) bool {
	a, b := arr[i], arr[j]
	// sort by status, features count, name
	if s1, s2 := a.Status.Rank(), b.Status.Rank(); s1 > s2 {
		return true
	} else if s1 < s2 {
		return false
	}
	if n1, n2 := len(a.Features), len(b.Features); n1 > n2 {
		return true
	} else if n1 < n2 {
		return false
	}
	return a.Language < b.Language
}

func (arr byStatusAndName) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
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

// InDevelopment indicates that driver is incomplete and should only be used for development purposes.
func (d Driver) InDevelopment() bool {
	return d.Status.Rank() < manifest.Alpha.Rank()
}

// IsRecommended indicates that driver is stable enough to be used in production.
func (d Driver) IsRecommended() bool {
	return d.Status.Rank() >= manifest.Beta.Rank()
}

// RepositoryURL returns Github repository URL for browsers (not git).
func (d Driver) RepositoryURL() string {
	return d.repo.GetHTMLURL()
}

// repositoryFileURL returns an URL of file in the driver's repository.
func (d Driver) repositoryFileURL(path string) string {
	path = strings.TrimPrefix(path, "/")
	return fmt.Sprintf("https://raw.githubusercontent.com/%s/master/%s", d.repo.GetFullName(), path)
}

// newReq constructs a GET request with context.
func newReq(ctx context.Context, url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		// if it fails, it's a programmer's error
		panic(err)
	}
	return req.WithContext(ctx)
}

// loadManifest reads manifest file from repository and decodes it into object.
func (d *Driver) loadManifest(ctx context.Context) error {
	req := newReq(ctx, d.repositoryFileURL(manifest.Filename))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// outdated driver
		d.Name = d.Language
		d.Status = manifest.Inactive
		return nil
	} else if resp.StatusCode/100 != 2 { // 2xx
		return fmt.Errorf("status: %v", resp.Status)
	}

	lang := d.Language
	if err := d.Manifest.Decode(resp.Body); err != nil {
		return err
	}
	// override language ID from manifest (prevents copy-paste of manifests)
	d.Language = lang
	if d.Name == "" {
		d.Name = d.Language
	}
	return nil
}

// reMaintainer is a regexp for one line of MAINTAINERS file (Github handle is optional):
//
//		John Doe <john@domain.com> (@john_at_github)
var reMaintainer = regexp.MustCompile(`^([^<(]+)\s<([^>]+)>(\s\(@([^\s]+)\))?`)

// parseMaintainers parses the MAINTAINERS file.
//
// Each line in a file should follow the format defined by reMaintainer regexp.
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

// loadMaintainers reads MAINTAINERS file from repository and decodes it into object.
func (d *Driver) loadMaintainers(ctx context.Context) error {
	req := newReq(ctx, d.repositoryFileURL("MAINTAINERS"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	} else if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status: %v", resp.Status)
	}
	d.Maintainers = parseMaintainers(resp.Body)
	return nil
}

// Options controls how drivers are being discovered and what information is fetched for them.
type Options struct {
	Organization  string // Github organization name
	NamesOnly     bool   // driver manifest will only have Language field populated
	NoMaintainers bool   // do not load maintainers list
}

// getDriversForOrg lists all repositories for an organization and filters ones that contains topics of the driver.
func getDriversForOrg(ctx context.Context, org string) ([]Driver, error) {
	cli := github.NewClient(nil)

	var out []Driver
	// list all repositories in organization
	for page := 1; ; page++ {
		repos, _, err := cli.Repositories.ListByOrg(ctx, org, &github.RepositoryListByOrgOptions{
			ListOptions: github.ListOptions{
				Page: page, PerPage: 100,
			},
			Type: "public",
		})
		if err != nil {
			return out, err
		} else if len(repos) == 0 {
			break
		}
		for _, r := range repos {
			// filter repos by topics to find babelfish drivers
			if containsTopics(r.Topics, topics...) {
				out = append(out, Driver{
					Manifest: manifest.Manifest{
						Language: strings.TrimSuffix(r.GetName(), "-driver"),
					},
					repo: r,
				})
			}
		}
	}
	return out, nil
}

// OfficialDrivers lists all available language drivers for Babelfish.
func OfficialDrivers(ctx context.Context, opt *Options) ([]Driver, error) {
	if opt == nil {
		opt = &Options{}
	}
	if opt.Organization == "" {
		opt.Organization = GithubOrg
	}
	out, err := getDriversForOrg(ctx, opt.Organization)
	if err != nil {
		return out, err
	}
	if opt.NamesOnly {
		sort.Sort(byStatusAndName(out))
		return out, nil
	}

	// load manifest and maintainers file from repositories
	var (
		wg sync.WaitGroup
		// limits the number of concurrent requests
		tokens = make(chan struct{}, 3)

		mu   sync.Mutex
		last error
	)

	setErr := func(err error) {
		mu.Lock()
		last = err
		mu.Unlock()
	}
	for i := range out {
		wg.Add(1)
		go func(d *Driver) {
			defer wg.Done()

			tokens <- struct{}{}
			defer func() {
				<-tokens
			}()
			if err := d.loadManifest(ctx); err != nil {
				setErr(err)
			}
			if !opt.NoMaintainers {
				if err := d.loadMaintainers(ctx); err != nil {
					setErr(err)
				}
			}
		}(&out[i])
	}
	wg.Wait()
	sort.Sort(byStatusAndName(out))
	return out, last
}

// containsTopics returns true if all inc topics are present in the list.
func containsTopics(topics []string, inc ...string) bool {
	n := 0
	for _, t := range topics {
		ok := false
		for _, t2 := range inc {
			if t == t2 {
				ok = true
				break
			}
		}
		if ok {
			n++
		}
	}
	return n == len(inc)
}
