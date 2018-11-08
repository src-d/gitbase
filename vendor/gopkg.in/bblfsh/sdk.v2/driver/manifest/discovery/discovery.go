// Package discovery package implements helpers for clients to discover language drivers supported by Babelfish.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/google/go-github/github"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
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
	repo *github.Repository
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
//
// Deprecated: use manifest.Maintainer
type Maintainer = manifest.Maintainer

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

// fetchFromGithub returns a manifest.OpenFunc that is bound to context and fetches files directly from Github.
func (d Driver) fetchFromGithub(ctx context.Context) manifest.OpenFunc {
	return func(path string) (io.ReadCloser, error) {
		req := newReq(ctx, d.repositoryFileURL(path))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, nil
		} else if resp.StatusCode/100 != 2 {
			resp.Body.Close()
			return nil, fmt.Errorf("status: %v", resp.Status)
		}
		return resp.Body, nil
	}
}

// loadMaintainers reads MAINTAINERS file from repository and decodes it into object.
func (d *Driver) loadMaintainers(ctx context.Context) error {
	list, err := manifest.Maintainers(d.fetchFromGithub(ctx))
	if err != nil {
		return err
	}
	d.Maintainers = list
	return nil
}

// loadSDKVersion reads SDK version from repository and decodes it into object.
func (d *Driver) loadSDKVersion(ctx context.Context) error {
	vers, err := manifest.SDKVersion(d.fetchFromGithub(ctx))
	if err != nil {
		return err
	}
	d.SDKVersion = vers
	return nil
}

// Options controls how drivers are being discovered and what information is fetched for them.
type Options struct {
	Organization  string // Github organization name
	NamesOnly     bool   // driver manifest will only have Language field populated
	NoMaintainers bool   // do not load maintainers list
	NoSDKVersion  bool   // do not check SDK version
	NoStatic      bool   // do not use a static manifest - discover drivers
}

// isRateLimit checks if error is due to rate limiting.
func isRateLimit(err error) bool {
	_, ok := err.(*github.RateLimitError)
	return ok
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

const staticDriversURL = `https://raw.githubusercontent.com/` + GithubOrg + `/documentation/master/languages.json`

// getStaticDrivers downloads a static drivers list hosted by Babelfish org.
func getStaticDrivers(ctx context.Context) ([]Driver, error) {
	req, err := http.NewRequest("GET", staticDriversURL, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cannot download static driver list: status: %v", resp.Status)
	}
	var drivers []Driver
	err = json.NewDecoder(resp.Body).Decode(&drivers)
	if err != nil {
		return nil, fmt.Errorf("cannot decode static driver list: %v", err)
	}
	return drivers, nil
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
	if isRateLimit(err) && opt.Organization == GithubOrg && !opt.NoStatic {
		return getStaticDrivers(ctx)
	} else if err != nil {
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
			if !opt.NoSDKVersion {
				if err := d.loadSDKVersion(ctx); err != nil {
					setErr(err)
				}
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
