package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/BurntSushi/toml"
	git "gopkg.in/src-d/go-git.v4"
)

const lockFile = "Gopkg.lock"

type project struct {
	Name     string
	Revision string
}

type projects struct {
	Projects []project
}

func init() {
	flag.Usage = func() {
		fmt.Println("\ngo run ./tools/rev-upgrade/main.go [-p \"project name\"] [-r \"revision\"]")
		flag.PrintDefaults()
	}
}

func main() {
	var (
		prj    string
		newRev string
		oldRev string

		w   *git.Worktree
		err error
	)

	flag.StringVar(&prj, "p", "gopkg.in/src-d/go-mysql-server.v0", "project name (e.g.: gopkg.in/src-d/go-mysql-server.v0)")
	flag.StringVar(&newRev, "r", "", "revision (by default the latest allowed by Gopkg.toml)")
	flag.Parse()

	if prj == "" {
		log.Fatalln("Project's name cannot be an empty string")
	}

	w, err = worktree()
	if err != nil {
		log.Fatalln(err)
	}

	oldRev, err = revision(filepath.Join(w.Filesystem.Root(), "Gopkg.lock"), prj)
	if err != nil {
		log.Fatalf("Current revision of %s is an empty string (%s)", prj, err)
	}

	if oldRev == newRev {
		return
	}

	defer func() {
		if err != nil {
			log.Println(err)
			w.Reset(&git.ResetOptions{Mode: git.MixedReset})
		} else {
			// let commit manually
		}
	}()

	if newRev != "" {
		fmt.Printf("Project: %s\nOld rev: %s\nNew rev: %s\n", prj, oldRev, newRev)

		if err = replace(w, oldRev, newRev); err != nil {
			return
		}
	}

	err = ensure(prj)
	if err != nil {
		return
	}

	if newRev == "" {
		newRev, err = revision(filepath.Join(w.Filesystem.Root(), "Gopkg.lock"), prj)
		fmt.Printf("Project: %s\nOld rev: %s\nNew rev: %s\n", prj, oldRev, newRev)
		if newRev == oldRev {
			return
		}

		if err = replace(w, oldRev, newRev); err != nil {
			return
		}
	}
}

// repo's worktree
func worktree() (*git.Worktree, error) {
	repo, err := git.PlainOpenWithOptions(".", &git.PlainOpenOptions{DetectDotGit: true})
	if err != nil {
		return nil, err
	}

	return repo.Worktree()
}

// project's current revision
func revision(gopkg string, prj string) (string, error) {
	data, err := ioutil.ReadFile(gopkg)
	if err != nil {
		return "", err
	}
	var projects = projects{}
	if err = toml.Unmarshal(data, &projects); err != nil {
		return "", err
	}
	for _, p := range projects.Projects {
		if p.Name == prj {
			return p.Revision, nil
		}
	}
	return "", io.EOF
}

func replace(w *git.Worktree, oldRev, newRev string) error {
	rexp, err := regexp.Compile(oldRev)
	if err != nil {
		return err
	}

	res, err := w.Grep(&git.GrepOptions{Patterns: []*regexp.Regexp{rexp}})
	if err != nil {
		return err
	}

	files := make(map[string]struct{})
	for _, r := range res {
		// ignore replacements on lockfile so update works
		if r.FileName == lockFile {
			continue
		}

		if _, ok := files[r.FileName]; !ok {
			files[r.FileName] = struct{}{}
		}
	}

	// replace oldRev by newRev in place
	var (
		wg sync.WaitGroup
	)
	for f := range files {
		wg.Add(1)
		go func(filename string, old, new []byte) {
			defer wg.Done()

			d, e := ioutil.ReadFile(filename)
			if e != nil {
				err = e
				return
			}

			d = bytes.Replace(d, old, new, -1)

			e = ioutil.WriteFile(filename, d, 0)
			if e != nil {
				err = e
			}

			fmt.Println("#", filename)
		}(filepath.Join(w.Filesystem.Root(), f), []byte(oldRev), []byte(newRev))
	}
	wg.Wait()

	return err
}

func ensure(prj string) error {
	cmd := exec.Command("dep", "ensure", "-v", "-update", prj)
	out, err := cmd.CombinedOutput()
	fmt.Println(string(out))
	if err != nil {
		return err
	}

	return nil
}
