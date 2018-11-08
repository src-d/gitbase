package cmd

import (
	"bytes"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode"

	"gopkg.in/bblfsh/sdk.v2/assets/skeleton"
	"gopkg.in/bblfsh/sdk.v2/build"
	"gopkg.in/bblfsh/sdk.v2/cmd"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
)

const (
	tplExtension = ".tpl"
	manifestName = "manifest.toml"
	manifestTpl  = manifestName + ".tpl"
)

var overwriteManagedFiles = os.Getenv("BABELFISH_OVERWRITE_MANAGED") == "true"

// managedFiles are files that always are overwritten
var managedFiles = map[string]bool{
	".travis.yml":                         true,
	"Makefile":                            true,
	"README.md.tpl":                       true,
	"LICENSE":                             true,
	"driver/main.go.tpl":                  true,
	"driver/normalizer/transforms.go.tpl": true,
}

const UpdateCommandDescription = "updates an already initialized driver"

type UpdateCommand struct {
	DryRun bool `long:"dry-run" description:"don't writes nothing just checks if something should be written"`

	changes int
	context map[string]interface{}
	cmd.Command
}

func (c *UpdateCommand) Execute(args []string) error {
	m, err := c.readManifest()
	if err != nil {
		return err
	}

	c.context = map[string]interface{}{
		"Manifest": m,
	}

	for _, file := range skeleton.AssetNames() {
		if file == manifestTpl {
			continue
		}

		if err := c.processAsset(file); err != nil {
			return err
		}
	}
	d, err := build.NewDriver(c.Root)
	if err != nil {
		return err
	}
	var changed bool
	if c.DryRun {
		changed, err = d.ScriptChanged()
	} else {
		changed, err = d.Prepare()
	}
	if err != nil {
		return err
	} else if changed {
		c.notifyChangedFile(build.ScriptName)
	}

	if c.DryRun && c.changes > 0 {
		return fmt.Errorf("changes are required")
	}

	return nil
}

func (c *UpdateCommand) processAsset(name string) error {
	overwrite := managedFiles[name]
	if overwriteManagedFiles {
		overwrite = false
	}

	if strings.HasSuffix(name, tplExtension) {
		return c.processTemplateAsset(name, c.context, overwrite)
	}

	return c.processFileAsset(name, overwrite)
}

func (c *UpdateCommand) processFileAsset(name string, overwrite bool) error {
	content := skeleton.MustAsset(name)
	info, _ := skeleton.AssetInfo(name)

	name = fixGitFolder(name)
	return c.writeFile(filepath.Join(c.Root, name), content, info.Mode(), overwrite)
}

var funcs = map[string]interface{}{
	"escape_shield": escapeShield,
	"expName":       expName,
}

func (c *UpdateCommand) processTemplateAsset(name string, v interface{}, overwrite bool) error {
	tpl := string(skeleton.MustAsset(name))

	t, err := template.New(name).Funcs(funcs).Parse(tpl)
	if err != nil {
		return err
	}

	name = fixGitFolder(name)
	file := filepath.Join(c.Root, name[:len(name)-len(tplExtension)])

	buf := bytes.NewBuffer(nil)
	if err := t.Execute(buf, v); err != nil {
		return err
	}

	info, _ := skeleton.AssetInfo(name)
	return c.writeFile(file, buf.Bytes(), info.Mode(), overwrite)
}

func (c *UpdateCommand) writeFile(file string, content []byte, m os.FileMode, overwrite bool) error {
	f, err := os.Open(file)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if f == nil {
		c.notifyMissingFile(file)
		return c.doWriteFile(file, content, m)
	}

	if !overwrite {
		return nil
	}

	original, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	if bytes.Compare(original, content) == 0 {
		return nil
	}

	c.notifyChangedFile(file)
	return c.doWriteFile(file, content, m)
}

func (c *UpdateCommand) doWriteFile(file string, content []byte, m os.FileMode) error {
	if c.DryRun {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, m)
	if err != nil {
		return err
	}

	defer f.Close()

	if c.Verbose {
		cmd.Debug.Printf("file %q has been written\n", file)
	}

	_, err = f.Write(content)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(c.Root, file)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(rel, ".git"+string(filepath.Separator)) {
		git := exec.Command("git", "add", rel)
		git.Dir = c.Root
		if out, err := git.CombinedOutput(); err != nil {
			cmd.Warning.Println("cannot add a file to git:", err, "\n"+string(out))
		}
	}
	return nil
}

func (c *UpdateCommand) readManifest() (*manifest.Manifest, error) {
	return manifest.Load(filepath.Join(c.Root, manifest.Filename))
}

func (c *UpdateCommand) notifyMissingFile(file string) {

	if !c.DryRun {
		cmd.Notice.Printf("creating file %q\n", file)
		return
	}

	if isDotGit(file) {
		return
	}

	c.changes++
	cmd.Warning.Printf("missing file %q\n", file)
}

func (c *UpdateCommand) notifyChangedFile(file string) {
	if !c.DryRun {
		cmd.Warning.Printf("managed file %q has changed, discarding changes\n", file)
		return

	}

	c.changes++
	cmd.Warning.Printf("managed file changed %q\n", file)
}

func escapeShield(text interface{}) string {
	return strings.Replace(fmt.Sprintf("%s", text), "-", "--", -1)
}

func expName(s string) string {
	if len(s) == 0 {
		return ""
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

func fixGitFolder(path string) string {
	return strings.Replace(path, "git/", ".git/", 1)
}

func isDotGit(path string) bool {
	return strings.Contains(path, ".git/")
}
