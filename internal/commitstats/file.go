package commitstats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/hhatto/gocloc"
	"github.com/src-d/enry/v2"
	"github.com/src-d/go-git/utils/binary"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

// CommitFileStats represents the stats for a file in a commit.
type CommitFileStats struct {
	Path     string
	Language string
	Code     KindStats
	Comment  KindStats
	Blank    KindStats
	Other    KindStats
	Total    KindStats
}

// CalculateByFile calculates the stats for all files from a commit to another.
// If from is nil, the first parent is used. if the commit is an orphan,
// the stats are compared against an empty commit.
func CalculateByFile(r *git.Repository, from, to *object.Commit) ([]CommitFileStats, error) {
	var err error
	if to.NumParents() != 0 && from == nil {
		from, err = to.Parent(0)
		if err != nil {
			return nil, err
		}
	}

	if from == nil {
		return fileStatsFromCommit(to)
	}

	return fileStatsFromDiff(r, from, to)
}

func fileStatsFromCommit(c *object.Commit) ([]CommitFileStats, error) {
	var result []CommitFileStats
	files, err := c.Files()
	if err != nil {
		return nil, err
	}

	err = files.ForEach(func(f *object.File) error {
		lang := getLanguage(f.Name)
		fi, err := blobFileStats(&f.Blob, f.Name, lang)
		if err != nil {
			if err == errIgnored {
				return nil
			}
			return err
		}

		stats := commitFileStatsFromFileStats(fi, f.Name, lang)
		result = append(result, stats)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func commitFileStatsFromFileStats(fi fileStats, path, lang string) CommitFileStats {
	stats := CommitFileStats{
		Path: path,
	}

	if fi != nil {
		stats.Language = lang
		for _, info := range fi {
			fillKindStats(&stats.Total, info)
			switch info.Kind {
			case Code:
				fillKindStats(&stats.Code, info)
			case Comment:
				fillKindStats(&stats.Comment, info)
			case Blank:
				fillKindStats(&stats.Blank, info)
			case Other:
				fillKindStats(&stats.Other, info)
			}
		}
	}

	return stats
}

func fileStatsFromDiff(r *git.Repository, from, to *object.Commit) ([]CommitFileStats, error) {
	ch, err := computeDiff(from, to)
	if err != nil {
		return nil, err
	}

	var result []CommitFileStats
	for _, change := range ch {
		s, err := fileStatsFromChange(r, change)
		if err != nil {
			if err == errIgnored {
				continue
			}
			return nil, err
		}

		result = append(result, s)
	}

	return result, nil
}

func fileStatsFromChange(r *git.Repository, ch *object.Change) (CommitFileStats, error) {
	a, err := ch.Action()
	if err != nil {
		return CommitFileStats{}, err
	}

	var fi fileStats
	var name string

	switch a {
	case merkletrie.Delete:
		name = ch.From.Name
		fi, err = changeEntryFileStats(r, &ch.From)
		if err != nil {
			return CommitFileStats{}, err
		}
	case merkletrie.Insert:
		name = ch.To.Name
		fi, err = changeEntryFileStats(r, &ch.To)
		if err != nil {
			return CommitFileStats{}, err
		}
	case merkletrie.Modify:
		src, err := changeEntryFileStats(r, &ch.From)
		if err != nil {
			return CommitFileStats{}, err
		}

		name = ch.To.Name
		dst, err := changeEntryFileStats(r, &ch.To)
		if err != nil {
			return CommitFileStats{}, err
		}

		if src == nil {
			src = make(fileStats)
		}

		if dst == nil {
			dst = make(fileStats)
		}

		dst.sub(src)
		fi = dst
	}

	return commitFileStatsFromFileStats(fi, name, getLanguage(name)), nil
}

var errIgnored = errors.New("ignored file")

var languages = gocloc.NewDefinedLanguages()

type fileStats map[string]*LineInfo

// LineInfo represents the information about a sigle line.
type LineInfo struct {
	Kind  LineKind
	Count int
}

func newFileStats(f *object.Blob, lang string) (fileStats, error) {
	ff := make(fileStats, 50)

	r, err := f.Reader()
	if err != nil {
		return ff, err
	}

	defer ioutil.CheckClose(r, &err)

	l, ok := languages.Langs[lang]
	if ok {
		fillStatsCloc(r, l, &ff)
		return ff, nil
	}

	return ff, fillStatsPlaintext(r, &ff)
}

func fillStatsCloc(r io.Reader, l *gocloc.Language, ff *fileStats) {
	gocloc.AnalyzeReader("", l, r, &gocloc.ClocOptions{
		OnBlank:   ff.addBlank,
		OnCode:    ff.addCode,
		OnComment: ff.addComment,
	})
}

func fillStatsPlaintext(r io.Reader, ff *fileStats) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		ff.addOther(s.Text())
	}

	return s.Err()
}

func (fi fileStats) addCode(line string)    { fi.add(line, Code) }
func (fi fileStats) addComment(line string) { fi.add(line, Comment) }
func (fi fileStats) addBlank(line string)   { fi.add(line, Blank) }
func (fi fileStats) addOther(line string)   { fi.add(line, Other) }
func (fi fileStats) add(line string, k LineKind) {
	if fi[line] == nil {
		fi[line] = &LineInfo{}
	}

	fi[line].Count++
	fi[line].Kind = k
}

func (fi fileStats) sub(to fileStats) {
	for line, i := range to {
		if _, ok := fi[line]; ok {
			fi[line].Count -= i.Count
		} else {
			fi[line] = i
			fi[line].Count *= -1
		}
	}
}

func fillKindStats(ks *KindStats, info *LineInfo) {
	if info.Count > 0 {
		ks.Additions += info.Count
	}
	if info.Count < 0 {
		ks.Deletions += (info.Count * -1)
	}
}

func (fi fileStats) String() string {
	buf := bytes.NewBuffer(nil)
	for line, i := range fi {
		sign := ' '
		switch {
		case i.Count > 0:
			sign = '+'
		case i.Count < 0:
			sign = '-'
		}

		fmt.Fprintf(buf, "%c [%3dx] %s\n", sign, i.Count, line)
	}

	return buf.String()
}

func blobFileStats(blob *object.Blob, filename, lang string) (fileStats, error) {
	if enry.IsVendor(filename) {
		return nil, errIgnored
	}

	isBinary, err := isBinary(blob)
	if err != nil {
		return nil, err
	}

	if isBinary {
		return nil, nil
	}

	return newFileStats(blob, lang)
}

func getLanguage(filename string) string {
	if lang, ok := enry.GetLanguageByFilename(filename); ok {
		return lang
	}

	if lang, ok := enry.GetLanguageByExtension(filename); ok {
		return lang
	}

	return ""
}

func isBinary(b *object.Blob) (bin bool, err error) {
	reader, err := b.Reader()
	if err != nil {
		return false, err
	}

	defer ioutil.CheckClose(reader, &err)
	return binary.IsBinary(reader)
}

func computeDiff(from, to *object.Commit) (object.Changes, error) {
	src, err := to.Tree()
	if err != nil {
		return nil, err
	}

	dst, err := from.Tree()
	if err != nil {
		return nil, err
	}

	return object.DiffTree(dst, src)
}

func changeEntryFileStats(r *git.Repository, ch *object.ChangeEntry) (fileStats, error) {
	blob, err := r.BlobObject(ch.TreeEntry.Hash)
	if err != nil {
		return nil, err
	}

	return blobFileStats(blob, ch.Name, getLanguage(ch.Name))
}
