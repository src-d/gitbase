package utils

import (
	"bytes"
	"fmt"

	"github.com/hhatto/gocloc"
	"github.com/src-d/enry"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/binary"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type LineKind int

const (
	Code LineKind = iota + 1
	Comment
	Blank
)

type CommitStatsCalculator struct {
	r *git.Repository
	c *object.Commit
	p *object.Commit

	src map[string]LineKind
	dst map[string]LineKind
}

func NewCommitStatsCalculator(r *git.Repository, c *object.Commit) *CommitStatsCalculator {
	return &CommitStatsCalculator{r: r, c: c}
}

func (c *CommitStatsCalculator) Do() (*Stats, error) {
	var err error
	c.p, err = c.c.Parent(0)
	if err != nil {
		return nil, nil
	}

	return c.doLines(c.c, c.dst)
}

func (c *CommitStatsCalculator) doLines(commit *object.Commit, m map[string]LineKind) (*Stats, error) {
	src, _ := c.c.Tree()
	dst, _ := c.p.Tree()

	ch, err := object.DiffTree(dst, src)
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	for _, change := range ch {
		s, err := c.doChange(change)
		if err != nil {
			return nil, err
		}

		stats.Sum(s)
		stats.Files++

	}

	return stats, nil
}

func (c *CommitStatsCalculator) doChange(ch *object.Change) (*Stats, error) {
	a, err := ch.Action()
	if err != nil {
		return nil, err
	}

	var fi FileInfo

	switch a {
	case merkletrie.Delete:
		fi, err = c.doChangeEntry(&ch.From)
		if err != nil {
			return nil, err
		}
	case merkletrie.Insert:
		fi, err = c.doChangeEntry(&ch.To)
		if err != nil {
			return nil, err
		}
	case merkletrie.Modify:
		src, err := c.doChangeEntry(&ch.From)
		if err != nil {
			return nil, err
		}

		dst, err := c.doChangeEntry(&ch.To)
		if err != nil {
			return nil, err
		}

		if src == nil {
			src = make(FileInfo)
		}

		if dst == nil {
			dst = make(FileInfo)
		}

		dst.Sub(src)
		fi = dst
	}

	return fi.Stats(), nil
}

func (c *CommitStatsCalculator) doChangeEntry(ch *object.ChangeEntry) (FileInfo, error) {
	if enry.IsVendor(string(ch.Name)) {
		return nil, nil
	}

	blob, err := c.r.BlobObject(ch.TreeEntry.Hash)
	if err != nil {
		return nil, err
	}

	isBinary, err := IsBinary(blob)
	if err != nil {
		return nil, err
	}

	if isBinary {
		return nil, nil
	}

	return NewFileInfo(blob)
}

var languages = gocloc.NewDefinedLanguages()

type FileInfo map[string]*LineInfo
type LineInfo struct {
	Kind  LineKind
	Count int
}

func NewFileInfo(f *object.Blob) (FileInfo, error) {
	ff := make(FileInfo, 50)

	r, err := f.Reader()
	if err != nil {
		return ff, err
	}

	defer ioutil.CheckClose(r, &err)

	gocloc.AnalyzeReader("", languages.Langs["Go"], r, &gocloc.ClocOptions{
		OnBlank:   ff.AddBlank,
		OnCode:    ff.AddCode,
		OnComment: ff.AddComment,
	})

	return ff, nil
}

func (fi FileInfo) AddCode(line string)    { fi.Add(line, Code) }
func (fi FileInfo) AddComment(line string) { fi.Add(line, Comment) }
func (fi FileInfo) AddBlank(line string)   { fi.Add(line, Blank) }
func (fi FileInfo) Add(line string, k LineKind) {
	if fi[line] == nil {
		fi[line] = &LineInfo{}
	}

	fi[line].Count++
	fi[line].Kind = k
}

func (fi FileInfo) Remove(line string, k LineKind, count int) {
	if fi[line] == nil {
		return
	}

	fi[line].Count -= count
	fi[line].Kind = k
}

type KindStats struct {
	Additions int
	Deletions int
}

type Stats struct {
	Files   int
	Code    KindStats
	Comment KindStats
	Blank   KindStats
	Total   KindStats
}

func (s *Stats) Sum(stats *Stats) {
	sumKindStats(&s.Code, &stats.Code)
	sumKindStats(&s.Comment, &stats.Comment)
	sumKindStats(&s.Blank, &stats.Blank)
	sumKindStats(&s.Total, &stats.Total)
}

func sumKindStats(a, b *KindStats) {
	a.Additions += b.Additions
	a.Deletions += b.Deletions
}

func (s *Stats) String() string {
	return fmt.Sprintf("Code (+%d/-%d)\nComment (+%d/-%d)\nBlank (+%d/-%d)\nTotal (+%d/-%d)\nFiles (%d)\n",
		s.Code.Additions, s.Code.Deletions,
		s.Comment.Additions, s.Comment.Deletions,
		s.Blank.Additions, s.Blank.Deletions,
		s.Total.Additions, s.Total.Deletions,
		s.Files,
	)
}

func (fi FileInfo) Sub(to FileInfo) {
	for line, i := range to {
		if _, ok := fi[line]; ok {
			fi[line].Count -= i.Count
		} else {
			fi[line] = i
			fi[line].Count *= -1
		}
	}
}

func (fi FileInfo) Stats() *Stats {
	stats := &Stats{}
	for _, info := range fi {
		fillKindStats(&stats.Total, info)
		switch info.Kind {
		case Code:
			fillKindStats(&stats.Code, info)
		case Comment:
			fillKindStats(&stats.Comment, info)
		case Blank:
			fillKindStats(&stats.Blank, info)
		}
	}

	return stats
}

func fillKindStats(ks *KindStats, info *LineInfo) {
	if info.Count > 0 {
		ks.Additions += info.Count
	}
	if info.Count < 0 {
		ks.Deletions += (info.Count * -1)
	}
}

func (fi FileInfo) String() string {
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

func IsBinary(b *object.Blob) (bin bool, err error) {
	reader, err := b.Reader()
	if err != nil {
		return false, err
	}

	defer ioutil.CheckClose(reader, &err)
	return binary.IsBinary(reader)
}
