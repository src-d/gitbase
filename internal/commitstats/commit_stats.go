package commitstats

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/hhatto/gocloc"
	"gopkg.in/src-d/enry.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/binary"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

// LineKind defines the kind of a line in a file.
type LineKind int

const (
	// Code represents a line of code.
	Code LineKind = iota + 1
	// Comment represents a line of comment.
	Comment
	// Blank represents an empty line.
	Blank
	// Other represents a line from any other kind.
	Other
)

// Calculate calculates the CommitStats for from commit to another.
// if from is nil the first parent is used, if the commit is orphan the stats
// are compared against a empty commit.
func Calculate(r *git.Repository, from, to *object.Commit) (*CommitStats, error) {
	cc := &commitStatsCalculator{}

	var err error
	if to.NumParents() != 0 && from == nil {
		from, err = to.Parent(0)
		if err != nil {
			return nil, err
		}
	}

	if from == nil {
		return cc.doCommit(to)
	}

	return cc.doDiff(r, from, to)
}

type commitStatsCalculator struct{}

func (cc *commitStatsCalculator) doCommit(c *object.Commit) (*CommitStats, error) {
	files, err := c.Files()
	if err != nil {
		return nil, err
	}

	stats := &CommitStats{}
	return stats, files.ForEach(func(f *object.File) error {
		fi, err := cc.doBlob(&f.Blob, f.Name)
		if err != nil {
			return err
		}

		stats.Add(fi.stats())
		stats.Files++
		return nil
	})
}

func (cc *commitStatsCalculator) doDiff(r *git.Repository, from, to *object.Commit) (*CommitStats, error) {
	ch, err := cc.computeDiff(from, to)
	if err != nil {
		return nil, err
	}

	stats := &CommitStats{}
	for _, change := range ch {
		s, err := cc.doChange(r, change)
		if err != nil {
			return nil, err
		}

		stats.Add(s)
		stats.Files++

	}

	return stats, nil
}

func (cc *commitStatsCalculator) computeDiff(from, to *object.Commit) (object.Changes, error) {
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

func (cc *commitStatsCalculator) doChange(r *git.Repository, ch *object.Change) (*CommitStats, error) {
	a, err := ch.Action()
	if err != nil {
		return nil, err
	}

	var fi fileStats

	switch a {
	case merkletrie.Delete:
		fi, err = cc.doChangeEntry(r, &ch.From)
		if err != nil {
			return nil, err
		}
	case merkletrie.Insert:
		fi, err = cc.doChangeEntry(r, &ch.To)
		if err != nil {
			return nil, err
		}
	case merkletrie.Modify:
		src, err := cc.doChangeEntry(r, &ch.From)
		if err != nil {
			return nil, err
		}

		dst, err := cc.doChangeEntry(r, &ch.To)
		if err != nil {
			return nil, err
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

	return fi.stats(), nil
}

func (cc *commitStatsCalculator) doChangeEntry(r *git.Repository, ch *object.ChangeEntry) (fileStats, error) {
	blob, err := r.BlobObject(ch.TreeEntry.Hash)
	if err != nil {
		return nil, err
	}

	return cc.doBlob(blob, ch.Name)
}

func (cc *commitStatsCalculator) doBlob(blob *object.Blob, filename string) (fileStats, error) {
	if enry.IsVendor(filename) {
		return nil, nil
	}

	isBinary, err := isBinary(blob)
	if err != nil {
		return nil, err
	}

	if isBinary {
		return nil, nil
	}

	lang := cc.getLanguage(filename)

	return newFileStats(blob, lang)
}

func (*commitStatsCalculator) getLanguage(filename string) string {
	if lang, ok := enry.GetLanguageByFilename(filename); ok {
		return lang
	}

	if lang, ok := enry.GetLanguageByExtension(filename); ok {
		return lang
	}

	return ""
}

// KindStats represents the stats for a kind of lines in a file.
type KindStats struct {
	// Additions number of lines added.
	Additions int
	// Deletions number of lines deleted.
	Deletions int
}

// Add adds the given stats to this stats.
func (k *KindStats) Add(add KindStats) {
	k.Additions += add.Additions
	k.Deletions += add.Deletions
}

// CommitStats represents the stats for a commit.
type CommitStats struct {
	// Files add/modified/removed by this commit.
	Files int
	// Code stats of the code lines.
	Code KindStats
	// Comment stats of the comment lines.
	Comment KindStats
	// Blank stats of the blank lines.
	Blank KindStats
	// Other stats of files that are not from a recognized or format language.
	Other KindStats
	// Total the sum of the previous stats.
	Total KindStats
}

// Add adds the given stats to this stats.
func (s *CommitStats) Add(stats *CommitStats) {
	s.Code.Add(stats.Code)
	s.Comment.Add(stats.Comment)
	s.Blank.Add(stats.Blank)
	s.Other.Add(stats.Other)
	s.Total.Add(stats.Total)
}

func (s *CommitStats) String() string {
	return fmt.Sprintf("Code (+%d/-%d)\nComment (+%d/-%d)\nBlank (+%d/-%d)\nOther (+%d/-%d)\nTotal (+%d/-%d)\nFiles (%d)\n",
		s.Code.Additions, s.Code.Deletions,
		s.Comment.Additions, s.Comment.Deletions,
		s.Blank.Additions, s.Blank.Deletions,
		s.Other.Additions, s.Other.Deletions,
		s.Total.Additions, s.Total.Deletions,
		s.Files,
	)
}

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
		doNewFileStatsGoCloc(r, l, &ff)
		return ff, nil
	}

	return ff, doNewFileStatsPlain(r, &ff)
}

func doNewFileStatsGoCloc(r io.Reader, l *gocloc.Language, ff *fileStats) {
	gocloc.AnalyzeReader("", l, r, &gocloc.ClocOptions{
		OnBlank:   ff.addBlank,
		OnCode:    ff.addCode,
		OnComment: ff.addComment,
	})
}

func doNewFileStatsPlain(r io.Reader, ff *fileStats) error {
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

func (fi fileStats) stats() *CommitStats {
	stats := &CommitStats{}
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

func isBinary(b *object.Blob) (bin bool, err error) {
	reader, err := b.Reader()
	if err != nil {
		return false, err
	}

	defer ioutil.CheckClose(reader, &err)
	return binary.IsBinary(reader)
}
