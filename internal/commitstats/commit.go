package commitstats

import (
	"fmt"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

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

// Calculate calculates the CommitStats for from commit to another.
// if from is nil the first parent is used, if the commit is orphan the stats
// are compared against a empty commit.
func Calculate(r *git.Repository, from, to *object.Commit) (*CommitStats, error) {
	fs, err := CalculateByFile(r, from, to)
	if err != nil {
		return nil, err
	}

	return commitStatsFromCommitFileStats(fs), nil
}

func commitStatsFromCommitFileStats(fs []CommitFileStats) *CommitStats {
	var s CommitStats
	for _, f := range fs {
		s.Blank.Add(f.Blank)
		s.Comment.Add(f.Comment)
		s.Code.Add(f.Code)
		s.Other.Add(f.Other)
		s.Total.Add(f.Total)
		s.Files++
	}
	return &s
}
