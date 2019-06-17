package commitstats

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
