package gocloc

import "regexp"

type ClocOptions struct {
	Debug          bool
	SkipDuplicated bool
	ExcludeExts    map[string]struct{}
	IncludeLangs   map[string]struct{}
	ReNotMatchDir  *regexp.Regexp
	ReMatchDir     *regexp.Regexp

	// OnCode is triggered for each line of code.
	OnCode func(line string)
	// OnBlack is triggered for each blank line.
	OnBlank func(line string)
	// OnComment is triggered for each line of comments.
	OnComment func(line string)
}

func NewClocOptions() *ClocOptions {
	return &ClocOptions{
		Debug:          false,
		SkipDuplicated: false,
		ExcludeExts:    make(map[string]struct{}),
		IncludeLangs:   make(map[string]struct{}),
	}
}
