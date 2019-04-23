package gocloc

type Processor struct {
	langs *DefinedLanguages
	opts  *ClocOptions
}

type Result struct {
	Total         *Language
	Files         map[string]*ClocFile
	Languages     map[string]*Language
	MaxPathLength int
}

func NewProcessor(langs *DefinedLanguages, options *ClocOptions) *Processor {
	return &Processor{
		langs: langs,
		opts:  options,
	}
}

func (p *Processor) Analyze(paths []string) (*Result, error) {
	total := NewLanguage("TOTAL", []string{}, [][]string{{"", ""}})
	languages, err := getAllFiles(paths, p.langs, p.opts)
	if err != nil {
		return nil, err
	}
	maxPathLen := 0
	num := 0
	for _, lang := range languages {
		num += len(lang.Files)
		for _, file := range lang.Files {
			l := len(file)
			if maxPathLen < l {
				maxPathLen = l
			}
		}
	}
	clocFiles := make(map[string]*ClocFile, num)

	for _, language := range languages {
		for _, file := range language.Files {
			cf := AnalyzeFile(file, language, p.opts)
			cf.Lang = language.Name

			language.Code += cf.Code
			language.Comments += cf.Comments
			language.Blanks += cf.Blanks
			clocFiles[file] = cf
		}

		files := int32(len(language.Files))
		if len(language.Files) <= 0 {
			continue
		}

		total.Total += files
		total.Blanks += language.Blanks
		total.Comments += language.Comments
		total.Code += language.Code
	}

	return &Result{
		Total:         total,
		Files:         clocFiles,
		Languages:     languages,
		MaxPathLength: maxPathLen,
	}, nil
}
