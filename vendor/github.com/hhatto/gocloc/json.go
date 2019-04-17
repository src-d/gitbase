package gocloc

type JSONLanguagesResult struct {
	Languages []ClocLanguage `json:"languages"`
	Total     ClocLanguage   `json:"total"`
}

type JSONFilesResult struct {
	Files []ClocFile   `json:"files"`
	Total ClocLanguage `json:"total"`
}

func NewJSONLanguagesResultFromCloc(total *Language, sortedLanguages Languages) JSONLanguagesResult {
	var langs []ClocLanguage
	for _, language := range sortedLanguages {
		c := ClocLanguage{
			Name:       language.Name,
			FilesCount: int32(len(language.Files)),
			Code:       language.Code,
			Comments:   language.Comments,
			Blanks:     language.Blanks,
		}
		langs = append(langs, c)
	}
	t := ClocLanguage{
		FilesCount: total.Total,
		Code:       total.Code,
		Comments:   total.Comments,
		Blanks:     total.Blanks,
	}

	return JSONLanguagesResult{
		Languages: langs,
		Total:     t,
	}
}

func NewJSONFilesResultFromCloc(total *Language, sortedFiles ClocFiles) JSONFilesResult {
	t := ClocLanguage{
		FilesCount: total.Total,
		Code:       total.Code,
		Comments:   total.Comments,
		Blanks:     total.Blanks,
	}

	return JSONFilesResult{
		Files: sortedFiles,
		Total: t,
	}
}
