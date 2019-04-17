package gocloc

import (
	"encoding/xml"
	"fmt"
)

type XMLResultType int8

const (
	XMLResultWithLangs XMLResultType = iota
	XMLResultWithFiles
)

type XMLTotalLanguages struct {
	SumFiles int32 `xml:"sum_files,attr"`
	Code     int32 `xml:"code,attr"`
	Comment  int32 `xml:"comment,attr"`
	Blank    int32 `xml:"blank,attr"`
}
type XMLResultLanguages struct {
	Languages []ClocLanguage    `xml:"language"`
	Total     XMLTotalLanguages `xml:"total"`
}

type XMLTotalFiles struct {
	Code    int32 `xml:"code,attr"`
	Comment int32 `xml:"comment,attr"`
	Blank   int32 `xml:"blank,attr"`
}
type XMLResultFiles struct {
	Files []ClocFile    `xml:"file"`
	Total XMLTotalFiles `xml:"total"`
}

type XMLResult struct {
	XMLName      xml.Name            `xml:"results"`
	XMLFiles     *XMLResultFiles     `xml:"files,omitempty"`
	XMLLanguages *XMLResultLanguages `xml:"languages,omitempty"`
}

func (x *XMLResult) Encode() {
	if output, err := xml.MarshalIndent(x, "", "  "); err == nil {
		fmt.Printf(xml.Header)
		fmt.Println(string(output))
	}
}

func NewXMLResultFromCloc(total *Language, sortedLanguages Languages, option XMLResultType) *XMLResult {
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
	t := XMLTotalLanguages{
		Code:     total.Code,
		Comment:  total.Comments,
		Blank:    total.Blanks,
		SumFiles: total.Total,
	}
	f := &XMLResultLanguages{
		Languages: langs,
		Total:     t,
	}

	return &XMLResult{
		XMLLanguages: f,
	}
}
