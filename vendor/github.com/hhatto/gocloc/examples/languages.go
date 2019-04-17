package main

import (
	"fmt"

	"github.com/hhatto/gocloc"
)

func main() {
	languages := gocloc.NewDefinedLanguages()
	options := gocloc.NewClocOptions()
	paths := []string{
		".",
	}

	processor := gocloc.NewProcessor(languages, options)
	result, err := processor.Analyze(paths)
	if err != nil {
		fmt.Printf("gocloc fail. error: %v\n", err)
		return
	}

	for _, lang := range result.Languages {
		fmt.Println(lang)
	}
	fmt.Println(result.Total)
	fmt.Printf("%+v", result)
}
