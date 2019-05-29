package gocloc

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestAnalayzeFile4Python(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "tmp.py")
	if err != nil {
		t.Logf("ioutil.TempFile() error. err=[%v]", err)
		return
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.Write([]byte(`#!/bin/python

class A:
	"""comment1
	comment2
	comment3
	"""
	pass
`))

	language := NewLanguage("Python", []string{"#"}, [][]string{{"\"\"\"", "\"\"\""}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeFile(tmpfile.Name(), language, clocOpts)
	tmpfile.Close()

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 4 {
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 3 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Python" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}

func TestAnalayzeFile4PythonInvalid(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "tmp.py")
	if err != nil {
		t.Logf("ioutil.TempFile() error. err=[%v]", err)
		return
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.Write([]byte(`#!/bin/python

class A:
	"""comment1
	comment2
	comment3"""
	pass
`))

	language := NewLanguage("Python", []string{"#"}, [][]string{{"\"\"\"", "\"\"\""}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeFile(tmpfile.Name(), language, clocOpts)
	tmpfile.Close()

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 3 {
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 3 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Python" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}

func TestAnalayzeFile4Go(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "tmp.go")
	if err != nil {
		t.Logf("ioutil.TempFile() error. err=[%v]", err)
		return
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.Write([]byte(`package main

func main() {
	var n string /*
		comment
		comment
	*/
}
`))

	language := NewLanguage("Go", []string{"//"}, [][]string{{"/*", "*/"}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeFile(tmpfile.Name(), language, clocOpts)
	tmpfile.Close()

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 3 {
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 4 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Go" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}

func TestAnalayzeFile4GoWithOnelineBlockComment(t *testing.T) {
	t.SkipNow()
	tmpfile, err := ioutil.TempFile("", "tmp.go")
	if err != nil {
		t.Logf("ioutil.TempFile() error. err=[%v]", err)
		return
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.Write([]byte(`package main

func main() {
	st := "/*"
	a := 1
	en := "*/"
	/* comment */
}
`))

	language := NewLanguage("Go", []string{"//"}, [][]string{{"/*", "*/"}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeFile(tmpfile.Name(), language, clocOpts)
	tmpfile.Close()

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 1 { // cloc->3, tokei->1, gocloc->4
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 6 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Go" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}

func TestAnalayzeFile4GoWithCommentInnerBlockComment(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "tmp.go")
	if err != nil {
		t.Logf("ioutil.TempFile() error. err=[%v]", err)
		return
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.Write([]byte(`package main

func main() {
	// comment /*
	a := 1
	b := 2
}
`))

	language := NewLanguage("Go", []string{"//"}, [][]string{{"/*", "*/"}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeFile(tmpfile.Name(), language, clocOpts)
	tmpfile.Close()

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 1 {
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 5 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Go" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}

func TestAnalayzeReader(t *testing.T) {
	buf := bytes.NewBuffer([]byte(`#!/bin/python

class A:
	"""comment1
	comment2
	comment3
	"""
	pass
`))

	language := NewLanguage("Python", []string{"#"}, [][]string{{"\"\"\"", "\"\"\""}})
	clocOpts := NewClocOptions()
	clocFile := AnalyzeReader("test.py", language, buf, clocOpts)

	if clocFile.Blanks != 1 {
		t.Errorf("invalid logic. blanks=%v", clocFile.Blanks)
	}
	if clocFile.Comments != 4 {
		t.Errorf("invalid logic. comments=%v", clocFile.Comments)
	}
	if clocFile.Code != 3 {
		t.Errorf("invalid logic. code=%v", clocFile.Code)
	}
	if clocFile.Lang != "Python" {
		t.Errorf("invalid logic. lang=%v", clocFile.Lang)
	}
}
