package gocloc

import "testing"

func TestContainComments(t *testing.T) {
	line := "/* hoge */"
	st := "/*"
	ed := "*/"
	if containComments(line, st, ed) {
		t.Errorf("invalid")
	}

	line = "/* comment"
	if !containComments(line, st, ed) {
		t.Errorf("invalid")
	}
}

func TestCheckMD5SumIgnore(t *testing.T) {
	fileCache := make(map[string]struct{})

	if checkMD5Sum("./utils_test.go", fileCache) {
		t.Errorf("invalid sequence")
	}
	if !checkMD5Sum("./utils_test.go", fileCache) {
		t.Errorf("invalid sequence")
	}
}
