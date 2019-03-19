// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rubex

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

var good_re = []string{
	``,
	`.`,
	`^.$`,
	`a`,
	`a*`,
	`a+`,
	`a?`,
	`a|b`,
	`a*|b*`,
	`(a*|b)(c*|d)`,
	`[a-z]`,
	`[a-abc-c\-\]\[]`,
	`[a-z]+`,
	//`[]`, //this is not considered as good by ruby/javascript regex
	`[abc]`,
	`[^1234]`,
	`[^\n]`,
	`\!\\`,
}

type stringError struct {
	re  string
	err error
}

var bad_re = []stringError{
	{`*`, errors.New("target of repeat operator is not specified")},
	{`+`, errors.New("target of repeat operator is not specified")},
	{`?`, errors.New("target of repeat operator is not specified")},
	{`(abc`, errors.New("end pattern with unmatched parenthesis")},
	{`abc)`, errors.New("unmatched close parenthesis")},
	{`x[a-z`, errors.New("premature end of char-class")},
	//{`abc]`, Err}, //this is not considered as bad by ruby/javascript regex; nor are the following commented out regex patterns
	{`abc[`, errors.New("premature end of char-class")},
	{`[z-a]`, errors.New("empty range in char class")},
	{`abc\`, errors.New("end pattern at escape")},
	//{`a**`, Err},
	//{`a*+`, Err},
	//{`a??`, Err},
	//{`\x`, Err},
}

func runParallel(testFunc func(chan bool), concurrency int) {
	runtime.GOMAXPROCS(4)
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go testFunc(done)
	}
	for i := 0; i < concurrency; i++ {
		<-done
		<-done
	}
	runtime.GOMAXPROCS(1)
}

const numConcurrentRuns = 200

func compileTest(t *testing.T, expr string, error error) *Regexp {
	re, err := Compile(expr)
	if (error == nil && err != error) || (error != nil && err.Error() != error.Error()) {
		t.Error("compiling `", expr, "`; unexpected error: ", err.Error())
	}
	return re
}

func TestGoodCompile(t *testing.T) {
	testFunc := func(done chan bool) {
		done <- false
		for i := 0; i < len(good_re); i++ {
			compileTest(t, good_re[i], nil)
		}
		done <- true
	}
	runParallel(testFunc, numConcurrentRuns)
}

func TestBadCompile(t *testing.T) {
	for i := 0; i < len(bad_re); i++ {
		compileTest(t, bad_re[i].re, bad_re[i].err)
	}
}

func matchTest(t *testing.T, test *FindTest) {
	re := compileTest(t, test.pat, nil)
	if re == nil {
		return
	}
	m := re.MatchString(test.text)
	if m != (len(test.matches) > 0) {
		t.Errorf("MatchString failure on %s: %t should be %t", test.pat, m, len(test.matches) > 0)
	}
	// now try bytes
	m = re.Match([]byte(test.text))
	if m != (len(test.matches) > 0) {
		t.Errorf("Match failure on %s: %t should be %t", test.pat, m, len(test.matches) > 0)
	}
}

func TestMatch(t *testing.T) {
	for _, test := range findTests {
		matchTest(t, &test)
	}
}

func matchFunctionTest(t *testing.T, test *FindTest) {
	m, err := MatchString(test.pat, test.text)
	if err == nil {
		return
	}
	if m != (len(test.matches) > 0) {
		t.Errorf("Match failure on %s: %t should be %t", test, m, len(test.matches) > 0)
	}
}

func TestMatchFunction(t *testing.T) {
	for _, test := range findTests {
		matchFunctionTest(t, &test)
	}
}

type ReplaceTest struct {
	pattern, replacement, input, output string
}

var replaceTests = []ReplaceTest{
	// Test empty input and/or replacement, with pattern that matches the empty string.
	{"", "", "", ""},
	{"", "x", "", "x"},
	{"", "", "abc", "abc"},
	{"", "x", "abc", "xaxbxcx"},

	// Test empty input and/or replacement, with pattern that does not match the empty string.
	{"b", "", "", ""},
	{"b", "x", "", ""},
	{"b", "", "abc", "ac"},
	{"b", "x", "abc", "axc"},
	{"y", "", "", ""},
	{"y", "x", "", ""},
	{"y", "", "abc", "abc"},
	{"y", "x", "abc", "abc"},

	// Multibyte characters -- verify that we don't try to match in the middle
	// of a character.
	{"[a-c]*", "x", "\u65e5", "x\u65e5x"},
	{"[^\u65e5]", "x", "abc\u65e5def", "xxx\u65e5xxx"},

	// Start and end of a string.
	{"^[a-c]*", "x", "abcdabc", "xdabc"},
	{"[a-c]*$", "x", "abcdabc", "abcdxx"},
	{"^[a-c]*$", "x", "abcdabc", "abcdabc"},
	{"^[a-c]*", "x", "abc", "x"},
	{"[a-c]*$", "x", "abc", "xx"},
	{"^[a-c]*$", "x", "abc", "x"},
	{"^[a-c]*", "x", "dabce", "xdabce"},
	{"[a-c]*$", "x", "dabce", "dabcex"},
	{"^[a-c]*$", "x", "dabce", "dabce"},
	{"^[a-c]*", "x", "", "x"},
	{"[a-c]*$", "x", "", "x"},
	{"^[a-c]*$", "x", "", "x"},

	{"^[a-c]+", "x", "abcdabc", "xdabc"},
	{"[a-c]+$", "x", "abcdabc", "abcdx"},
	{"^[a-c]+$", "x", "abcdabc", "abcdabc"},
	{"^[a-c]+", "x", "abc", "x"},
	{"[a-c]+$", "x", "abc", "x"},
	{"^[a-c]+$", "x", "abc", "x"},
	{"^[a-c]+", "x", "dabce", "dabce"},
	{"[a-c]+$", "x", "dabce", "dabce"},
	{"^[a-c]+$", "x", "dabce", "dabce"},
	{"^[a-c]+", "x", "", ""},
	{"[a-c]+$", "x", "", ""},
	{"^[a-c]+$", "x", "", ""},

	// Other cases.
	{"abc", "def", "abcdefg", "defdefg"},
	{"bc", "BC", "abcbcdcdedef", "aBCBCdcdedef"},
	{"abc", "", "abcdabc", "d"},
	{"x", "xXx", "xxxXxxx", "xXxxXxxXxXxXxxXxxXx"},
	{"abc", "d", "", ""},
	{"abc", "d", "abc", "d"},
	{".+", "x", "abc", "x"},
	{"[a-c]*", "x", "def", "xdxexfx"},
	{"[a-c]+", "x", "abcbcdcdedef", "xdxdedef"},
	{"[a-c]*", "x", "abcbcdcdedef", "xxdxxdxexdxexfx"},
	{"(foo)*bar(s)", "\\1", "bars", ""},
}

type ReplaceFuncTest struct {
	pattern       string
	replacement   func(string) string
	input, output string
}

var replaceFuncTests = []ReplaceFuncTest{
	{"[a-c]", func(s string) string { return "x" + s + "y" }, "defabcdef", "defxayxbyxcydef"},
	{"[a-c]+", func(s string) string { return "x" + s + "y" }, "defabcdef", "defxabcydef"},
	{"[a-c]*", func(s string) string { return "x" + s + "y" }, "defabcdef", "xydxyexyfxabcyxydxyexyfxy"},
}

func TestReplaceAll(t *testing.T) {
	for _, tc := range replaceTests {
		re, err := Compile(tc.pattern)

		if err != nil {
			t.Errorf("Unexpected error compiling %q: %v", tc.pattern, err)
			continue
		}

		actual := re.ReplaceAllString(tc.input, tc.replacement)

		if actual != tc.output {
			t.Errorf("%q.Replace(%q,%q) = %q; want %q",
				tc.pattern, tc.input, tc.replacement, actual, tc.output)
		}

		// now try bytes

		actual = string(re.ReplaceAll([]byte(tc.input), []byte(tc.replacement)))
		if actual != tc.output {
			t.Errorf("%q.Replace(%q,%q) = %q; want %q",
				tc.pattern, tc.input, tc.replacement, actual, tc.output)
		}

	}
}

func TestReplaceAllFunc(t *testing.T) {
	for _, tc := range replaceFuncTests {
		re, err := Compile(tc.pattern)
		if err != nil {
			t.Errorf("Unexpected error compiling %q: %v", tc.pattern, err)
			continue
		}
		actual := re.ReplaceAllStringFunc(tc.input, tc.replacement)
		if actual != tc.output {
			t.Errorf("%q.ReplaceFunc(%q) = %q; want %q",
				tc.pattern, tc.input, actual, tc.output)
		}
		// now try bytes
		actual = string(re.ReplaceAllFunc([]byte(tc.input), func(s []byte) []byte { return []byte(tc.replacement(string(s))) }))
		if actual != tc.output {
			t.Errorf("%q.ReplaceFunc(%q) = %q; want %q",
				tc.pattern, tc.input, actual, tc.output)
		}
	}
}

/*
* "hallo".gsub(/h(.*)llo/, "e")
 */
func TestGsub1(t *testing.T) {
	input := "hallo"
	pattern := "h(.*)llo"
	expected := "e"
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.Gsub(input, "e")
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/*
* "hallo".gsub(/h(?<foo>.*)llo/, "\\k<foo>")
 */
func TestGsubNamedCapture1(t *testing.T) {
	input := "hallo"
	pattern := "h(?<foo>.*)llo"
	expected := "a"
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.Gsub(input, "\\k<foo>")
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/*
* "hallo".gsub(/h(?<foo>.*)ll(?<bar>.*)/, "\\k<foo>\\k<bar>\\k<foo>")
 */
func TestGsubNamedCapture2(t *testing.T) {
	input := "hallo"
	pattern := "h(?<foo>.*)ll(?<bar>.*)"
	expected := "aoa"
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.Gsub(input, "\\k<foo>\\k<bar>\\k<foo>")
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/*
* "hallo".gsub(/h(?<foo>.*)(l*)(?<bar>.*)/, "\\k<foo>\\k<bar>\\k<foo>\\1")
 */
func TestGsubNamedCapture3(t *testing.T) {
	input := "hallo"
	pattern := "h(?<foo>.*)(l*)(?<bar>.*)"
	expected := "alloallo"
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.Gsub(input, "\\k<foo>\\k<bar>\\k<foo>\\1")
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/*
* "hallo".gsub(/h(?<foo>.*)(l*)(?<bar>.*)/, "\\k<foo>\\k<bar>\\k<foo>\\1")
 */
func TestGsubNamedCapture4(t *testing.T) {
	input := "The lamb was sure to go."
	pattern := "(?<word>[^\\s\\.]+)(?<white_space>\\s)"
	expected := "They lamby wasy surey toy go."
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}

	actual := re.GsubFunc(input, func(_ string, captures map[string]string) string {
		return captures["word"] + "y" + captures["white_space"]
	})
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}

}

/*
* "hallo".gsub(/h(.*)llo/) { |match|
*    "e"
* }
 */
func TestGsubFunc1(t *testing.T) {
	input := "hallo"
	pattern := "h(.*)llo"
	expected := "e"
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.GsubFunc(input, func(match string, captures map[string]string) string {
		return "e"
	})
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/*
* @env = {}
* "hallo".gsub(/h(.*)llo/) { |match|
*   $~.captures.each_with_index do |arg, index|
*     @env["#{index + 1}"] = arg
*     "abcd".gsub(/(d)/) do
*       env["1"]
*     end
*   end
* }
 */
func TestGsubFunc2(t *testing.T) {
	input := "hallo"
	pattern := "h(.*)llo"
	expected := "abca"
	env := make(map[string]string)
	re, err := Compile(pattern)
	if err != nil {
		t.Errorf("Unexpected error compiling %q: %v", pattern, err)
		return
	}
	actual := re.GsubFunc(input, func(_ string, captures map[string]string) string {
		for name, capture := range captures {
			env[name] = capture
		}
		re1 := MustCompile("(d)")
		return re1.GsubFunc("abcd", func(_ string, captures2 map[string]string) string {
			return env["1"]
		})
	})
	if actual != expected {
		t.Errorf("expected %q, actual %q\n", expected, actual)
	}
}

/* how to match $ as itself */
func TestPattern1(t *testing.T) {
	re := MustCompile(`b\$a`)
	if !re.MatchString("b$a") {
		t.Errorf("expect to match\n")
	}
	re = MustCompile("b\\$a")
	if !re.MatchString("b$a") {
		t.Errorf("expect to match 2\n")
	}
}

/* how to use $ as the end of line */
func TestPattern2(t *testing.T) {
	re := MustCompile("a$")
	if !re.MatchString("a") {
		t.Errorf("expect to match\n")
	}
	if re.MatchString("ab") {
		t.Errorf("expect to mismatch\n")
	}
}

func TestCompileWithOption(t *testing.T) {
	re := MustCompileWithOption("a$", ONIG_OPTION_IGNORECASE)
	if !re.MatchString("A") {
		t.Errorf("expect to match\n")
	}
	re = MustCompile("a$")
	if re.MatchString("A") {
		t.Errorf("expect to mismatch\n")
	}

}

type MetaTest struct {
	pattern, output, literal string
	isLiteral                bool
}

var metaTests = []MetaTest{
	{``, ``, ``, true},
	{`foo`, `foo`, `foo`, true},
	{`foo\.\$`, `foo\\\.\\\$`, `foo.$`, true}, // has meta but no operator
	{`foo.\$`, `foo\.\\\$`, `foo`, false},     // has escaped operators and real operators
	{`!@#$%^&*()_+-=[{]}\|,<.>/?~`, `!@#\$%\^&\*\(\)_\+-=\[{\]}\\\|,<\.>/\?~`, `!@#`, false},
}

func TestQuoteMeta(t *testing.T) {
	for _, tc := range metaTests {
		// Verify that QuoteMeta returns the expected string.
		quoted := QuoteMeta(tc.pattern)
		if quoted != tc.output {
			t.Errorf("QuoteMeta(`%s`) = `%s`; want `%s`",
				tc.pattern, quoted, tc.output)
			continue
		}

		// Verify that the quoted string is in fact treated as expected
		// by Compile -- i.e. that it matches the original, unquoted string.
		if tc.pattern != "" {
			re, err := Compile(quoted)
			if err != nil {
				t.Errorf("Unexpected error compiling QuoteMeta(`%s`): %v", tc.pattern, err)
				continue
			}
			src := "abc" + tc.pattern + "def"
			repl := "xyz"
			replaced := re.ReplaceAllString(src, repl)
			expected := "abcxyzdef"
			if replaced != expected {
				t.Errorf("QuoteMeta(`%s`).Replace(`%s`,`%s`) = `%s`; want `%s`",
					tc.pattern, src, repl, replaced, expected)
			}
		}
	}
}

type numSubexpCase struct {
	input    string
	expected int
}

var numSubexpCases = []numSubexpCase{
	{``, 0},
	{`.*`, 0},
	{`abba`, 0},
	{`ab(b)a`, 1},
	{`ab(.*)a`, 1},
	{`(.*)ab(.*)a`, 2},
	{`(.*)(ab)(.*)a`, 3},
	{`(.*)((a)b)(.*)a`, 4},
	{`(.*)(\(ab)(.*)a`, 3},
	{`(.*)(\(a\)b)(.*)a`, 3},
}

func TestNumSubexp(t *testing.T) {
	for _, c := range numSubexpCases {
		re := MustCompile(c.input)
		n := re.NumSubexp()
		if n != c.expected {
			t.Errorf("NumSubexp for %q returned %d, expected %d", c.input, n, c.expected)
		}
	}
}

// For each pattern/text pair, what is the expected output of each function?
// We can derive the textual results from the indexed results, the non-submatch
// results from the submatched results, the single results from the 'all' results,
// and the byte results from the string results. Therefore the table includes
// only the FindAllStringSubmatchIndex result.
type FindTest struct {
	pat     string
	text    string
	matches [][]int
}

func (t FindTest) String() string {
	return fmt.Sprintf("pat: %#q text: %#q", t.pat, t.text)
}

var findTests = []FindTest{
	{``, ``, build(1, 0, 0)},
	{`^abcdefg`, "abcdefg", build(1, 0, 7)},
	{`a+`, "baaab", build(1, 1, 4)},
	{"abcd..", "abcdef", build(1, 0, 6)},
	{`a`, "a", build(1, 0, 1)},
	{`x`, "y", nil},
	{`b`, "abc", build(1, 1, 2)},
	{`.`, "a", build(1, 0, 1)},
	{`.*`, "abcdef", build(2, 0, 6, 6, 6)},
	{`^`, "abcde", build(1, 0, 0)},
	{`$`, "abcde", build(1, 5, 5)},
	{`^abcd$`, "abcd", build(1, 0, 4)},
	{`^bcd'`, "abcdef", nil},
	{`^abcd$`, "abcde", nil},
	{`a+`, "baaab", build(1, 1, 4)},
	{`a*`, "baaab", build(4, 0, 0, 1, 4, 4, 4, 5, 5)},
	{`[a-z]+`, "abcd", build(1, 0, 4)},
	{`[^a-z]+`, "ab1234cd", build(1, 2, 6)},
	{`[a\-\]z]+`, "az]-bcz", build(2, 0, 4, 6, 7)},
	{`[^\n]+`, "abcd\n", build(1, 0, 4)},
	{`[日本語]+`, "日本語日本語", build(1, 0, 18)},
	{`日本語+`, "日本語", build(1, 0, 9)},
	{`a*`, "日本語", build(4, 0, 0, 3, 3, 6, 6, 9, 9)},
	{`日本語+`, "日本語語語語", build(1, 0, 18)},
	{`()`, "", build(1, 0, 0, 0, 0)},
	{`(a)`, "a", build(1, 0, 1, 0, 1)},
	{`(.)(.)`, "日a", build(1, 0, 4, 0, 3, 3, 4)},
	{`(.*)`, "", build(1, 0, 0, 0, 0)},
	{`(.*)`, "abcd", build(2, 0, 4, 0, 4, 4, 4, 4, 4)},
	{`(..)(..)`, "abcd", build(1, 0, 4, 0, 2, 2, 4)},
	{`(([^xyz]*)(d))`, "abcd", build(1, 0, 4, 0, 4, 0, 3, 3, 4)},
	{`((a|b|c)*(d))`, "abcd", build(1, 0, 4, 0, 4, 2, 3, 3, 4)},
	{`(((a|b|c)*)(d))`, "abcd", build(1, 0, 4, 0, 4, 0, 3, 2, 3, 3, 4)},
	{"\a\b\f\n\r\t\v", "\a\b\f\n\r\t\v", build(1, 0, 7)},
	{`[\a\b\f\n\r\t\v]+`, "\a\b\f\n\r\t\v", build(1, 0, 7)},

	//{`a*(|(b))c*`, "aacc", build(2, 0, 4, 4, 4)},
	{`(.*).*`, "ab", build(2, 0, 2, 0, 2, 2, 2, 2, 2)},
	{`[.]`, ".", build(1, 0, 1)},
	{`/$`, "/abc/", build(1, 4, 5)},
	{`/$`, "/abc", nil},

	// multiple matches
	{`.`, "abc", build(3, 0, 1, 1, 2, 2, 3)},
	{`(.)`, "abc", build(3, 0, 1, 0, 1, 1, 2, 1, 2, 2, 3, 2, 3)},
	{`.(.)`, "abcd", build(2, 0, 2, 1, 2, 2, 4, 3, 4)},
	{`ab*`, "abbaab", build(3, 0, 3, 3, 4, 4, 6)},
	{`a(b*)`, "abbaab", build(3, 0, 3, 1, 3, 3, 4, 4, 4, 4, 6, 5, 6)},

	// fixed bugs
	{`ab$`, "cab", build(1, 1, 3)},
	{`axxb$`, "axxcb", nil},
	{`data`, "daXY data", build(1, 5, 9)},
	{`da(.)a$`, "daXY data", build(1, 5, 9, 7, 8)},
	{`zx+`, "zzx", build(1, 1, 3)},

	// can backslash-escape any punctuation
	{`\!\"\#\$\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\{\|\}\~`,
		`!"#$%&'()*+,-./:;<=>?@[\]^_{|}~`, build(1, 0, 31)},
	{`[\!\"\#\$\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\{\|\}\~]+`,
		`!"#$%&'()*+,-./:;<=>?@[\]^_{|}~`, build(1, 0, 31)},
	{"\\`", "`", build(1, 0, 1)},
	{"[\\`]+", "`", build(1, 0, 1)},

	// long set of matches (longer than startSize)
	{
		".",
		"qwertyuiopasdfghjklzxcvbnm1234567890",
		build(36, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10,
			10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 17, 17, 18, 18, 19, 19, 20,
			20, 21, 21, 22, 22, 23, 23, 24, 24, 25, 25, 26, 26, 27, 27, 28, 28, 29, 29, 30,
			30, 31, 31, 32, 32, 33, 33, 34, 34, 35, 35, 36),
	},
}

// build is a helper to construct a [][]int by extracting n sequences from x.
// This represents n matches with len(x)/n submatches each.
func build(n int, x ...int) [][]int {
	ret := make([][]int, n)
	runLength := len(x) / n
	j := 0
	for i := range ret {
		ret[i] = make([]int, runLength)
		copy(ret[i], x[j:])
		j += runLength
		if j > len(x) {
			panic("invalid build entry")
		}
	}
	return ret
}

// First the simple cases.

func TestFind(t *testing.T) {
	for _, test := range findTests {
		re := MustCompile(test.pat)
		if re.String() != test.pat {
			t.Errorf("String() = `%s`; should be `%s`", re.String(), test.pat)
		}
		result := re.Find([]byte(test.text))
		switch {
		case len(test.matches) == 0 && len(result) == 0:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case test.matches != nil && result != nil:
			expect := test.text[test.matches[0][0]:test.matches[0][1]]
			if expect != string(result) {
				t.Errorf("expected %q got %q: %s", expect, result, test)
			}
		}
	}
}

func TestFindString(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindString(test.text)
		switch {
		case len(test.matches) == 0 && len(result) == 0:
			// ok
		case test.matches == nil && result != "":
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == "":
			// Tricky because an empty result has two meanings: no match or empty match.
			if test.matches[0][0] != test.matches[0][1] {
				t.Errorf("expected match; got none: %s", test)
			}
		case test.matches != nil && result != "":
			expect := test.text[test.matches[0][0]:test.matches[0][1]]
			if expect != result {
				t.Errorf("expected %q got %q: %s", expect, result, test)
			}
		}
	}
}

func testFindIndex(test *FindTest, result []int, t *testing.T) {
	switch {
	case len(test.matches) == 0 && len(result) == 0:
		// ok
	case test.matches == nil && result != nil:
		t.Errorf("expected no match; got one: %s", test)
	case test.matches != nil && result == nil:
		t.Errorf("expected match; got none: %s", test)
	case test.matches != nil && result != nil:
		expect := test.matches[0]
		if expect[0] != result[0] || expect[1] != result[1] {
			t.Errorf("expected %v got %v: %s", expect, result, test)
		}
	}
}

func TestFindIndex(t *testing.T) {
	for _, test := range findTests {
		testFindIndex(&test, MustCompile(test.pat).FindIndex([]byte(test.text)), t)
	}
}

func TestFindStringIndex(t *testing.T) {
	for _, test := range findTests {
		testFindIndex(&test, MustCompile(test.pat).FindStringIndex(test.text), t)
	}
}

func TestFindStringContentType(t *testing.T) {
	pattern := `text/(.*);\s*charset\s*=\s*(.*)`
	regex := MustCompile(pattern)

	data1 := "text/html; charset=utf8"
	data2 := "text/;charset=iso-8859-1"
	data3 := "image/png"
	matches := regex.FindStringSubmatch(data1)
	if matches[1] != "html" || matches[2] != "utf8" {
		t.Errorf("does not match content-type 1")
	}
	matches = regex.FindStringSubmatch(data2)
	if matches[1] != "" || matches[2] != "iso-8859-1" {
		println(matches[1])
		println(matches[2])
		t.Errorf("does not match content-type 2")
	}
	matches = regex.FindStringSubmatch(data3)
	if len(matches) != 0 {
		t.Errorf("does not match content-type 3")
	}
}

func TestFindReaderIndex(t *testing.T) {
	for _, test := range findTests {
		testFindIndex(&test, MustCompile(test.pat).FindReaderIndex(strings.NewReader(test.text)), t)
	}
}

// Now come the simple All cases.

func TestFindAll(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindAll([]byte(test.text), -1)
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case test.matches != nil && result != nil:
			if len(test.matches) != len(result) {
				t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
				continue
			}
			for k, e := range test.matches {
				expect := test.text[e[0]:e[1]]
				if expect != string(result[k]) {
					t.Errorf("match %d: expected %q got %q: %s", k, expect, result[k], test)
				}
			}
		}
	}
}

func TestFindAllString(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindAllString(test.text, -1)
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case test.matches != nil && result != nil:
			if len(test.matches) != len(result) {
				t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
				continue
			}
			for k, e := range test.matches {
				expect := test.text[e[0]:e[1]]
				if expect != result[k] {
					t.Errorf("expected %q got %q: %s", expect, result, test)
				}
			}
		}
	}
}

func testFindAllIndex(test *FindTest, result [][]int, t *testing.T) {
	switch {
	case test.matches == nil && result == nil:
		// ok
	case test.matches == nil && result != nil:
		t.Errorf("expected no match; got one: %s", test)
	case test.matches != nil && result == nil:
		t.Errorf("expected match; got none: %s", test)
	case test.matches != nil && result != nil:
		if len(test.matches) != len(result) {
			t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
			return
		}
		for k, e := range test.matches {
			if e[0] != result[k][0] || e[1] != result[k][1] {
				t.Errorf("match %d: expected %v got %v: %s", k, e, result[k], test)
			}
		}
	}
}

func TestFindAllIndex(t *testing.T) {
	for _, test := range findTests {
		testFindAllIndex(&test, MustCompile(test.pat).FindAllIndex([]byte(test.text), -1), t)
	}
}

func TestFindAllStringIndex(t *testing.T) {
	for _, test := range findTests {
		testFindAllIndex(&test, MustCompile(test.pat).FindAllStringIndex(test.text, -1), t)
	}
}

// Now come the Submatch cases.

func testSubmatchBytes(test *FindTest, n int, submatches []int, result [][]byte, t *testing.T) {
	if len(submatches) != len(result)*2 {
		t.Errorf("match %d: expected %d submatches; got %d: %s", n, len(submatches)/2, len(result), test)
		return
	}
	for k := 0; k < len(submatches); k += 2 {
		if submatches[k] == -1 {
			if result[k/2] != nil {
				t.Errorf("match %d: expected nil got %q: %s", n, result, test)
			}
			continue
		}
		expect := test.text[submatches[k]:submatches[k+1]]
		if expect != string(result[k/2]) {
			t.Errorf("match %d: expected %q got %q: %s", n, expect, result, test)
			return
		}
	}
}

func TestFindSubmatch(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindSubmatch([]byte(test.text))
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case test.matches != nil && result != nil:
			testSubmatchBytes(&test, 0, test.matches[0], result, t)
		}
	}
}

func testSubmatchString(test *FindTest, n int, submatches []int, result []string, t *testing.T) {
	if len(submatches) != len(result)*2 {
		t.Errorf("match %d: expected %d submatches; got %d: %s", n, len(submatches)/2, len(result), test)
		return
	}
	for k := 0; k < len(submatches); k += 2 {
		if submatches[k] == -1 {
			if result[k/2] != "" {
				t.Errorf("match %d: expected nil got %q: %s", n, result, test)
			}
			continue
		}
		expect := test.text[submatches[k]:submatches[k+1]]
		if expect != result[k/2] {
			t.Errorf("match %d: expected %q got %q: %s", n, expect, result, test)
			return
		}
	}
}

func TestFindStringSubmatch(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindStringSubmatch(test.text)
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case test.matches != nil && result != nil:
			testSubmatchString(&test, 0, test.matches[0], result, t)
		}
	}
}

func testSubmatchIndices(test *FindTest, n int, expect, result []int, t *testing.T) {
	if len(expect) != len(result) {
		t.Errorf("match %d: expected %d matches; got %d: %s", n, len(expect)/2, len(result)/2, test)
		return
	}
	for k, e := range expect {
		if e != result[k] {
			t.Errorf("match %d: submatch error: expected %v got %v: %s", n, expect, result, test)
		}
	}
}

func testFindSubmatchIndex(test *FindTest, result []int, t *testing.T) {
	switch {
	case test.matches == nil && result == nil:
		// ok
	case test.matches == nil && result != nil:
		t.Errorf("expected no match; got one: %s", test)
	case test.matches != nil && result == nil:
		t.Errorf("expected match; got none: %s", test)
	case test.matches != nil && result != nil:
		testSubmatchIndices(test, 0, test.matches[0], result, t)
	}
}

func TestFindSubmatchIndex(t *testing.T) {
	for _, test := range findTests {
		testFindSubmatchIndex(&test, MustCompile(test.pat).FindSubmatchIndex([]byte(test.text)), t)
	}
}

func TestFindStringSubmatchIndex(t *testing.T) {
	for _, test := range findTests {
		testFindSubmatchIndex(&test, MustCompile(test.pat).FindStringSubmatchIndex(test.text), t)
	}
}

func TestFindReaderSubmatchIndex(t *testing.T) {
	for _, test := range findTests {
		testFindSubmatchIndex(&test, MustCompile(test.pat).FindReaderSubmatchIndex(strings.NewReader(test.text)), t)
	}
}

// Now come the monster AllSubmatch cases.

func TestFindAllSubmatch(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindAllSubmatch([]byte(test.text), -1)
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case len(test.matches) != len(result):
			t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
		case test.matches != nil && result != nil:
			for k, match := range test.matches {
				testSubmatchBytes(&test, k, match, result[k], t)
			}
		}
	}
}

func TestFindAllStringSubmatch(t *testing.T) {
	for _, test := range findTests {
		result := MustCompile(test.pat).FindAllStringSubmatch(test.text, -1)
		switch {
		case test.matches == nil && result == nil:
			// ok
		case test.matches == nil && result != nil:
			t.Errorf("expected no match; got one: %s", test)
		case test.matches != nil && result == nil:
			t.Errorf("expected match; got none: %s", test)
		case len(test.matches) != len(result):
			t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
		case test.matches != nil && result != nil:
			for k, match := range test.matches {
				testSubmatchString(&test, k, match, result[k], t)
			}
		}
	}
}

func testFindAllSubmatchIndex(test *FindTest, result [][]int, t *testing.T) {
	switch {
	case test.matches == nil && result == nil:
		// ok
	case test.matches == nil && result != nil:
		t.Errorf("expected no match; got one: %s", test)
	case test.matches != nil && result == nil:
		t.Errorf("expected match; got none: %s", test)
	case len(test.matches) != len(result):
		t.Errorf("expected %d matches; got %d: %s", len(test.matches), len(result), test)
	case test.matches != nil && result != nil:
		for k, match := range test.matches {
			testSubmatchIndices(test, k, match, result[k], t)
		}
	}
}

func TestFindAllSubmatchIndex(t *testing.T) {
	for _, test := range findTests {
		testFindAllSubmatchIndex(&test, MustCompile(test.pat).FindAllSubmatchIndex([]byte(test.text), -1), t)
	}
}

func TestFindAllStringSubmatchIndex(t *testing.T) {
	for _, test := range findTests {
		testFindAllSubmatchIndex(&test, MustCompile(test.pat).FindAllStringSubmatchIndex(test.text, -1), t)
	}
}

func BenchmarkLiteral(b *testing.B) {
	x := strings.Repeat("x", 50) + "y"
	b.StopTimer()
	re := MustCompile("y")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if !re.MatchString(x) {
			println("no match!")
			break
		}
	}
}

func BenchmarkNotLiteral(b *testing.B) {
	x := strings.Repeat("x", 50) + "y"
	b.StopTimer()
	re := MustCompile(".y")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if !re.MatchString(x) {
			println("no match!")
			break
		}
	}
}

func BenchmarkMatchClass(b *testing.B) {
	b.StopTimer()
	x := strings.Repeat("xxxx", 20) + "w"
	re := MustCompile("[abcdw]")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if !re.MatchString(x) {
			println("no match!")
			break
		}
	}
}

func BenchmarkMatchClass_InRange(b *testing.B) {
	b.StopTimer()
	// 'b' is between 'a' and 'c', so the charclass
	// range checking is no help here.
	x := strings.Repeat("bbbb", 20) + "c"
	re := MustCompile("[ac]")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if !re.MatchString(x) {
			println("no match!")
			break
		}
	}
}

func BenchmarkReplaceAll(b *testing.B) {
	x := "abcdefghijklmnopqrstuvwxyz"
	b.StopTimer()
	re := MustCompile("[cjrw]")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.ReplaceAllString(x, "")
	}
}

func BenchmarkFindAllStringSubmatchIndex(b *testing.B) {
	x := "abcdefghijklmnopqrstuvwxyz"
	b.StopTimer()
	re := MustCompile("[cjrw]")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.FindAllStringSubmatchIndex(x, 0)
	}
}

func BenchmarkAnchoredLiteralShortNonMatch(b *testing.B) {
	b.StopTimer()
	x := []byte("abcdefghijklmnopqrstuvwxyz")
	re := MustCompile("^zbc(d|e)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.Match(x)
	}
}

func BenchmarkAnchoredLiteralLongNonMatch(b *testing.B) {
	b.StopTimer()
	x := []byte("abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < 15; i++ {
		x = append(x, x...)
	}
	re := MustCompile("^zbc(d|e)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.Match(x)
	}
}

func BenchmarkAnchoredShortMatch(b *testing.B) {
	b.StopTimer()
	x := []byte("abcdefghijklmnopqrstuvwxyz")
	re := MustCompile("^.bc(d|e)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.Match(x)
	}
}

func BenchmarkAnchoredLongMatch(b *testing.B) {
	b.StopTimer()
	x := []byte("abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < 15; i++ {
		x = append(x, x...)
	}
	re := MustCompile("^.bc(d|e)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		re.Match(x)
	}
}
