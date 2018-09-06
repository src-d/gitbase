// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rubex

import (
	"errors"
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
			t.Errorf("%q.ReplaceFunc(%q,%q) = %q; want %q",
				tc.pattern, tc.input, tc.replacement, actual, tc.output)
		}
		// now try bytes
		actual = string(re.ReplaceAllFunc([]byte(tc.input), func(s []byte) []byte { return []byte(tc.replacement(string(s))) }))
		if actual != tc.output {
			t.Errorf("%q.ReplaceFunc(%q,%q) = %q; want %q",
				tc.pattern, tc.input, tc.replacement, actual, tc.output)
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

/*
 * LiteralPrefix is not supported by rubex
 *
//LiteralPrefix
func TestLiteralPrefix(t *testing.T) {
	for _, tc := range metaTests {
		// Literal method needs to scan the pattern.
		re := MustCompile(tc.pattern)
		str, complete := re.LiteralPrefix()
		if complete != tc.isLiteral {
			t.Errorf("LiteralPrefix(`%s`) = %t; want %t", tc.pattern, complete, tc.isLiteral)
		}
		if str != tc.literal {
			t.Errorf("LiteralPrefix(`%s`) = `%s`; want `%s`", tc.pattern, str, tc.literal)
		}
	}
}
*/
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
