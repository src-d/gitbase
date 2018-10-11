package enry

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (s *EnryTestSuite) TestIsAuxiliaryLanguage() {
	type testType struct {
		name     string
		lang     string
		expected bool
	}

	tests := []testType{
		{name: "TestIsAuxiliaryLanguage_Invalid", lang: "invalid", expected: false},
	}
	for k := range auxiliaryLanguages {
		t := testType{
			name:     fmt.Sprintf("TestIsAuxiliaryLanguage_%s", k),
			lang:     k,
			expected: true,
		}
		tests = append(tests, t)
	}

	for _, test := range tests {
		is := IsAuxiliaryLanguage(test.lang)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsVendor() {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "TestIsVendor_1", path: "foo/bar", expected: false},
		{name: "TestIsVendor_2", path: "foo/vendor/foo", expected: true},
		{name: "TestIsVendor_3", path: ".sublime-project", expected: true},
		{name: "TestIsVendor_4", path: "leaflet.draw-src.js", expected: true},
		{name: "TestIsVendor_5", path: "foo/bar/MochiKit.js", expected: true},
		{name: "TestIsVendor_6", path: "foo/bar/dojo.js", expected: true},
		{name: "TestIsVendor_7", path: "foo/env/whatever", expected: true},
		{name: "TestIsVendor_8", path: "foo/.imageset/bar", expected: true},
		{name: "TestIsVendor_9", path: "Vagrantfile", expected: true},
	}

	for _, test := range tests {
		is := IsVendor(test.path)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsDocumentation() {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "TestIsDocumentation_1", path: "foo", expected: false},
		{name: "TestIsDocumentation_2", path: "README", expected: true},
	}

	for _, test := range tests {
		is := IsDocumentation(test.path)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsImage() {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "TestIsImage_1", path: "invalid.txt", expected: false},
		{name: "TestIsImage_2", path: "image.png", expected: true},
		{name: "TestIsImage_3", path: "image.jpg", expected: true},
		{name: "TestIsImage_4", path: "image.jpeg", expected: true},
		{name: "TestIsImage_5", path: "image.gif", expected: true},
	}

	for _, test := range tests {
		is := IsImage(test.path)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestGetMimeType() {
	tests := []struct {
		name     string
		path     string
		lang     string
		expected string
	}{
		{name: "TestGetMimeType_1", path: "text.txt", lang: "", expected: "text/plain"},
		{name: "TestGetMimeType_2", path: "file.go", lang: "Go", expected: "text/x-go"},
		{name: "TestGetMimeType_3", path: "image.png", lang: "", expected: "image/png"},
	}

	for _, test := range tests {
		is := GetMimeType(test.path, test.lang)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsConfiguration() {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "TestIsConfiguration_1", path: "foo", expected: false},
		{name: "TestIsConfiguration_2", path: "foo.ini", expected: true},
		{name: "TestIsConfiguration_3", path: "/test/path/foo.json", expected: true},
	}

	for _, test := range tests {
		is := IsConfiguration(test.path)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsBinary() {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{name: "TestIsBinary_1", data: []byte("foo"), expected: false},
		{name: "TestIsBinary_2", data: []byte{0}, expected: true},
		{name: "TestIsBinary_3", data: bytes.Repeat([]byte{'o'}, 8000), expected: false},
	}

	for _, test := range tests {
		is := IsBinary(test.data)
		assert.Equal(s.T(), is, test.expected, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func (s *EnryTestSuite) TestIsDotFile() {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "TestIsDotFile_1", path: "foo/bar/./", expected: false},
		{name: "TestIsDotFile_2", path: "./", expected: false},
	}

	for _, test := range tests {
		is := IsDotFile(test.path)
		assert.Equal(s.T(), test.expected, is, fmt.Sprintf("%v: is = %v, expected: %v", test.name, is, test.expected))
	}
}

func TestFileCountListSort(t *testing.T) {
	sampleData := FileCountList{{"a", 8}, {"b", 65}, {"c", 20}, {"d", 90}}
	const ascending = "ASC"
	const descending = "DESC"

	tests := []struct {
		name         string
		data         FileCountList
		order        string
		expectedData FileCountList
	}{
		{
			name:         "ascending order",
			data:         sampleData,
			order:        ascending,
			expectedData: FileCountList{{"a", 8}, {"c", 20}, {"b", 65}, {"d", 90}},
		},
		{
			name:         "descending order",
			data:         sampleData,
			order:        descending,
			expectedData: FileCountList{{"d", 90}, {"b", 65}, {"c", 20}, {"a", 8}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.order == descending {
				sort.Sort(sort.Reverse(test.data))
			} else {
				sort.Sort(test.data)
			}

			for i := 0; i < len(test.data); i++ {
				assert.Equal(t, test.data[i], test.expectedData[i], fmt.Sprintf("%v: FileCount at position %d = %v, expected: %v", test.name, i, test.data[i], test.expectedData[i]))
			}
		})
	}
}
