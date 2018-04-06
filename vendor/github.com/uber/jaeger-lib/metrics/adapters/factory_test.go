// Copyright (c) 2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adapters

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-lib/metrics"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions(Options{})
	assert.Equal(t, ".", o.ScopeSep)
	assert.Equal(t, ".", o.TagsSep)
	assert.Equal(t, "_", o.TagKVSep)
}

func TestSubScope(t *testing.T) {
	f := &factory{
		Options: defaultOptions(Options{}),
	}
	assert.Equal(t, "", f.subScope(""))
	assert.Equal(t, "x", f.subScope("x"))
	f.scope = "x"
	assert.Equal(t, "x", f.subScope(""))
	assert.Equal(t, "x.y", f.subScope("y"))
}

func TestFactory(t *testing.T) {
	var (
		counterPrefix = "counter_"
		gaugePrefix   = "gauge_"
		timerPrefix   = "timer_"

		tagsA = map[string]string{"a": "b"}
		tagsX = map[string]string{"x": "y"}
	)

	testCases := []struct {
		name            string
		tags            map[string]string
		namespace       string
		nsTags          map[string]string
		fullName        string
		expectedCounter string
	}{
		{name: "x", fullName: "%sx"},
		{tags: tagsX, fullName: "%s.x_y"},
		{name: "x", tags: tagsA, fullName: "%sx.a_b"},
		{namespace: "y", fullName: "y.%s"},
		{nsTags: tagsA, fullName: "%s.a_b"},
		{namespace: "y", nsTags: tagsX, fullName: "y.%s.x_y"},
		{name: "x", namespace: "y", nsTags: tagsX, fullName: "y.%sx.x_y"},
		{name: "x", tags: tagsX, namespace: "y", nsTags: tagsX, fullName: "y.%sx.x_y", expectedCounter: "84"},
		{name: "x", tags: tagsA, namespace: "y", nsTags: tagsX, fullName: "y.%sx.a_b.x_y"},
		{name: "x", tags: tagsX, namespace: "y", nsTags: tagsA, fullName: "y.%sx.a_b.x_y", expectedCounter: "84"},
	}
	local := metrics.NewLocalFactory(100 * time.Second)
	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			if testCase.expectedCounter == "" {
				testCase.expectedCounter = "42"
			}
			ff := &fakeTagless{factory: local}
			f := WrapFactoryWithoutTags(ff, Options{})
			if testCase.namespace != "" || testCase.nsTags != nil {
				f = f.Namespace(testCase.namespace, testCase.nsTags)
			}
			counter := f.Counter(counterPrefix+testCase.name, testCase.tags)
			gauge := f.Gauge(gaugePrefix+testCase.name, testCase.tags)
			timer := f.Timer(timerPrefix+testCase.name, testCase.tags)

			assert.Equal(t, counter, f.Counter(counterPrefix+testCase.name, testCase.tags))
			assert.Equal(t, gauge, f.Gauge(gaugePrefix+testCase.name, testCase.tags))
			assert.Equal(t, timer, f.Timer(timerPrefix+testCase.name, testCase.tags))

			assert.Equal(t, fmt.Sprintf(testCase.fullName, counterPrefix), ff.counter)
			assert.Equal(t, fmt.Sprintf(testCase.fullName, gaugePrefix), ff.gauge)
			assert.Equal(t, fmt.Sprintf(testCase.fullName, timerPrefix), ff.timer)
		})
	}
}

type fakeTagless struct {
	factory metrics.Factory
	counter string
	gauge   string
	timer   string
}

func (f *fakeTagless) Counter(name string) metrics.Counter {
	f.counter = name
	return f.factory.Counter(name, nil)
}

func (f *fakeTagless) Gauge(name string) metrics.Gauge {
	f.gauge = name
	return f.factory.Gauge(name, nil)
}

func (f *fakeTagless) Timer(name string) metrics.Timer {
	f.timer = name
	return f.factory.Timer(name, nil)
}
