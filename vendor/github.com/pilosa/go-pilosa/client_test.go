// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"crypto/tls"
	"errors"
	"reflect"
	"testing"
)

func TestQueryWithError(t *testing.T) {
	var err error
	client := DefaultClient()
	index := NewIndex("foo")
	field := index.Field("foo")
	invalid := field.FilterAttrTopN(12, field.Row(7), "$invalid$", 80, 81)
	_, err = client.Query(invalid, nil)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestClientOptions(t *testing.T) {
	targets := []*ClientOptions{
		{SocketTimeout: 10},
		{ConnectTimeout: 5},
		{PoolSizePerRoute: 7},
		{TotalPoolSize: 17},
		{TLSConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	optionsList := [][]ClientOption{
		{OptClientSocketTimeout(10)},
		{OptClientConnectTimeout(5)},
		{OptClientPoolSizePerRoute(7)},
		{OptClientTotalPoolSize(17)},
		{OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true})},
	}

	for i := 0; i < len(targets); i++ {
		options := &ClientOptions{}
		err := options.addOptions(optionsList[i]...)
		if err != nil {
			t.Fatal(err)
		}
		target := targets[i]
		if !reflect.DeepEqual(target, options) {
			t.Fatalf("%v != %v", target, options)
		}
	}
}

func TestNewClientWithErrorredOption(t *testing.T) {
	_, err := NewClient(":8888", ClientOptionErr(0))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestNewClient(t *testing.T) {
	client, err := NewClient(":9999")
	if err != nil {
		t.Fatal(err)
	}
	target := []*URI{URIFromAddress(":9999")}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}
	client, err = NewClient(":invalid")
	if err == nil {
		t.Fatalf("should have failed")
	}

	client, err = NewClient([]*URI{URIFromAddress(":9999"), URIFromAddress(":8888")})
	if err != nil {
		t.Fatal(err)
	}
	target = []*URI{URIFromAddress(":9999"), URIFromAddress(":8888")}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}

	client, err = NewClient(DefaultCluster())
	if err != nil {
		t.Fatal(err)
	}
	target = []*URI{}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}
}

func TestNewClientWithInvalidAddr(t *testing.T) {
	_, err := NewClient(10)
	if err != ErrAddrURIClusterExpected {
		t.Fatalf("%v != %v", ErrAddrURIClusterExpected, err)
	}
}

func ClientOptionErr(int) ClientOption {
	return func(*ClientOptions) error {
		return errors.New("Some error")
	}
}

func TestQueryOptions(t *testing.T) {
	targets := []*QueryOptions{
		{ColumnAttrs: true},
		{ColumnAttrs: false},
		{ExcludeRowAttrs: true},
		{ExcludeRowAttrs: false},
		{ExcludeColumns: true},
		{ExcludeColumns: false},
	}

	optionsList := [][]interface{}{
		{OptQueryColumnAttrs(true)},
		{OptQueryColumnAttrs(false)},
		{OptQueryExcludeAttrs(true)},
		{OptQueryExcludeAttrs(false)},
		{OptQueryExcludeColumns(true)},
		{OptQueryExcludeColumns(false)},
	}

	for i := 0; i < len(targets); i++ {
		options := &QueryOptions{}
		err := options.addOptions(optionsList[i]...)
		if err != nil {
			t.Fatal(err)
		}
		target := targets[i]
		if !reflect.DeepEqual(target, options) {
			t.Fatalf("%v != %v", target, options)
		}
	}

	target := &QueryOptions{
		ColumnAttrs:     true,
		ExcludeRowAttrs: true,
		ExcludeColumns:  true,
	}
	options := &QueryOptions{}
	options.addOptions(&QueryOptions{
		ColumnAttrs:     true,
		ExcludeRowAttrs: true,
		ExcludeColumns:  true,
	})
	if !reflect.DeepEqual(target, options) {
		t.Fatalf("%v != %v", target, options)
	}
}

func TestQueryOptionsWithError(t *testing.T) {
	options := &QueryOptions{}
	err := options.addOptions(1)
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(OptQueryColumnAttrs(true), nil)
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(OptQueryColumnAttrs(true), &QueryOptions{})
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(QueryOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestQueryOptionsError(t *testing.T) {
	client := DefaultClient()
	index := NewIndex("foo")
	_, err := client.Query(index.RawQuery(""), QueryOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func QueryOptionErr(int) QueryOption {
	return func(*QueryOptions) error {
		return errors.New("Some error")
	}
}
