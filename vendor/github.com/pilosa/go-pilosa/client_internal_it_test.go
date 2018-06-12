// +build integration

package pilosa

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestNewClientFromAddresses(t *testing.T) {
	cli, err := NewClient([]string{":10101", "node0.pilosa.com:10101", "node2.pilosa.com"})

	if err != nil {
		t.Fatalf("Creating client from addresses: %v", err)
	}
	expectedHosts := []URI{
		{scheme: "http", port: 10101, host: "localhost"},
		{scheme: "http", port: 10101, host: "node0.pilosa.com"},
		{scheme: "http", port: 10101, host: "node2.pilosa.com"},
	}
	actualHosts := cli.cluster.Hosts()
	if !reflect.DeepEqual(actualHosts, expectedHosts) {
		t.Fatalf("Unexpected hosts in client's cluster, got: %v, expected: %v", actualHosts, expectedHosts)
	}

	cli, err = NewClient([]string{"://"})
	if err == nil {
		t.Fatalf("Did not get expected error when creating client: %v", cli.cluster.Hosts())
	}

	cli, err = NewClient([]string{})
	if err != nil {
		t.Fatalf("Got error when creating empty client from addresses: %v", err)
	}

	cli, err = NewClient(nil)
	if err != nil {
		t.Fatalf("Got error when creating empty client from addresses: %v", err)
	}
}

func TestAnyError(t *testing.T) {
	err := anyError(
		&http.Response{StatusCode: 400,
			Body: ioutil.NopCloser(iotest.TimeoutReader(bytes.NewBuffer([]byte("asdf"))))},
		nil)
	if err == nil {
		t.Fatalf("should have gotten an error")
	}
}
