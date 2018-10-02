/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpcclient

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	vtgatepb "gopkg.in/src-d/go-vitess.v1/vt/proto/vtgate"
	vtgateservicepb "gopkg.in/src-d/go-vitess.v1/vt/proto/vtgateservice"
)

func TestDialErrors(t *testing.T) {
	addresses := []string{
		"badhost",
		"badhost:123456",
		"[::]:12346",
	}
	wantErr := "Unavailable"
	for _, address := range addresses {
		gconn, err := Dial(address, FailFast(true), grpc.WithInsecure())
		if err != nil {
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("Dial(%s, FailFast=true): %v, must contain %s", address, err, wantErr)
		}
	}

	wantErr = "DeadlineExceeded"
	for _, address := range addresses {
		gconn, err := Dial(address, FailFast(false), grpc.WithInsecure())
		if err != nil {
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("Dial(%s, FailFast=false): %v, must contain %s", address, err, wantErr)
		}
	}
}
