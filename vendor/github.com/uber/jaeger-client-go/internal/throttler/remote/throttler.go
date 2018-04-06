// Copyright (c) 2018 The Jaeger Authors.
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

package remote

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/utils"
)

const (
	// minimumCredits is the minimum amount of credits necessary to not be throttled.
	// i.e. if currentCredits > minimumCredits, then the operation will not be throttled.
	minimumCredits = 1.0
)

var (
	errorUUIDNotSet = errors.New("Throttler uuid must be set")
)

type creditResponse struct {
	Operation string  `json:"operation"`
	Credits   float64 `json:"credits"`
}

type httpCreditManagerProxy struct {
	hostPort string
}

func newHTTPCreditManagerProxy(hostPort string) *httpCreditManagerProxy {
	return &httpCreditManagerProxy{
		hostPort: hostPort,
	}
}

func (m *httpCreditManagerProxy) FetchCredits(uuid, serviceName string, operation []string) ([]creditResponse, error) {
	params := url.Values{}
	params.Set("service", serviceName)
	params.Set("uuid", uuid)
	for _, op := range operation {
		params.Add("operation", op)
	}
	var resp []creditResponse
	if err := utils.GetJSON(fmt.Sprintf("http://%s/credits?%s", m.hostPort, params.Encode()), &resp); err != nil {
		return nil, errors.Wrap(err, "Failed to receive credits from agent")
	}
	return resp, nil
}

// Throttler retrieves credits from agent and uses it to throttle operations.
type Throttler struct {
	options

	mux           sync.RWMutex
	service       string
	uuid          atomic.Value
	creditManager *httpCreditManagerProxy
	credits       map[string]float64 // map of operation->credits
	close         chan struct{}
	stopped       sync.WaitGroup
}

// NewThrottler returns a Throttler that polls agent for credits and uses them to throttle
// the service.
func NewThrottler(service string, options ...Option) *Throttler {
	// TODO add metrics
	// TODO set a limit on the max number of credits
	opts := applyOptions(options...)
	creditManager := newHTTPCreditManagerProxy(opts.hostPort)
	t := &Throttler{
		options:       opts,
		creditManager: creditManager,
		service:       service,
		credits:       make(map[string]float64),
		close:         make(chan struct{}),
	}
	t.stopped.Add(1)
	go t.pollManager()
	return t
}

// IsAllowed implements Throttler#IsAllowed.
func (t *Throttler) IsAllowed(operation string) bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	_, ok := t.credits[operation]
	if !ok {
		if !t.synchronousInitialization {
			t.credits[operation] = 0
			return false
		}
		// If it is the first time this operation is being checked, synchronously fetch
		// the credits.
		credits, err := t.fetchCredits([]string{operation})
		if err != nil {
			// Failed to receive credits from agent, try again next time
			t.logger.Error("Failed to fetch credits: " + err.Error())
			return false
		}
		if len(credits) == 0 {
			// This shouldn't happen but just in case
			return false
		}
		t.credits[operation] = credits[0].Credits
	}
	return t.isAllowed(operation)
}

// Close stops the throttler from fetching credits from remote.
func (t *Throttler) Close() error {
	close(t.close)
	t.stopped.Wait()
	return nil
}

// SetProcess implements ProcessSetter#SetProcess. It's imperative that the UUID is set before any remote
// requests are made.
func (t *Throttler) SetProcess(process jaeger.Process) {
	t.uuid.Store(process.UUID)
}

// N.B. This function must be called with the Write Lock
func (t *Throttler) isAllowed(operation string) bool {
	credits := t.credits[operation]
	if credits < minimumCredits {
		return false
	}
	t.credits[operation] = credits - minimumCredits
	return true
}

func (t *Throttler) pollManager() {
	defer t.stopped.Done()
	ticker := time.NewTicker(t.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.refreshCredits()
		case <-t.close:
			return
		}
	}
}

func (t *Throttler) refreshCredits() {
	t.mux.RLock()
	operations := make([]string, 0, len(t.credits))
	for op := range t.credits {
		operations = append(operations, op)
	}
	t.mux.RUnlock()
	newCredits, err := t.fetchCredits(operations)
	if err != nil {
		t.logger.Error("Failed to fetch credits: " + err.Error())
		return
	}

	t.mux.Lock()
	defer t.mux.Unlock()
	for _, credit := range newCredits {
		t.credits[credit.Operation] += credit.Credits
	}
}

func (t *Throttler) fetchCredits(operations []string) ([]creditResponse, error) {
	uuid := t.uuid.Load()
	uuidStr, _ := uuid.(string)
	if uuid == nil || uuidStr == "" {
		return nil, errorUUIDNotSet
	}
	return t.creditManager.FetchCredits(uuidStr, t.service, operations)
}
