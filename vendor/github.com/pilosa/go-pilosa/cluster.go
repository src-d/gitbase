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
	"sync"
)

// Cluster contains hosts in a Pilosa cluster.
type Cluster struct {
	hosts       []*URI
	okList      []bool
	mutex       *sync.RWMutex
	lastHostIdx int
}

// DefaultCluster returns the default Cluster.
func DefaultCluster() *Cluster {
	return &Cluster{
		hosts:  make([]*URI, 0),
		okList: make([]bool, 0),
		mutex:  &sync.RWMutex{},
	}
}

// NewClusterWithHost returns a cluster with the given URIs.
func NewClusterWithHost(hosts ...*URI) *Cluster {
	cluster := DefaultCluster()
	for _, host := range hosts {
		cluster.AddHost(host)
	}
	return cluster
}

// AddHost adds a host to the cluster.
func (c *Cluster) AddHost(address *URI) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.hosts = append(c.hosts, address)
	c.okList = append(c.okList, true)
}

// Host returns a host in the cluster.
func (c *Cluster) Host() *URI {
	c.mutex.Lock()
	var host *URI
	for i := range c.okList {
		idx := (i + c.lastHostIdx) % len(c.okList)
		ok := c.okList[idx]
		if ok {
			host = c.hosts[idx]
			break
		}
	}
	c.lastHostIdx++
	c.mutex.Unlock()
	if host != nil {
		return host
	}
	c.reset()
	return host
}

// RemoveHost black lists the host with the given URI from the cluster.
func (c *Cluster) RemoveHost(address *URI) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, uri := range c.hosts {
		if uri.Equals(address) {
			c.okList[i] = false
			break
		}
	}
}

// Hosts returns all available hosts in the cluster.
func (c *Cluster) Hosts() []URI {
	hosts := make([]URI, 0, len(c.hosts))
	for i, host := range c.hosts {
		if c.okList[i] {
			hosts = append(hosts, *host)
		}
	}
	return hosts
}

func (c *Cluster) reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i := range c.okList {
		c.okList[i] = true
	}
}
