/*
Copyright 2011 Google Inc.

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

package memcache

import (
	"hash/crc32"
	"net"
	"strings"
	"sync"

	"github.com/hailocab/circuitbreaker"
)

// ServerSelector is the interface that selects a memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be safe for concurrent use
// by multiple goroutines.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, DoneFunc, error)
	Each(func(net.Addr) error) error
}

type DoneFunc func(error)

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	// Options
	BreakerRate       float64
	BreakerMinSamples int64

	mu    sync.RWMutex
	addrs []net.Addr
	cbs   map[net.Addr]*circuit.Breaker
}

// SetServers changes a ServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error {
	// Get breaker options
	cbRate := 0.95
	cbMinSamples := int64(25)

	if ss.BreakerRate > 0 {
		cbRate = ss.BreakerRate
	}
	if ss.BreakerMinSamples > 0 {
		cbMinSamples = ss.BreakerMinSamples
	}

	naddr := make([]net.Addr, len(servers))
	cbs := make(map[net.Addr]*circuit.Breaker, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = addr
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = tcpaddr
		}

		// Setup breaker
		cbs[naddr[i]] = circuit.NewRateBreaker(cbRate, cbMinSamples)
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr
	ss.cbs = cbs
	return nil
}

// Each iterates over each server calling the given function
func (ss *ServerList) Each(f func(net.Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

func (ss *ServerList) PickServer(key string) (net.Addr, DoneFunc, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, circuitBreakerDoneFunc(nil), ErrNoServers
	}
	var addr net.Addr
	if len(ss.addrs) == 1 {
		addr = ss.addrs[0]
	} else {
		bufp := keyBufPool.Get().(*[]byte)
		n := copy(*bufp, key)
		cs := crc32.ChecksumIEEE((*bufp)[:n])
		keyBufPool.Put(bufp)

		addr = ss.addrs[cs%uint32(len(ss.addrs))]
	}

	cb := ss.cbs[addr]

	if !cb.Ready() {
		return nil, circuitBreakerDoneFunc(nil), ErrServerUnavailable
	}

	return addr, circuitBreakerDoneFunc(cb), nil
}

func circuitBreakerDoneFunc(cb *circuit.Breaker) DoneFunc {
	return func(err error) {
		if cb == nil {
			return
		}

		if err == nil {
			cb.Success()
		} else {
			cb.Fail()
		}
	}
}
