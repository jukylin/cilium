// Copyright 2018 Authors of Cilium
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"time"

	"github.com/cilium/cilium/pkg/lock"
)

var (
	requestLifetime = 2 * time.Minute
)

// CorrelationID is the correlation ID in the Kafka protocol
type CorrelationID int32

type requestsCache map[CorrelationID]*RequestMessage

// CorrelationCache is a cache of requests to correlate responses with requests
type CorrelationCache struct {
	cache  requestsCache
	mutex  lock.RWMutex
	stopGc chan struct{}
}

// NewCorrelationCache returns a new correlation cache
func NewCorrelationCache() *CorrelationCache {
	cc := &CorrelationCache{
		cache:  requestsCache{},
		stopGc: make(chan struct{}),
	}

	go cc.garbageCollector()

	return cc
}

// DeleteCache releases the cache and stops the garbage collector. This
// function must be called when the cache is no longer required, otherwise go
// routines are leaked.
func (cc *CorrelationCache) DeleteCache() {
	close(cc.stopGc)
}

// Insert inserts a request into the correlation cache. After insertion, the
// request can be correlated with responses via Correlate() or
// CorrelateResponse()
func (cc *CorrelationCache) Insert(req *RequestMessage) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	correlationID := CorrelationID(req.GetCorrelationID())
	if correlationID == 0 {
		log.Warning("Correlation ID is zero")
		return
	}

	if _, ok := cc.cache[correlationID]; ok {
		log.Warning("Overwriting Kafka request message in correlation cache")
	}

	req.created = time.Now()
	cc.cache[correlationID] = req
}

// Delete removes a request from the correlation cache. After deletion, the
// request will no longer be correlated with responses. Typically this is done
// when the request has been responded to.
func (cc *CorrelationCache) Delete(req *RequestMessage) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	correlationID := CorrelationID(req.GetCorrelationID())
	if correlationID != 0 {
		delete(cc.cache, correlationID)
	}
}

// Correlate returns the request message with the matching correlation ID
func (cc *CorrelationCache) Correlate(id CorrelationID) *RequestMessage {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()

	req, _ := cc.cache[id]
	return req
}

// CorrelateResponse extracts the correlation ID from the response message and
// correlates the corresponding request
func (cc *CorrelationCache) CorrelateResponse(res *ResponseMessage) *RequestMessage {
	return cc.Correlate(CorrelationID(res.GetCorrelationID()))
}

func (cc *CorrelationCache) garbageCollector() {
	for {
		select {
		case <-cc.stopGc:
			return
		default:
		}

		// calculate the creation time for expiration, entries created
		// prior to this timestamp must be expired
		expiryCreationTime := time.Now().Add(-requestLifetime)

		log.WithField("expiryCreationTime", expiryCreationTime).
			Debug("Running Kafka correlation cache garbage collector")

		cc.mutex.Lock()
		for correlationID, req := range cc.cache {
			if req.created.After(expiryCreationTime) {
				log.WithField(fieldRequest, req).Debug("Request expired in cache, removing")
				delete(cc.cache, correlationID)
			}
		}
		cc.mutex.Unlock()

		time.Sleep(requestLifetime)
	}
}
