// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package traces // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor/internal/traces"

import (
	"context"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type MemoryStorage struct {
	sync.RWMutex
	content                   map[pcommon.TraceID][]ptrace.ResourceSpans
	stopped                   bool
	stoppedLock               sync.RWMutex
	metricsCollectionInterval time.Duration
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		content:                   make(map[pcommon.TraceID][]ptrace.ResourceSpans),
		metricsCollectionInterval: time.Second,
	}
}

func (st *MemoryStorage) createOrAppend(traceID pcommon.TraceID, td ptrace.Traces) error {
	st.Lock()
	defer st.Unlock()

	// getting zero value is fine
	content := st.content[traceID]

	newRss := ptrace.NewResourceSpansSlice()
	td.ResourceSpans().CopyTo(newRss)
	for i := 0; i < newRss.Len(); i++ {
		content = append(content, newRss.At(i))
	}
	st.content[traceID] = content

	return nil
}
func (st *MemoryStorage) get(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	st.RLock()
	rss, ok := st.content[traceID]
	st.RUnlock()
	if !ok {
		return nil, nil
	}

	var result []ptrace.ResourceSpans
	for _, rs := range rss {
		newRS := ptrace.NewResourceSpans()
		rs.CopyTo(newRS)
		result = append(result, newRS)
	}

	return result, nil
}

// delete will return a reference to a ResourceSpans. Changes to the returned object may not be applied
// to the version in the storage.
func (st *MemoryStorage) delete(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	st.Lock()
	defer st.Unlock()

	defer delete(st.content, traceID)
	return st.content[traceID], nil
}

func (st *MemoryStorage) start() error {
	go st.periodicMetrics()
	return nil
}

func (st *MemoryStorage) shutdown() error {
	st.stoppedLock.Lock()
	defer st.stoppedLock.Unlock()
	st.stopped = true
	return nil
}

func (st *MemoryStorage) periodicMetrics() {
	numTraces := st.count()
	stats.Record(context.Background(), mNumTracesInMemory.M(int64(numTraces)))

	st.stoppedLock.RLock()
	stopped := st.stopped
	st.stoppedLock.RUnlock()
	if stopped {
		return
	}

	time.AfterFunc(st.metricsCollectionInterval, func() {
		st.periodicMetrics()
	})
}

func (st *MemoryStorage) count() int {
	st.RLock()
	defer st.RUnlock()
	return len(st.content)
}
