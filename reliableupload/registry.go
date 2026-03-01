package reliableupload

import (
	"fmt"
	"sync"
)

// Registry stores per-task datasource/reporter implementations.
type Registry struct {
	mu          sync.RWMutex
	dataSources map[string]DataSource
	reporters   map[string]Reporter
}

func NewRegistry() *Registry {
	return &Registry{
		dataSources: make(map[string]DataSource),
		reporters:   make(map[string]Reporter),
	}
}

func (r *Registry) RegisterDataSource(taskCode string, ds DataSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dataSources[taskCode] = ds
}

func (r *Registry) RegisterReporter(taskCode string, rp Reporter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reporters[taskCode] = rp
}

func (r *Registry) DataSource(taskCode string) (DataSource, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ds, ok := r.dataSources[taskCode]
	if !ok {
		return nil, fmt.Errorf("datasource not registered for task_code=%s", taskCode)
	}
	return ds, nil
}

func (r *Registry) Reporter(taskCode string) (Reporter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rp, ok := r.reporters[taskCode]
	if !ok {
		return nil, fmt.Errorf("reporter not registered for task_code=%s", taskCode)
	}
	return rp, nil
}
