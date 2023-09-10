package radosgwbackendstore

import (
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	"sync"

	"github.com/linode/provider-ceph/apis/v1alpha1"
)

// cephUserBackends is a map of CephUser backend name (eg radosgw cluster name) to backend.
type cephUserBackends map[string]*backend

// BackendStore stores the active cephUser backends.
type BackendStore struct {
	cephUserBackends cephUserBackends
	mu               sync.RWMutex
}

func NewBackendStore() *BackendStore {
	return &BackendStore{
		cephUserBackends: make(cephUserBackends),
	}
}

func (b *BackendStore) GetBackendClient(backendName string) *radosgw_admin.API {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.cephUserBackends[backendName]; ok {
		return b.cephUserBackends[backendName].radosgwClient
	}

	return nil
}

func (b *BackendStore) GetAllBackendClients() []*radosgw_admin.API {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a new clients slice hold a copy of the backend clients
	clients := make([]*radosgw_admin.API, 0)
	for _, v := range b.cephUserBackends {
		clients = append(clients, v.radosgwClient)
	}

	return clients
}

func (b *BackendStore) GetBackendClients(beNames []string) []*radosgw_admin.API {
	requestedBackends := map[string]bool{}
	for p := range beNames {
		requestedBackends[beNames[p]] = true
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a new clients slice hold a copy of the backend clients
	clients := make([]*radosgw_admin.API, 0)
	for k, v := range b.cephUserBackends {
		if _, ok := requestedBackends[k]; !ok {
			continue
		}
		clients = append(clients, v.radosgwClient)
	}

	return clients
}

func (b *BackendStore) IsBackendActive(backendName string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.cephUserBackends[backendName]; ok {
		return b.cephUserBackends[backendName].active
	}

	return false
}

func (b *BackendStore) ToggleBackendActiveStatus(backendName string, active bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.cephUserBackends[backendName]; ok {
		b.cephUserBackends[backendName].active = active
	}
}

func (b *BackendStore) GetBackendHealthStatus(backendName string) v1alpha1.HealthStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.cephUserBackends[backendName]; ok {
		return b.cephUserBackends[backendName].health
	}

	return v1alpha1.HealthStatusUnknown
}

func (b *BackendStore) SetBackendHealthStatus(backendName string, health v1alpha1.HealthStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.cephUserBackends[backendName]; ok {
		b.cephUserBackends[backendName].health = health
	}
}

func (b *BackendStore) DeleteBackend(backendName string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.cephUserBackends, backendName)
}

func (b *BackendStore) AddOrUpdateBackend(backendName string, backendClient *radosgw_admin.API, active bool, health v1alpha1.HealthStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.cephUserBackends[backendName] = newBackend(backendClient, active, health)
}

func (b *BackendStore) GetBackend(backendName string) *backend {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if backend, ok := b.cephUserBackends[backendName]; ok {
		return backend
	}

	return nil
}

func (b *BackendStore) GetAllBackends() cephUserBackends {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a new cephUserBackends to hold a copy of the backends
	backends := make(cephUserBackends, len(b.cephUserBackends))
	for k, v := range b.cephUserBackends {
		backends[k] = v
	}

	return backends
}

func (b *BackendStore) GetActiveBackends(beNames []string) cephUserBackends {
	requestedBackends := map[string]bool{}
	for p := range beNames {
		requestedBackends[beNames[p]] = true
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a new cephUserBackends to hold a copy of the backends
	backends := make(cephUserBackends, 0)
	for k, v := range b.cephUserBackends {
		if _, ok := requestedBackends[k]; !ok || !v.active {
			continue
		}

		backends[k] = v
	}

	return backends
}

func (b *BackendStore) GetAllActiveBackendNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	backends := make([]string, 0)
	for k, v := range b.cephUserBackends {
		if !v.active {
			continue
		}

		backends = append(backends, k)
	}

	return backends
}

func (b *BackendStore) BackendsAreStored() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.cephUserBackends) != 0
}
