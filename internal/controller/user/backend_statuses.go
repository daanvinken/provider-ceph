package user

import (
	"sync"

	"github.com/linode/provider-ceph/apis/radosgw/v1alpha1"
)

type cephUserBackends struct {
	// cephUserBackendStatuses maps cephUser names to backend statuses
	// for backends on which the cephUser exists.
	cephUserBackendStatuses map[string]v1alpha1.BackendStatuses
	mu                      sync.RWMutex
}

func newCephUserBackends() *cephUserBackends {
	return &cephUserBackends{
		cephUserBackendStatuses: make(map[string]v1alpha1.BackendStatuses),
	}
}

func (b *cephUserBackends) setCephUserBackendStatus(cephUserName, backendName string, status v1alpha1.BackendStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cephUserBackendStatuses[cephUserName] == nil {
		b.cephUserBackendStatuses[cephUserName] = make(v1alpha1.BackendStatuses)
	}

	b.cephUserBackendStatuses[cephUserName][backendName] = status
}

func (b *cephUserBackends) deleteCephUserBackend(cephUserName, backendName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.cephUserBackendStatuses[cephUserName]; !ok {
		return
	}

	delete(b.cephUserBackendStatuses[cephUserName], backendName)
}

func (b *cephUserBackends) getCephUserBackendStatuses(cephUserName string, beNames []string) v1alpha1.BackendStatuses {
	requestedBackends := map[string]bool{}
	for p := range beNames {
		requestedBackends[beNames[p]] = true
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	be := make(v1alpha1.BackendStatuses)
	if _, ok := b.cephUserBackendStatuses[cephUserName]; !ok {
		return be
	}

	for k, v := range b.cephUserBackendStatuses[cephUserName] {
		if _, ok := requestedBackends[k]; !ok {
			continue
		}

		be[k] = v
	}

	return be
}
