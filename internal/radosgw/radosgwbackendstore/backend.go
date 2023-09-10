package radosgwbackendstore

import (
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/linode/provider-ceph/apis/v1alpha1"
)

type backend struct {
	radosgwClient *radosgw_admin.API
	active        bool
	health        v1alpha1.HealthStatus
}

func newBackend(radosgwClient *radosgw_admin.API, active bool, health v1alpha1.HealthStatus) *backend {
	return &backend{
		radosgwClient: radosgwClient,
		active:        active,
		health:        health,
	}
}
