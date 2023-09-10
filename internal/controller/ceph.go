/*
Copyright 2020 The Crossplane Authors.

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

package controller

import (
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/linode/provider-ceph/internal/controller/bucket"
	"github.com/linode/provider-ceph/internal/radosgw/radosgwbackendstore"
	"github.com/linode/provider-ceph/internal/s3/s3backendstore"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/linode/provider-ceph/internal/controller/providerconfig"
	"github.com/linode/provider-ceph/internal/controller/user"
)

// Setup creates all Ceph controllers with the supplied logger and adds them to
// the supplied manager.
func Setup(mgr ctrl.Manager, o controller.Options, s3backendStore *s3backendstore.BackendStore, radosgwbackendStore *radosgwbackendstore.BackendStore) error {
	for _, setup := range []func(ctrl.Manager, controller.Options, *s3backendstore.BackendStore) error{
		bucket.Setup,
	} {
		if err := setup(mgr, o, s3backendStore); err != nil {
			return err
		}
	}

	for _, setup := range []func(ctrl.Manager, controller.Options, *s3backendstore.BackendStore, *radosgwbackendstore.BackendStore) error{
		providerconfig.Setup,
	} {
		if err := setup(mgr, o, s3backendStore, radosgwbackendStore); err != nil {
			return err
		}
	}

	for _, setup := range []func(ctrl.Manager, controller.Options, *radosgwbackendstore.BackendStore) error{
		user.Setup,
	} {
		if err := setup(mgr, o, radosgwbackendStore); err != nil {
			return err
		}
	}

	return nil
}
