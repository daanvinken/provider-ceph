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

package providerconfig

import (
	"context"
	"github.com/linode/provider-ceph/internal/radosgw/radosgwbackendstore"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/providerconfig"

	apisv1alpha1 "github.com/linode/provider-ceph/apis/v1alpha1"
	radosgwinternal "github.com/linode/provider-ceph/internal/radosgw"
)

const (
	errCreateRadosgwClient = "cannot create radosgw client"
	// #nosec
	errGetRadosgwSecret        = "cannot get Secret"
	errBackendRadosgwNotStored = "radosgw backend is not stored"
)

func newRadosgwBackendStoreReconciler(k client.Client, o controller.Options, s *radosgwbackendstore.BackendStore) *RadosgwBackendStoreReconciler {
	return &RadosgwBackendStoreReconciler{
		kube:         k,
		backendStore: s,
		log:          o.Logger.WithValues("backend-store-controller", providerconfig.ControllerName(apisv1alpha1.ProviderConfigGroupKind)),
	}
}

type RadosgwBackendStoreReconciler struct {
	kube         client.Client
	backendStore *radosgwbackendstore.BackendStore
	log          logging.Logger
}

func (r *RadosgwBackendStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.log.Info("Reconciling radosgw backend store", "name", req.Name)
	providerConfig := &apisv1alpha1.ProviderConfig{}
	if err := r.kube.Get(ctx, req.NamespacedName, providerConfig); err != nil {
		if kerrors.IsNotFound(err) {
			r.log.Info("Marking radosgw backend as inactive on backend store", "name", req.Name)
			r.backendStore.ToggleBackendActiveStatus(req.Name, false)
			r.backendStore.SetBackendHealthStatus(req.Name, apisv1alpha1.HealthStatusUnknown)

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}
	// ProviderConfig has been created or updated, add or
	// update its backend in the backend store.
	return ctrl.Result{}, r.addOrUpdateBackend(ctx, providerConfig)
}

func (r *RadosgwBackendStoreReconciler) addOrUpdateBackend(ctx context.Context, pc *apisv1alpha1.ProviderConfig) error {
	secret, err := r.getProviderConfigSecret(ctx, pc.Spec.Credentials.SecretRef.Namespace, pc.Spec.Credentials.SecretRef.Name)
	if err != nil {
		return err
	}

	radosgwclient, err := radosgwinternal.NewClient(ctx, secret.Data, &pc.Spec)
	if err != nil {
		return errors.Wrap(err, errCreateRadosgwClient)
	}

	var health apisv1alpha1.HealthStatus
	health = apisv1alpha1.HealthStatusUnknown
	if pc.Status.Health != "" {
		health = pc.Status.Health
	}

	r.backendStore.AddOrUpdateBackend(pc.Name, radosgwclient, true, health)

	return nil
}

func (r *RadosgwBackendStoreReconciler) getProviderConfigSecret(ctx context.Context, secretNamespace, secretName string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	ns := types.NamespacedName{Namespace: secretNamespace, Name: secretName}
	if err := r.kube.Get(ctx, ns, secret); err != nil {
		return nil, errors.Wrap(err, "cannot get provider secret")
	}

	return secret, nil
}

func (r *RadosgwBackendStoreReconciler) setupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apisv1alpha1.ProviderConfig{}).
		Complete(r)
}
