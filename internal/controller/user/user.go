/*
Copyright 2022 The Crossplane Authors.

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

package user

import (
	"context"
	"github.com/allegro/bigcache/v3"
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/linode/provider-ceph/apis/radosgw/v1alpha1"
	apisv1alpha1 "github.com/linode/provider-ceph/apis/v1alpha1"
	"github.com/linode/provider-ceph/internal/features"
	"github.com/linode/provider-ceph/internal/radosgw"
	"github.com/linode/provider-ceph/internal/radosgw/radosgwbackendstore"
	s3internal "github.com/linode/provider-ceph/internal/s3"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strings"
	"sync"
	"time"
)

const (
	errNotCephUser                = "managed resource is not a CephUser custom resource"
	errTrackPCUsage               = "cannot track ProviderConfig usage"
	errCacheInit                  = "cannot init CephUser cache"
	errGetPC                      = "cannot get ProviderConfig"
	errListPC                     = "cannot list ProviderConfigs"
	errGetCephUser                = "cannot get CephUser"
	errListCephUsers              = "cannot list CephUsers"
	errCreateCephUser             = "cannot create CephUser"
	errDeleteCephUser             = "cannot delete CephUser"
	errUpdateCephUser             = "cannot update CephUser"
	errListObjects                = "cannot list objects"
	errDeleteObject               = "cannot delete object"
	errGetCreds                   = "cannot get credentials"
	errBackendNotStored           = "s3 backend is not stored"
	errBackendInactive            = "s3 backend is inactive"
	errNoS3BackendsStored         = "no s3 backends stored"
	errNoS3BackendsRegistered     = "no s3 backends registered"
	errMissingS3Backend           = "missing s3 backends"
	errCodeCephUserNotFound       = "NotFound"
	errFailedToCreateClient       = "failed to create s3 client"
	errCephUserCreationInProgress = "cephUser creation in progress"

	inUseFinalizer = "cephUser-in-use.s3.crossplane.io"
)

var cephUserCache *bigcache.BigCache

func init() {
	var err error

	cephUserCache, err = bigcache.New(context.Background(), bigcache.DefaultConfig(time.Hour))
	kingpin.FatalIfError(err, "Cannot init cephUser cache")
}

// A NoOpService does nothing.
type NoOpService struct{}

var (
	newNoOpService = func(_ []byte) (interface{}, error) { return &NoOpService{}, nil }
)

// Setup adds a controller that reconciles CephUser managed resources.
func Setup(mgr ctrl.Manager, o controller.Options, s *radosgwbackendstore.BackendStore) error {
	name := managed.ControllerName(v1alpha1.CephUserGroupKind)

	cps := []managed.ConnectionPublisher{managed.NewAPISecretPublisher(mgr.GetClient(), mgr.GetScheme())}
	//if o.Features.Enabled(features.EnableAlphaExternalSecretStores) {
	//	cps = append(cps, connection.NewDetailsManager(mgr.GetClient(), apisv1alpha1.StoreConfigGroupVersionKind))
	//}

	opts := []managed.ReconcilerOption{
		managed.WithExternalConnecter(&connector{
			kube:         mgr.GetClient(),
			usage:        resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newServiceFn: newNoOpService,
			backendStore: s,
			log:          o.Logger.WithValues("controller", name),
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
		managed.WithConnectionPublishers(cps...),
	}

	if o.Features.Enabled(features.EnableAlphaManagementPolicies) {
		opts = append(opts, managed.WithManagementPolicies())
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.CephUserGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&v1alpha1.CephUser{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// A connector is expected to produce an ExternalClient when its Connect method
// is called.
type connector struct {
	kube         client.Client
	usage        resource.Tracker
	newServiceFn func(creds []byte) (interface{}, error)
	backendStore *radosgwbackendstore.BackendStore
	log          logging.Logger
}

// Connect typically produces an ExternalClient by:
// 1. Tracking that the managed resource is using a ProviderConfig.
// 2. Getting the managed resource's ProviderConfig.
// 3. Getting the credentials specified by the ProviderConfig.
// 4. Using the credentials to form a client.
func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	return &external{
			kubeClient:   c.kube,
			backendStore: c.backendStore,
			log:          c.log},
		nil
}

// An ExternalClient observes, then either creates, updates, or deletes an
// external resource to ensure it reflects the managed resource's desired state.
type external struct {
	kubeClient   client.Client
	backendStore *radosgwbackendstore.BackendStore
	log          logging.Logger
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cephUser, ok := mg.(*v1alpha1.CephUser)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotCephUser)
	}

	if !c.backendStore.BackendsAreStored() {
		return managed.ExternalObservation{}, errors.New(errNoS3BackendsStored)
	}

	type cephUserExistsResult struct {
		cephUserExists bool
		err            error
	}

	cephUserExistsResults := make(chan cephUserExistsResult)

	// Create a new context and cancel it when we have either found the cephUser
	// somewhere or cannot find it anywhere.
	ctxC, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(cephUser.Spec.Providers) == 0 {
		cephUser.Spec.Providers = c.backendStore.GetAllActiveBackendNames()
	}

	// Check for the cephUser on each backend in a separate go routine
	allBackendClients := c.backendStore.GetBackendClients(cephUser.Spec.Providers)
	for _, backendClient := range allBackendClients {
		go func(backendClient *radosgw_admin.API, uid string) {
			cephUserExists, err := radosgw.CephUserExists(ctxC, backendClient, *cephUser.Spec.ForProvider.UID)
			cephUserExistsResults <- cephUserExistsResult{cephUserExists, err}
		}(backendClient, cephUser.Name)
	}

	// Wait for any go routine to finish, if the cephUser exists anywhere
	// return 'ResourceExists: true' as resulting calls to Create or Delete
	// are idempotent.
	for i := 0; i < len(allBackendClients); i++ {
		result := <-cephUserExistsResults
		if result.err != nil {
			c.log.Info(errors.Wrap(result.err, errGetCephUser).Error())

			continue
		}

		if result.cephUserExists {
			return managed.ExternalObservation{
				// Return false when the external resource does not exist. This lets
				// the managed resource reconciler know that it needs to call Create to
				// (re)create the resource, or that it has successfully been deleted.
				ResourceExists: true,

				// Return false when the external resource exists, but it not up to date
				// with the desired managed resource state. This lets the managed
				// resource reconciler know that it needs to call Update.
				ResourceUpToDate: false,

				// Return any details that may be required to connect to the external
				// resource. These will be stored as the connection secret.
				ConnectionDetails: managed.ConnectionDetails{},
			}, nil
		}
	}

	// cephUser not found anywhere.
	return managed.ExternalObservation{
		// Return false when the external resource does not exist. This lets
		// the managed resource reconciler know that it needs to call Create to
		// (re)create the resource, or that it has successfully been deleted.
		// If the cephUser's Disabled flag has been set, no further action is needed.
		ResourceExists: cephUser.Spec.Disabled,
	}, nil
}

//nolint:maintidx,gocognit,gocyclo,cyclop,nolintlint // Function requires numerous checks.
func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	cephUser, ok := mg.(*v1alpha1.CephUser)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotCephUser)
	}

	if cephUser.Spec.Disabled {
		c.log.Info("CephUser is disabled - no cephUsers to be created on backends", "cephUser name", cephUser.Name)

		return managed.ExternalCreation{}, nil
	}

	if !c.backendStore.BackendsAreStored() {
		return managed.ExternalCreation{}, errors.New(errNoS3BackendsStored)
	}

	// This solution expects we have one leader of the controllers.
	if err := cephUserCache.Set(string(cephUser.UID), []byte(cephUser.ObjectMeta.ResourceVersion)); err != nil {
		return managed.ExternalCreation{}, err
	}

	if len(cephUser.Spec.Providers) == 0 {
		cephUser.Spec.Providers = c.backendStore.GetAllActiveBackendNames()
	}

	// Create the cephUser on each backend in a separate go routine
	activeBackends := c.backendStore.GetActiveBackends(cephUser.Spec.Providers)
	if len(activeBackends) == 0 {
		return managed.ExternalCreation{}, errors.New(errNoS3BackendsRegistered)
	} else if len(activeBackends) != len(cephUser.Spec.Providers) {
		return managed.ExternalCreation{}, errors.New(errMissingS3Backend)
	}

	cephUser.Status.SetConditions(xpv1.Creating())

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	errorsLeft := 0
	errChan := make(chan error, len(activeBackends))

	for beName := range activeBackends {
		originalCephUser := cephUser.DeepCopy()

		cl := c.backendStore.GetBackendClient(beName)
		if cl == nil {
			c.log.Info("Backend client not found for backend - cephUser cannot be created on backend", "cephUser name", originalCephUser.Name, "backend name", beName)

			continue
		}

		c.log.Info("Creating cephUser", "cephUser name", originalCephUser.Name, "backend name", beName)

		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: beName}, pc); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, errGetPC)
		}

		//if utils.IsHealthCheckCephUser(cephUser) && pc.Spec.DisableHealthCheck {
		//	c.log.Info("Health check is disabled on backend - health-check-cephUser will not be created", "backend name", beName)
		//
		//	continue
		//}

		wg.Add(1)
		errorsLeft++

		beName := beName
		go func() {
			defer wg.Done()

			if status, ok := originalCephUser.Status.AtProvider.BackendStatuses[beName]; ok && status == v1alpha1.BackendReadyStatus {
				c.log.Info("CephUser already exists on backend", "cephUser name", originalCephUser.Name, "backend name", beName)

				errChan <- nil

				return
			}

			var err error
			requestRetries := 5
			user := radosgw.GenerateCephUserInput(cephUser)
			for i := 0; i < requestRetries; i++ {
				_, err = cl.CreateUser(ctx, *user)
				if resource.Ignore(isAlreadyExists, err) == nil {
					break
				}
			}

			if err != nil {
				c.log.Info("Failed to create cephUser on backend", "backend name", beName, "cephUser_name", originalCephUser.Name, "error", err.Error())

				errChan <- err

				return
			}

			lock.Lock()
			defer lock.Unlock()

			latestVersion, err := cephUserCache.Get(string(originalCephUser.UID))
			if err != nil && !errors.Is(err, bigcache.ErrEntryNotFound) {
				c.log.Info("Failed to get cephUser from cache", "backend name", beName, "cephUser_name", originalCephUser.Name)

				errChan <- err

				return
			}

			cephUserToUpdate := originalCephUser
			if latestVersion == nil || cephUserToUpdate.ObjectMeta.ResourceVersion != string(latestVersion) {
				c.log.Info("CephUser version is obsolete", "cephUser_name", originalCephUser.Name)

				cephUserToUpdate = &v1alpha1.CephUser{}

				if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: cephUserToUpdate.Name}, cephUserToUpdate); err != nil {
					c.log.Info("Failed to fetch latest cephUser", "backend name", beName, "cephUser_name", cephUserToUpdate.Name)

					errChan <- err

					return
				}
			}

			cephUserToUpdate.Status.SetConditions(xpv1.Available())

			if cephUserToUpdate.Status.AtProvider.BackendStatuses == nil {
				cephUserToUpdate.Status.AtProvider.BackendStatuses = v1alpha1.BackendStatuses{}
			}
			cephUserToUpdate.Status.AtProvider.BackendStatuses[beName] = v1alpha1.BackendReadyStatus

			if err := c.kubeClient.Status().Update(ctx, cephUserToUpdate); err != nil {
				c.log.Info("Failed to update cephUser", "backend name", beName, "cephUser_name", cephUserToUpdate.Name)

				errChan <- err

				return
			}

			if err := cephUserCache.Set(string(originalCephUser.UID), []byte(cephUserToUpdate.ObjectMeta.ResourceVersion)); err != nil {
				c.log.Info("Failed to set cephUser in cache", "backend name", beName, "cephUser_name", originalCephUser.Name)

				errChan <- err

				return
			}

			errChan <- nil
		}()
	}

	if errorsLeft == 0 {
		c.log.Info("Failed to find any backend for cephUser", "cephUser_name", cephUser.Name)

		if err := cephUserCache.Delete(string(cephUser.UID)); err != nil && !errors.Is(err, bigcache.ErrEntryNotFound) {
			c.log.Info("Failed to delete cephUser from cache", "cephUser_name", cephUser.Name)

			return managed.ExternalCreation{}, err
		}

		return managed.ExternalCreation{}, nil
	}

	return c.waitForCreation(ctx, cephUser, errChan, errorsLeft, &wg)
}

func (c *external) waitForCreation(ctx context.Context, cephUser *v1alpha1.CephUser, errChan chan error, errorsLeft int, wg *sync.WaitGroup) (managed.ExternalCreation, error) {
	var err error

WAIT:
	for {
		select {
		case <-ctx.Done():
			c.log.Info("Context timeout", "cephUser_name", cephUser.Name)

			return managed.ExternalCreation{}, ctx.Err()
		case err = <-errChan:
			errorsLeft--

			if err != nil {
				c.log.Info("Failed to create on backend", "cephUser_name", cephUser.Name)

				if errorsLeft > 0 {
					continue
				}

				break WAIT
			}

			go func() {
				wg.Wait()

				if err := cephUserCache.Delete(string(cephUser.UID)); err != nil && !errors.Is(err, bigcache.ErrEntryNotFound) {
					c.log.Info("Failed to delete cephUser from cache", "cephUser_name", cephUser.Name)
				}
			}()

			return managed.ExternalCreation{}, nil
		}
	}

	return managed.ExternalCreation{}, err
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cephUser, ok := mg.(*v1alpha1.CephUser)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotCephUser)
	}

	latestVersion, err := cephUserCache.Get(string(cephUser.UID))
	if latestVersion != nil || !errors.Is(err, bigcache.ErrEntryNotFound) {
		c.log.Info("CephUser creation in progress", "cephUser_name", cephUser.Name, "error", err)

		return managed.ExternalUpdate{}, errors.New(errCephUserCreationInProgress)
	}

	//if utils.IsHealthCheckCephUser(cephUser) {
	//	c.log.Info("Update is NOOP for health check cephUser - updates performed by heath-check-controller", "cephUser", cephUser.Name)
	//
	//	return managed.ExternalUpdate{}, nil
	//}

	if cephUser.Spec.Disabled {
		c.log.Info("CephUser is disabled - remove any existing cephUsers from backends", "cephUser name", cephUser.Name)

		return managed.ExternalUpdate{}, c.Delete(ctx, mg)
	}

	if err := c.updateAll(ctx, cephUser); err != nil {
		return managed.ExternalUpdate{}, err
	}

	cephUser.Status.SetConditions(xpv1.Available())

	return managed.ExternalUpdate{
		// Optionally return any details that may be required to connect to the
		// external resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (c *external) updateAll(ctx context.Context, cephUser *v1alpha1.CephUser) error {
	cephUserBackends := newCephUserBackends()
	defer c.setCephUserStatus(cephUser, cephUserBackends)

	g := new(errgroup.Group)

	activeBackends := c.backendStore.GetActiveBackends(cephUser.Spec.Providers)
	if len(activeBackends) == 0 {
		return errors.New(errNoS3BackendsRegistered)
	} else if len(activeBackends) != len(cephUser.Spec.Providers) {
		return errors.New(errMissingS3Backend)
	}

	for backendName := range activeBackends {
		if !c.backendStore.IsBackendActive(backendName) {
			c.log.Info("Backend is marked inactive - cephUser will not be updated on backend", "cephUser name", cephUser.Name, "backend name", backendName)

			continue
		}

		cl := c.backendStore.GetBackendClient(backendName)
		if cl == nil {
			c.log.Info("Backend client not found for backend - cephUser cannot be updated on backend", "cephUser name", cephUser.Name, "backend name", backendName)

			continue
		}

		c.log.Info("Updating cephUser", "cephUser name", cephUser.Name, "backend name", backendName)

		beName := backendName
		g.Go(func() error {
			cephUserBackends.setCephUserBackendStatus(cephUser.Name, beName, v1alpha1.BackendNotReadyStatus)
			requestRetries := 5
			for i := 0; i < requestRetries; i++ {
				cephUserExists, err := radosgw.CephUserExists(ctx, cl, cephUser.Name)
				if err != nil {
					return err
				}
				if !cephUserExists {
					cephUserBackends.deleteCephUserBackend(cephUser.Name, beName)

					return nil
				}

				cephUserBackends.setCephUserBackendStatus(cephUser.Name, beName, v1alpha1.BackendNotReadyStatus)

				err = c.update(ctx, cephUser, cl)
				if err == nil {
					// Check to see if this backend has been marked as 'Unhealthy'. It may be 'Unknown' due to
					// the healthcheck being disabled. In which case we can only assume the backend is healthy
					// and mark the cephUser as 'Ready' for this backend.
					if c.backendStore.GetBackendHealthStatus(beName) == apisv1alpha1.HealthStatusUnhealthy {
						break
					}

					cephUserBackends.setCephUserBackendStatus(cephUser.Name, beName, v1alpha1.BackendReadyStatus)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, errUpdateCephUser)
	}

	return nil
}
func (c *external) update(ctx context.Context, cephUser *v1alpha1.CephUser, radosgwclient *radosgw_admin.API) error {
	// TODO implement ownership check and any other required ones

	if controllerutil.AddFinalizer(cephUser, inUseFinalizer) {
		// we need to update the object to add the finalizer otherwise it is only added
		// to the object's managed fields and does not block deletion.
		return c.kubeClient.Update(ctx, cephUser)
	}

	return nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cephUser, ok := mg.(*v1alpha1.CephUser)
	if !ok {
		return errors.New(errNotCephUser)
	}

	//	if utils.IsHealthCheckCephUser(cephUser) {
	//		c.log.Info("Delete is NOOP for health check cephUser as it is owned by, and garbage collected on deletion of its related providerconfig", "cephUser", cephUser.Name)
	//
	//		return nil
	//	}
	//
	// There are two scenarios where the cephUser status needs to be updated during a
	// Delete invocation:
	// 1. The caller attempts to delete the CR and an error occurs during the call to
	// the cephUser's backends. In this case the cephUser may be successfully deleted
	// from some backends, but not from others. As such, we must update the cephUser CR
	// status accordingly as Delete has ultimately failed and the 'in-use' finalizer
	// will not be removed.
	// 2. The caller attempts to delete the cephUser from it's backends without deleting
	// the cephUser CR. This is done by setting the Disabled flag on the cephUser
	// CR spec. If the deletion is successful or unsuccessful, the cephUser CR status must be
	// updated.
	cephUserBackends := newCephUserBackends()
	defer c.setCephUserStatus(cephUser, cephUserBackends)

	if !c.backendStore.BackendsAreStored() {
		return errors.New(errNoS3BackendsStored)
	}

	cephUser.Status.SetConditions(xpv1.Deleting())

	g := new(errgroup.Group)

	activeBackends := cephUser.Spec.Providers
	if len(activeBackends) == 0 {
		activeBackends = c.backendStore.GetAllActiveBackendNames()
	}

	for _, backendName := range activeBackends {
		cephUserBackends.setCephUserBackendStatus(cephUser.Name, backendName, v1alpha1.BackendDeletingStatus)

		c.log.Info("Deleting cephUser", "cephUser name", cephUser.Name, "backend name", backendName)
		cl := c.backendStore.GetBackendClient(backendName)
		beName := backendName
		g.Go(func() error {
			var err error
			for i := 0; i < s3internal.RequestRetries; i++ {
				user := radosgw.GenerateCephUserInput(cephUser)
				if err := cl.RemoveUser(ctx, *user); err != nil {
					break
				}

				//TODO should we manage backend deletion?
				cephUserBackends.deleteCephUserBackend(cephUser.Name, beName)
			}

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, errDeleteCephUser)
	}

	// update object to remove in-use finalizer and allow deletion
	if controllerutil.RemoveFinalizer(cephUser, inUseFinalizer) {
		// we need to update the object to add the finalizer otherwise it is only added
		// to the object's managed fields and does not block deletion.
		return c.kubeClient.Update(ctx, cephUser)
	}

	return nil
}

// isAlreadyExists helper function to test for ErrCodeCephUserAlreadyOwnedByYou error
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if strings.HasPrefix(err.Error(), "KeyExists") {
		return true
	}
	return false
}

func (c *external) setCephUserStatus(cephUser *v1alpha1.CephUser, cephUserBackends *cephUserBackends) {
	cephUser.Status.SetConditions(xpv1.Unavailable())
	cephUserBackendStatuses := cephUserBackends.getCephUserBackendStatuses(cephUser.Name, cephUser.Spec.Providers)
	cephUser.Status.AtProvider.BackendStatuses = cephUserBackendStatuses
	for _, backendStatus := range cephUserBackendStatuses {
		if backendStatus == v1alpha1.BackendReadyStatus {
			cephUser.Status.SetConditions(xpv1.Available())
			break
		}
	}
}
