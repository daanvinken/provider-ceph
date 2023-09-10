package radosgw

import (
	"context"
	"fmt"
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/linode/provider-ceph/apis/radosgw/v1alpha1"
	"log"
	"strings"
)

const (
	errListObjects  = "cannot list objects"
	errDeleteObject = "cannot delete object"

	RequestRetries = 5
)

func GenerateCephUserInput(cephUser *v1alpha1.CephUser) *radosgw_admin.User {
	//quotaEnable := true
	//userQuotaSpec := radosgw_admin.QuotaSpec{
	//	QuotaType:  "user",
	//	UID:        *cephUser.Spec.ForProvider.UID,
	//	MaxSizeKb:  cephUser.Spec.ForProvider.UserQuotaMaxSizeKB,
	//	MaxObjects: cephUser.Spec.ForProvider.UserQuotaMaxObjects,
	//	Enabled:    &quotaEnable,
	//}

	createCephUserInput := &radosgw_admin.User{
		ID: *cephUser.Spec.ForProvider.UID,
		//MaxBuckets:  cephUser.Spec.ForProvider.UserQuotaMaxBuckets,
		//UserQuota:   userQuotaSpec,
		DisplayName: *cephUser.Spec.ForProvider.DisplayedName,
		//Keys:        []radosgw_admin.UserKeySpec{{AccessKey: "test", SecretKey: "test"}},
		// TODO fill all parameters and generate secrets
	}

	return createCephUserInput
}

func DeleteCephUser(ctx context.Context, client *radosgw_admin.API, cephUser v1alpha1.CephUser) error {
	cephUserExists, err := CephUserExists(ctx, client, *cephUser.Spec.ForProvider.UID)
	if err != nil {
		return err
	}
	if !cephUserExists {
		return nil
	}

	// Check if the user has any buckets
	buckets, err := client.ListUsersBucketsWithStat(ctx, *cephUser.Spec.ForProvider.UID)
	if err != nil {
		log.Fatalf("Error listing buckets for user %s: %v", *cephUser.Spec.ForProvider.UID, err)
	}

	// TODO can we heck if each bucket has objects
	//noObjects := true
	//for _, bucket := range buckets {
	//	objects, err := client.GetBucketInfo(context.TODO(), bucket)
	//	if err != nil {
	//		log.Fatalf("Error listing objects in bucket %s: %v", bucket.ID, err)
	//	}
	//}

	// If no objects were found, delete the user
	//if noObjects {
	//	fmt.Printf("User %s has no objects, deleting...\n", user)
	//	err := adminClient.DeleteUser(context.TODO(), user)
	//	if err != nil {
	//		log.Fatalf("Error deleting user %s: %v", user, err)
	//	}
	//	fmt.Printf("User %s deleted successfully.\n", user)
	//}
	if len(buckets) > 0 {
		err = client.RemoveUser(ctx, *GenerateCephUserInput(&cephUser))
		if err != nil {
			log.Fatalf("Error deleting user %s: %v", *cephUser.Spec.ForProvider.UID, err)
		}
	}

	return fmt.Errorf("User still has buckets, please clean up first.")

}

func CephUserExists(ctx context.Context, radosgwclient *radosgw_admin.API, UID string) (bool, error) {
	_, err := radosgwclient.GetUser(ctx, radosgw_admin.User{ID: UID})
	if err != nil {
		return false, resource.Ignore(isNotFound, err)
	}
	return true, nil
}

// isNotFound helper function to test for NotFound error
func isNotFound(err error) bool {
	if strings.HasPrefix(err.Error(), "NoSuchUser") {
		return true
	}

	return false
}
