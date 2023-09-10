package radosgw

import (
	"context"
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/linode/provider-ceph/apis/radosgw/v1alpha1"
	"strings"
)

const (
	errListObjects  = "cannot list objects"
	errDeleteObject = "cannot delete object"

	RequestRetries = 5
)

func GenerateCephUserInput(cephUser *v1alpha1.CephUser) *radosgw_admin.User {
	createCephUserInput := &radosgw_admin.User{
		ID: *cephUser.Spec.ForProvider.UID,
		//MaxBuckets: *cephUser.Spec.ForProvider.UserQuotaMaxObjects,
		//UserQuota:  *cephUser.Spec.ForProvider.UserQuotaMaxSize,
		DisplayName: *cephUser.Spec.ForProvider.DisplayedName,
		//Keys:        []radosgw_admin.UserKeySpec{{AccessKey: "test", SecretKey: "test"}},
		// TODO fill all parameters and generate secrets
	}

	return createCephUserInput
}

//func CephUserToPutCephUserACLInput(cephUser *v1alpha1.CephUser) *s3.PutCephUserAclInput {
//	return &s3.PutCephUserAclInput{
//		ACL:              s3types.CephUserCannedACL(aws.ToString(cephUser.Spec.ForProvider.ACL)),
//		CephUser:         aws.String(cephUser.Name),
//		GrantFullControl: cephUser.Spec.ForProvider.GrantFullControl,
//		GrantRead:        cephUser.Spec.ForProvider.GrantRead,
//		GrantReadACP:     cephUser.Spec.ForProvider.GrantReadACP,
//		GrantWrite:       cephUser.Spec.ForProvider.GrantWrite,
//		GrantWriteACP:    cephUser.Spec.ForProvider.GrantWriteACP,
//	}
//}
//
//func CephUserToPutCephUserOwnershipControlsInput(cephUser *v1alpha1.CephUser) *s3.PutCephUserOwnershipControlsInput {
//	return &s3.PutCephUserOwnershipControlsInput{
//		CephUser: aws.String(cephUser.Name),
//		OwnershipControls: &s3types.OwnershipControls{
//			Rules: []s3types.OwnershipControlsRule{
//				{
//					ObjectOwnership: s3types.ObjectOwnership(aws.ToString(cephUser.Spec.ForProvider.ObjectOwnership)),
//				},
//			},
//		},
//	}
//}

//func DeleteCephUser(ctx context.Context, s3Backend *s3.Client, cephUserName *string) error {
//	cephUserExists, err := CephUserExists(ctx, s3Backend, *cephUserName)
//	if err != nil {
//		return err
//	}
//	if !cephUserExists {
//		return nil
//	}
//
//	g := new(errgroup.Group)
//
//	// Delete all objects from the cephUser. This is sufficient for unversioned cephUsers.
//	g.Go(func() error {
//		return deleteCephUserObjects(ctx, s3Backend, cephUserName)
//	})
//
//	// Delete all object versions (required for versioned cephUsers).
//	g.Go(func() error {
//		return deleteCephUserObjectVersions(ctx, s3Backend, cephUserName)
//	})
//
//	if err := g.Wait(); err != nil {
//		return err
//	}
//
//	_, err = s3Backend.DeleteCephUser(ctx, &s3.DeleteCephUserInput{CephUser: cephUserName})
//
//	return resource.Ignore(isNotFound, err)
//}

//func deleteCephUserObjects(ctx context.Context, s3Backend *s3.Client, cephUserName *string) error {
//	objectsInput := &s3.ListObjectsV2Input{CephUser: cephUserName}
//	for {
//		objects, err := s3Backend.ListObjectsV2(ctx, objectsInput)
//		if err != nil {
//			return errors.Wrap(err, errListObjects)
//		}
//
//		g := new(errgroup.Group)
//		for _, object := range objects.Contents {
//			obj := object
//			g.Go(func() error {
//				return deleteObject(ctx, s3Backend, cephUserName, obj.Key, nil)
//			})
//		}
//
//		if err := g.Wait(); err != nil {
//			return errors.Wrap(err, errDeleteObject)
//		}
//
//		// If the cephUser contains many objects, the ListObjectsV2() call
//		// might not return all of the objects in the first listing. Check to
//		// see whether the listing was truncated. If so, retrieve the next page
//		// of objects and delete them.
//		if !objects.IsTruncated {
//			break
//		}
//
//		objectsInput.ContinuationToken = objects.ContinuationToken
//	}
//
//	return nil
//}

//func deleteCephUserObjectVersions(ctx context.Context, s3Backend *s3.Client, cephUserName *string) error {
//	objVersionsInput := &s3.ListObjectVersionsInput{CephUser: cephUserName}
//	for {
//		objectVersions, err := s3Backend.ListObjectVersions(ctx, objVersionsInput)
//		if err != nil {
//			return errors.Wrap(err, errListObjects)
//		}
//
//		g := new(errgroup.Group)
//		for _, deleteMarkerEntry := range objectVersions.DeleteMarkers {
//			delMark := deleteMarkerEntry
//			g.Go(func() error {
//				return deleteObject(ctx, s3Backend, cephUserName, delMark.Key, delMark.VersionId)
//			})
//		}
//
//		for _, objectVersion := range objectVersions.Versions {
//			objVer := objectVersion
//			g.Go(func() error {
//				return deleteObject(ctx, s3Backend, cephUserName, objVer.Key, objVer.VersionId)
//			})
//		}
//
//		if err := g.Wait(); err != nil {
//			return errors.Wrap(err, errDeleteObject)
//		}
//
//		// If the cephUser contains many objects, the ListObjectVersionsV2() call
//		// might not return all of the objects in the first listing. Check to
//		// see whether the listing was truncated. If so, retrieve the next page
//		// of objects and delete them.
//		if !objectVersions.IsTruncated {
//			break
//		}
//
//		objVersionsInput.VersionIdMarker = objectVersions.NextVersionIdMarker
//		objVersionsInput.KeyMarker = objectVersions.NextKeyMarker
//	}
//
//	return nil
//}

//	func deleteObject(ctx context.Context, s3Backend *s3.Client, cephUser, key, versionId *string) error {
//		var err error
//		for i := 0; i < RequestRetries; i++ {
//			_, err = s3Backend.DeleteObject(ctx, &s3.DeleteObjectInput{
//				CephUser:  cephUser,
//				Key:       key,
//				VersionId: versionId,
//			})
//			if resource.Ignore(isNotFound, err) == nil {
//				return nil
//			}
//		}
//
//		return err
//	}
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
