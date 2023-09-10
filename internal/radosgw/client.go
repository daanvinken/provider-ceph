package radosgw

import (
	"context"
	"fmt"
	radosgw_admin "github.com/ceph/go-ceph/rgw/admin"
	"net/http"
	"strings"

	apisv1alpha1 "github.com/linode/provider-ceph/apis/v1alpha1"
)

const (
	accessKey = "access_key"
	secretKey = "secret_key"
)

func NewClient(ctx context.Context, data map[string][]byte, pcSpec *apisv1alpha1.ProviderConfigSpec) (*radosgw_admin.API, error) {
	hostBase := resolveHostBase(pcSpec.HostBase, pcSpec.UseHTTPS)

	httpClient := &http.Client{}
	radosgwClient, err := radosgw_admin.New(hostBase, string(data[accessKey]), string(data[secretKey]), httpClient)
	if err != nil {
		return nil, err
	}
	_, err = radosgwClient.GetUsers(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error listing users, the configured user requires admin rights: %v", err)
	}
	return radosgwClient, nil
}

func resolveHostBase(hostBase string, useHTTPS bool) string {
	httpsPrefix := "https://"
	httpPrefix := "http://"
	// Remove prefix in either case if it has been specified.
	// Let useHTTPS option take precedence.
	hostBase = strings.TrimPrefix(hostBase, httpPrefix)
	hostBase = strings.TrimPrefix(hostBase, httpsPrefix)

	if useHTTPS {
		return httpsPrefix + hostBase
	}

	return httpPrefix + hostBase
}
