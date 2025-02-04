#!/bin/bash

: "${TEST_KIND_NODES?= required}"
: "${REPO?= required}"

# This script reads a comma-delimited string TEST_KIND_NODES of kind node versions
# for kuttl tests to be run on, and generates the relevant files for each version.

IFS=', ' read -r -a kind_nodes <<< "$TEST_KIND_NODES"


# remove existing files
rm -f ./e2e/kind/*
rm -rf ./e2e/kuttl/*

HEADER="# This file was auto-generated by hack/generate-tests.sh"

for kind_node in "${kind_nodes[@]}"
do
	# write kind config file for version
	major=${kind_node%.*}
	if [ ! -d "./e2e/kind" ]; then
		mkdir -p ./e2e/kind
	fi
	file=./e2e/kind/kind-config-${major}.yaml

	cat <<EOF > "${file}"
${HEADER}
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
EOF
	# write kuttl config file for version
	if [ ! -d "./e2e/kuttl/ceph" ]; then
		mkdir -p ./e2e/kuttl/ceph
    fi

    if [ ! -d "./e2e/kuttl/stable" ]; then
		mkdir -p ./e2e/kuttl/stable
	fi
	file=./e2e/kuttl/stable/${REPO}-${major}.yaml
    file_ceph=./e2e/kuttl/ceph/${REPO}-${major}.yaml

	# tests use 'stable' testDir in CI
	cat <<EOF > "${file}"
${HEADER}
apiVersion: kuttl.dev/v1beta1
kind: TestSuite
testDirs:
- ./e2e/tests/stable
kindConfig: e2e/kind/kind-config-${major}.yaml
startKIND: false
timeout: 90
EOF

	# tests use 'ceph' testDir for manual tests
	cat <<EOF > "${file_ceph}"
${HEADER}
apiVersion: kuttl.dev/v1beta1
kind: TestSuite
testDirs:
- ./e2e/tests/ceph
kindConfig: e2e/kind/kind-config-${major}.yaml
startKIND: false
timeout: 90
EOF

file=./.github/workflows/kuttl-e2e-test-${major}.yaml

	cat <<EOF > "${file}"
${HEADER}
name: kuttl e2e test ${major}
on: [push]
jobs:
  test:
    name: kuttl e2e test ${major}
    runs-on: ubuntu-latest
    env:
      KUTTL: /usr/local/bin/kubectl-kuttl
      COMPOSE: /usr/local/bin/docker-compose
    steps:
      - name: Cancel Previous Runs
        uses: styfle/cancel-workflow-action@0.9.1
        with:
          access_token: \${{ github.token }}
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: '1.20'
      - name: Install dependencies
        run: |
          sudo curl -Lo \$KUTTL https://github.com/kudobuilder/kuttl/releases/download/v0.13.0/kubectl-kuttl_0.13.0_linux_x86_64
          sudo chmod +x \$KUTTL
          sudo curl -Lo \$COMPOSE https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64
          sudo chmod +x \$COMPOSE
      - name: Build
        run: make submodules build
      - name: Create test environment
        run: make crossplane-cluster load-package
      - name: Run kuttl tests ${major}
        run: kubectl-kuttl test --config e2e/kuttl/stable/${REPO}-${major}.yaml
        env:
          AWS_ACCESS_KEY_ID: 'Dummy'
          AWS_SECRET_ACCESS_KEY: 'Dummy'
          AWS_DEFAULT_REGION: 'us-east-1'
      - uses: actions/upload-artifact@v3
        if: \${{ always() }}
        with:
          name: kind-logs
          path: kind-logs-*
EOF

done
