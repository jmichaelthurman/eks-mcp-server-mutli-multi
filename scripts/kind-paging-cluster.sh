#!/usr/bin/env bash
# Throwaway kind cluster for tests/test_kind.py. etcd compacts every 10 s so continue tokens expire.
#   scripts/kind-paging-cluster.sh up   -> prints the kubeconfig path to export
#   scripts/kind-paging-cluster.sh down
set -euo pipefail
NAME=eks-multi-paging
KCFG="${TMPDIR:-/tmp}"; KCFG="${KCFG%/}/$NAME.kubeconfig"
case "${1:-up}" in
  up)
    KUBECONFIG="$KCFG" kind create cluster --name "$NAME" --wait 120s --config - <<'KIND'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
        etcd-compaction-interval: "10s"
KIND
    echo "export EKS_MULTI_MCP_KIND_KUBECONFIG=$KCFG" ;;
  down) kind delete cluster --name "$NAME"; rm -f "$KCFG" ;;
esac
