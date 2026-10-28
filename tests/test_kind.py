"""Paging and truncation against a real API server: a local, throwaway kind cluster.

    eval "$(scripts/kind-paging-cluster.sh up | tail -1)"
    uv run --group dev pytest -m kind -v --durations=0
    scripts/kind-paging-cluster.sh down

The tools run unchanged; only srv.auth's clients are swapped for ones built from the kind
kubeconfig, so no AWS credentials or EKS clusters are involved. etcd compacts every 10 s on
that cluster, which is what lets a continue token actually expire. Skipped when
EKS_MULTI_MCP_KIND_KUBECONFIG is unset.
"""

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from fakes import call, make_server, result, tool_error
from kubernetes import config
from kubernetes.client import ApiClient, Configuration, CoreV1Api
from kubernetes.dynamic import DynamicClient
from mcp.server.mcpserver.exceptions import ToolError

KCFG = os.environ.get("EKS_MULTI_MCP_KIND_KUBECONFIG")

pytestmark = [pytest.mark.kind,
              pytest.mark.skipif(not KCFG, reason="set EKS_MULTI_MCP_KIND_KUBECONFIG to run kind checks")]

BULK = 1200
ROOT_CA = "kube-root-ca.crt"  # the API server adds this ConfigMap to every namespace


@pytest.fixture(scope="module")
def api():
    c = Configuration()  # load into an explicit Configuration; a default ApiClient() does not pick it up
    config.load_kube_config(config_file=KCFG, client_configuration=c)
    return ApiClient(configuration=c)


def _namespace(api, seed: int = 0) -> str:
    ns = f"paging-{uuid.uuid4().hex[:8]}"
    dyn = DynamicClient(api)
    dyn.resources.get(api_version="v1", kind="Namespace").create(body={"metadata": {"name": ns}})
    cm = dyn.resources.get(api_version="v1", kind="ConfigMap")
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda i: cm.create(namespace=ns, body={"metadata": {"name": f"c{i:04d}"}, "data": {"v": "0"}}),
                      range(seed)))
    return ns


def _drop(api, ns: str) -> None:
    DynamicClient(api).resources.get(api_version="v1", kind="Namespace").delete(name=ns)


@pytest.fixture(scope="module")
def bulk(api):
    ns = _namespace(api, BULK)
    yield ns
    _drop(api, ns)


@pytest.fixture
def fresh(api):
    created: list[str] = []

    def make(seed: int = 0) -> str:
        created.append(_namespace(api, seed))
        return created[-1]

    yield make
    for ns in created:
        _drop(api, ns)


@pytest.fixture
def srv(files, monkeypatch, api):
    srv = make_server(files)
    monkeypatch.setattr(srv.auth, "dynamic", lambda t, mode="read": DynamicClient(api))
    monkeypatch.setattr(srv.auth, "api_client", lambda t, mode="read": api)
    return srv


def _list(srv, ns, **kw):
    return result(srv, "list_k8s_resources", target="dev", kind="ConfigMap", namespace=ns, **kw)


def test_kd1_pages_a_real_listing_to_completion(srv, bulk):
    pages, token = [], None
    for _ in range(10):
        out = _list(srv, bulk, limit=500, **({"continue_token": token} if token else {}))
        pages.append(out)
        token = out["continue_token"]
        if not out["truncated"]:
            break
    assert [p["count"] for p in pages] == [500, 500, BULK + 1 - 1000]
    assert all(p["continue_token"] for p in pages[:-1]) and pages[-1]["continue_token"] is None
    names = [r["name"] for p in pages for r in p["result"]]
    assert len(names) == len(set(names))
    assert set(names) == {f"c{i:04d}" for i in range(BULK)} | {ROOT_CA}


def test_kd2_remaining_without_selectors_only(srv, bulk):
    assert _list(srv, bulk, limit=500)["remaining"] == BULK + 1 - 500
    assert _list(srv, bulk, limit=500, label_selector="x!=y")["remaining"] is None


def test_kd3_junk_token_is_rejected_with_the_api_message(srv, bulk):
    msg = tool_error(srv, "list_k8s_resources", target="dev", kind="ConfigMap", namespace=bulk,
                     continue_token="garbage")
    assert "continue_token was rejected" in msg and "invalid continue token" in msg


def test_kd4_expired_token_says_restart(srv, api, fresh):
    ns = fresh(20)
    token = _list(srv, ns, limit=5)["continue_token"]
    cm = DynamicClient(api).resources.get(api_version="v1", kind="ConfigMap")
    deadline = time.monotonic() + 60
    round_ = 0
    while time.monotonic() < deadline:
        round_ += 1
        for i in range(20):
            cm.patch(namespace=ns, name=f"c{i:04d}", body={"data": {"v": str(round_)}},
                     content_type="application/merge-patch+json")
        time.sleep(5)
        try:
            call(srv, "list_k8s_resources", target="dev", kind="ConfigMap", namespace=ns, limit=5,
                 continue_token=token)
        except ToolError as e:
            assert "expired" in str(e) and "restart the listing without continue_token" in str(e)
            return
    pytest.fail("the continue token never expired within 60 s; is etcd-compaction-interval set to 10s?")


def test_kd5_limit_zero_is_not_unlimited(srv, bulk):
    out = _list(srv, bulk, limit=0)
    assert (out["count"], out["truncated"]) == (1, True)


def test_kd6_events_report_total_and_truncation(srv, api, fresh):
    ns = fresh(1)
    core = CoreV1Api(api)
    now = datetime.now(UTC)

    def event(i):
        core.create_namespaced_event(ns, {
            "metadata": {"name": f"e{i:03d}"}, "type": "Normal", "reason": "Probe", "message": f"m{i}",
            "involvedObject": {"kind": "ConfigMap", "name": "c0000", "namespace": ns},
            "firstTimestamp": now.isoformat(), "lastTimestamp": now.isoformat(), "count": 1})

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(event, range(150)))
    out = result(srv, "get_k8s_events", target="dev", namespace=ns, limit=100)
    assert (len(out["result"]), out["total"], out["truncated"]) == (100, 150, True)
