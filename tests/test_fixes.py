"""隔离行修复重放：按批次行号修复、乐观锁、失败重试、审计保留。

对应需求：修好一条不用整批重传；修正立即参与依赖分析；原始坏行与当时诊断
原样留审计；并发修同一行拒绝过期修改；重算失败可重试且不重复登记。
"""
from __future__ import annotations

import pytest

from app.modules.m3_diagnostics import fixes
from fixtures import expected as EX


def _batch_id(client) -> str:
    return client.get("/api/batch").json()["batch"]["batch_id"]


def _fix(client, line_no: int, raw_after: str, base_revision: int = 0):
    return client.post("/api/diagnostics/fix",
                       json={"line_no": line_no, "raw_after": raw_after,
                             "base_revision": base_revision, "author": "tester"})


def test_fix_applied_joins_registry_and_graph(store, client_with_run):
    """修复成功：指标立即进注册表与依赖图，四账仍平衡，隔离数减一。"""
    client = client_with_run
    batch_id = _batch_id(client)
    metrics_before = store.load_metrics(batch_id)

    resp = _fix(client, 39, "未知聚合 = SUM(order_amt)")
    assert resp.status_code == 200
    body = resp.json()
    assert body["accepted"] is True
    assert body["outcome"] == "applied"
    assert body["revision"] == 1
    assert body["line"] == {"line_no": 39, "state": "registered", "code": None}

    metrics_after = store.load_metrics(batch_id)
    assert len(metrics_after) == len(metrics_before) + 1
    assert any(m["name"] == "未知聚合" and m["line_no"] == 39
               for m in metrics_after)
    nodes = store.load_nodes(batch_id)
    assert any(n["node_id"] == "metric:未知聚合" for n in nodes)

    accounts = client.get("/api/reconcile").json()
    assert accounts["all_balanced"] is True
    assert accounts["details"]["lines_registered"] == EX.LINES_REGISTERED + 1
    assert accounts["details"]["lines_isolated"] == EX.LINES_ISOLATED - 1
    assert client.get("/api/impact?metric=未知聚合").status_code == 200


def test_fixed_metric_immediately_joins_dependency_analysis(store, client_with_run):
    """修出被引用的指标：依赖边与报表解析立即生效，不用整批重传。"""
    client = client_with_run
    batch_id = _batch_id(client)
    reports_before = store.load_reports(batch_id)
    assert sum(1 for r in reports_before if r["resolved"]) == EX.REPORTS_RESOLVED

    resp = _fix(client, 39, "已下线指标 = SUM(order_amt)")
    assert resp.json()["accepted"] is True

    edges = store.load_edges(batch_id)
    edge_triples = {(e["src"], e["dst"], e["kind"]) for e in edges}
    assert ("metric:悬空引用", "metric:已下线指标", "metric_metric") in edge_triples
    reports = store.load_reports(batch_id)
    assert sum(1 for r in reports if r["resolved"]) == EX.REPORTS_RESOLVED + 1
    assert next(r for r in reports if r["report_id"] == "R-10")["resolved"] is True


def test_fix_keeps_original_evidence_and_links(store, client_with_run):
    """原始坏行与当时诊断原样保留；修复记录带前后关联；其他行不动。"""
    client = client_with_run
    batch_id = _batch_id(client)
    diags_before = store.load_diagnostics(batch_id)
    lines_before = store.load_lines(batch_id)

    assert _fix(client, 39, "未知聚合 = SUM(order_amt)").json()["accepted"] is True

    assert store.load_diagnostics(batch_id) == diags_before
    assert store.load_lines(batch_id) == lines_before

    rows = store.load_fixes(batch_id)
    assert len(rows) == 1
    fix = rows[0]
    assert fix["raw_before"] == "未知聚合 = total(order_amt)"
    assert fix["raw_after"] == "未知聚合 = SUM(order_amt)"
    assert fix["state_before"] == "isolated"
    assert fix["state_after"] == "registered"
    assert "Q_AGG_UNKNOWN" in fix["diags_before_json"]
    assert fix["diags_after_json"] == "[]"
    assert fix["author"] == "tester"


def test_stale_fix_rejected_then_rebased(store, client_with_run):
    """并发修同一行：基于过期代数的修改 409，重新基于最新代数可提交。"""
    client = client_with_run
    batch_id = _batch_id(client)
    assert _fix(client, 39, "未知聚合 = SUM(order_amt)").json()["revision"] == 1

    stale = _fix(client, 39, "未知聚合 = AVG(order_amt)", base_revision=0)
    assert stale.status_code == 409
    payload = stale.json()
    assert payload["error"] == "F_STALE_FIX"
    assert payload["detail"]["effective"] == 1
    assert payload["detail"]["submitted"] == 0

    rebased = _fix(client, 39, "未知聚合 = AVG(order_amt)", base_revision=1)
    assert rebased.status_code == 200
    assert rebased.json()["accepted"] is True
    assert rebased.json()["revision"] == 2

    rows = store.load_fixes(batch_id)
    assert [f["revision"] for f in rows] == [1, 2]
    assert rows[1]["raw_before"] == "未知聚合 = SUM(order_amt)"
    assert rows[1]["raw_after"] == "未知聚合 = AVG(order_amt)"
    current = [m for m in store.load_metrics(batch_id) if m["line_no"] == 39]
    assert current[0]["expr_text"] == "AVG(order_amt)"


def test_failed_fix_retry_keeps_single_record(store, client_with_run):
    """重算失败：可重试，同一行只留一条记录，成功前不动注册表。"""
    client = client_with_run
    batch_id = _batch_id(client)
    metrics_before = len(store.load_metrics(batch_id))

    failed = _fix(client, 40, "未知字段 = SUM(nope_amt)")
    assert failed.status_code == 200
    body = failed.json()
    assert body["accepted"] is False
    assert body["outcome"] == "failed"
    assert body["line"]["code"] == "Q_FIELD_UNKNOWN"
    assert len(store.load_metrics(batch_id)) == metrics_before

    ok = _fix(client, 40, "未知字段 = SUM(order_amt)")
    assert ok.json()["accepted"] is True
    assert ok.json()["revision"] == 1

    rows = store.load_fixes(batch_id)
    assert len(rows) == 1
    assert rows[0]["revision"] == 1
    assert rows[0]["status"] == "applied"
    assert rows[0]["raw_after"] == "未知字段 = SUM(order_amt)"
    assert len(store.load_metrics(batch_id)) == metrics_before + 1


def test_same_payload_retry_is_idempotent(store, client_with_run):
    """重试同一请求：幂等返回已有记录，不重复登记，指标只进一次注册表。"""
    client = client_with_run
    batch_id = _batch_id(client)
    first = _fix(client, 39, "未知聚合 = SUM(order_amt)")
    assert first.json()["accepted"] is True

    second = _fix(client, 39, "未知聚合 = SUM(order_amt)")
    assert second.status_code == 200
    assert second.json()["reused"] is True
    assert second.json()["outcome"] == "reused"

    assert len(store.load_fixes(batch_id)) == 1
    names = [m["name"] for m in store.load_metrics(batch_id)]
    assert names.count("未知聚合") == 1


def test_fix_creating_name_conflict_fails(store, client_with_run):
    """修复后与已有指标同名不同定义：图阶段隔离，修复未通过。"""
    client = client_with_run
    batch_id = _batch_id(client)
    resp = _fix(client, 39, "销售额 = AVG(order_amt)")
    body = resp.json()
    assert body["accepted"] is False
    assert body["line"]["code"] == "C_DUP_NAME_BLOCK"
    assert not any(m["line_no"] == 39 for m in store.load_metrics(batch_id))


def test_fix_duplicate_of_existing_line_fails(store, client_with_run):
    """修复后与前面某行完全相同：Q_DUP_LINE 隔离，修复未通过。"""
    client = client_with_run
    resp = _fix(client, 39, "客单价 = AVG(order_amt) group by store_id")
    body = resp.json()
    assert body["accepted"] is False
    assert body["line"]["code"] == "Q_DUP_LINE"


def test_fix_validation_errors(client_with_run):
    """目标行不存在、不是定义行、内容为空、内容多行：分别 404/400。"""
    client = client_with_run
    missing = _fix(client, 999, "x = SUM(order_amt)")
    assert missing.status_code == 404
    assert missing.json()["error"] == "F_LINE_INVALID"

    comment = _fix(client, 1, "x = SUM(order_amt)")
    assert comment.status_code == 400
    assert comment.json()["error"] == "F_LINE_INVALID"

    empty = _fix(client, 39, "   ")
    assert empty.status_code == 400
    assert empty.json()["error"] == "Q_EXPR_EMPTY"

    multi = _fix(client, 39, "a = SUM(order_amt)\nb = SUM(qty)")
    assert multi.status_code == 400
    assert multi.json()["error"] == "F_LINE_INVALID"


def test_fix_listing_and_page(store, client_with_run):
    """诊断页与 API 都能查到修复记录和当前状态。"""
    client = client_with_run
    assert _fix(client, 39, "未知聚合 = SUM(order_amt)").json()["accepted"] is True

    listing = client.get("/api/diagnostics/fixes").json()
    assert listing["count"] == 1
    row = next(r for r in listing["lines"] if r["line_no"] == 39)
    assert row["original_state"] == "isolated"
    assert row["current_state"] == "registered"
    assert row["effective_revision"] == 1
    assert 39 not in [r["line_no"] for r in listing["repairable"]]

    page = client.get("/diagnostics")
    assert page.status_code == 200
    assert "修复记录" in page.text
    assert "未知聚合 = SUM(order_amt)" in page.text
    assert "未知聚合 = total(order_amt)" in page.text


def test_fix_requires_batch(client):
    resp = client.post("/api/diagnostics/fix",
                       json={"line_no": 1, "raw_after": "x = SUM(order_amt)",
                             "base_revision": 0})
    assert resp.status_code == 404
    assert resp.json()["error"] == "E_EMPTY_INPUT"


def test_view_groups_attempts_per_line(store, client_with_run):
    """视图：同一行多次尝试聚成一条当前状态，代数取最新 applied。"""
    client = client_with_run
    batch_id = _batch_id(client)
    _fix(client, 40, "未知字段 = SUM(nope_amt)")
    _fix(client, 40, "未知字段 = SUM(order_amt)")
    view = fixes.view(store, batch_id)
    assert view["count"] == 1
    line = next(r for r in view["lines"] if r["line_no"] == 40)
    assert line["attempts"] == 1
    assert line["current_state"] == "registered"
    assert line["last_status"] == "applied"
