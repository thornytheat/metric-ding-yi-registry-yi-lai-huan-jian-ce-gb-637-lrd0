"""隔离行修复重放：按原批次与行号修复，立即重算依赖，原始证据原样留审计。

约定：
- 原始 lines/diagnostics 只读（审计证据），每次修复的前后关联落在 fixes 表；
- base_revision 乐观锁：并发修同一行，后到者 409 F_STALE_FIX；
- 重算失败（修复后仍隔离）不消耗代数，同内容重试幂等返回，不重复登记；
- 只有修复成功（进入注册表）才重建当前状态表（metrics/图/冲突/报表/四账），
  未修复的行由确定性重放保持原状。
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from ... import config, errors, reasons
from ..m1_ingest import reader
from ..m4_graph import service as graph_service
from ..m6_impact import service as impact_service
from . import metrics as diag_metrics
from . import rules
from . import service as diag_service

APPLIED = "applied"
FAILED = "failed"


def _now() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------- 入参校验 ----------------
def validate(payload: dict) -> dict:
    try:
        line_no = int(payload.get("line_no"))
    except (TypeError, ValueError) as exc:
        raise errors.BadRequest("line_no 必须是整数", code="F_LINE_INVALID") from exc
    raw_after = payload.get("raw_after")
    if not isinstance(raw_after, str) or not raw_after.strip():
        raise errors.BadRequest("修复内容不能为空", code="Q_EXPR_EMPTY")
    raw_after = raw_after.strip()
    if "\n" in raw_after or "\r" in raw_after:
        raise errors.BadRequest("修复内容必须是一行", code="F_LINE_INVALID")
    try:
        base_revision = int(payload.get("base_revision", 0))
    except (TypeError, ValueError) as exc:
        raise errors.BadRequest("base_revision 必须是整数", code="F_LINE_INVALID") from exc
    if base_revision < 0:
        raise errors.BadRequest("base_revision 不能为负", code="F_LINE_INVALID")
    return {"line_no": line_no, "raw_after": raw_after,
            "base_revision": base_revision,
            "author": (payload.get("author") or "手工修复").strip()}


# ---------------- 重放 ----------------
def current_rows(lines: list[dict], fixes: list[dict]) -> list[reader.RawLine]:
    """当前有效行集：原始行被各行最新一条 applied 修复覆盖。"""
    latest: dict[int, dict] = {}
    for fix in fixes:
        if fix["status"] != APPLIED:
            continue
        seen = latest.get(fix["line_no"])
        if seen is None or fix["revision"] > seen["revision"]:
            latest[fix["line_no"]] = fix
    rows = []
    for row in lines:
        fix = latest.get(row["line_no"])
        rows.append(reader.RawLine(
            line_no=row["line_no"],
            raw=fix["raw_after"] if fix is not None else row["raw"]))
    return rows


def replay(lines: list[dict], fixes: list[dict], reports_text: str) -> dict:
    """对当前有效行集重跑诊断与依赖图；行状态规则与 m1 全量运行一致。"""
    inspect = diag_service.inspect_lines(current_rows(lines, fixes))
    report_rows = graph_service.parse_reports_text(reports_text)
    graph = graph_service.analyse(inspect["metrics"], report_rows)
    diagnostics = list(inspect["diagnostics"]) + list(graph["diagnostics"])
    conflict_lines = sorted({c["other_line"] for c in graph["conflicts"]})
    states = {r["line_no"]: {"state": r["state"], "code": r.get("code")}
              for r in inspect["lines"]}
    for line_no in conflict_lines:
        code = next((d["code"] for d in diagnostics
                     if d.get("line_no") == line_no and d["stage"] == "graph"),
                    "C_DUP_NAME_BLOCK")
        states[line_no] = {"state": "isolated", "code": code}
    return {"inspect": inspect, "graph": graph, "diagnostics": diagnostics,
            "states": states, "conflict_lines": conflict_lines}


def _summary(lines: list[dict]) -> dict:
    return {"total": len(lines),
            "blank": sum(1 for row in lines if row["kind"] == "blank"),
            "comment": sum(1 for row in lines if row["kind"] == "comment"),
            "definition": sum(1 for row in lines if row["kind"] == "definition")}


def _build_fix(data: dict, target: dict, original_diags: list[dict],
               reports_text: str, line_fixes: list[dict],
               all_fixes: list[dict], tx_lines: list[dict]) -> dict:
    """事务内回调：算修复记录；修复成功时附带当前状态表的重放结果。"""
    applied = [f for f in line_fixes if f["status"] == APPLIED]
    before = applied[-1] if applied else None
    raw_before = before["raw_after"] if before else target["raw"]
    state_before = before["state_after"] if before else target["state"]
    if before:
        diags_before = json.loads(before["diags_after_json"])
    else:
        diags_before = [d for d in original_diags
                        if d.get("line_no") == data["line_no"]]
    fix = {"line_no": data["line_no"], "raw_before": raw_before,
           "raw_after": data["raw_after"], "state_before": state_before,
           "diags_before_json": json.dumps(diags_before, ensure_ascii=False),
           "author": data["author"], "created_at": _now()}

    def finish(state_after: str, diags_after: list[dict], status: str,
               code: str | None) -> dict:
        return {"fix": {**fix, "state_after": state_after, "status": status,
                        "code": code,
                        "diags_after_json": json.dumps(diags_after,
                                                       ensure_ascii=False)},
                "replay": None}

    # 单行预判：结构/语法仍错的直接失败，不必全量重放
    single = rules.inspect_line(data["line_no"], data["raw_after"])
    single_diags = single["diagnostics"]
    if any(d["level"] == reasons.ERROR for d in single_diags):
        code = next(d["code"] for d in single_diags
                    if d["level"] == reasons.ERROR)
        return finish("isolated", single_diags, FAILED, code)

    # 全量重放：本行按修复后文本参与，其余行保持当前有效状态
    others = [f for f in all_fixes if f["line_no"] != data["line_no"]]
    trial = {"line_no": data["line_no"], "revision": 1 << 30,
             "raw_after": data["raw_after"], "status": APPLIED}
    result = replay(tx_lines, others + [trial], reports_text)
    state = result["states"].get(data["line_no"],
                                 {"state": "isolated", "code": None})
    line_diags = [d for d in result["diagnostics"]
                  if d.get("line_no") == data["line_no"]]
    if state["state"] != "registered":
        # 重复行、同名冲突等图阶段隔离：修复未通过，当前状态表不动
        return finish("isolated", line_diags, FAILED, state["code"])

    graph = result["graph"]
    isolated = diag_metrics.isolated_line_numbers(
        [d for d in result["diagnostics"] if d["stage"] in {"ingest", "diagnose"}])
    isolated_set = sorted(set(isolated) | set(result["conflict_lines"]))
    summary = _summary(tx_lines)
    accounts = impact_service.accounts(
        summary=summary, metrics=graph["metrics"], nodes=graph["nodes"],
        edges=graph["edges"], components=graph["components"],
        isolated_lines=len(isolated_set), reports=graph["reports"])
    totals = impact_service.totals_payload(accounts)
    totals["lines_total"] = str(summary["total"])
    out = finish("registered", line_diags, APPLIED, None)
    out["replay"] = {"metrics": graph["metrics"], "nodes": graph["nodes"],
                     "edges": graph["edges"], "components": graph["components"],
                     "conflicts": graph["conflicts"], "reports": graph["reports"],
                     "totals": totals}
    return out


# ---------------- 对外入口 ----------------
def apply_fix(settings, store, batch: dict, payload: dict) -> dict:
    data = validate(payload)
    batch_id = batch["batch_id"]
    target = next((row for row in store.load_lines(batch_id)
                   if row["line_no"] == data["line_no"]), None)
    if target is None:
        raise errors.NotFound(f"批次里没有第 {data['line_no']} 行",
                              code="F_LINE_INVALID")
    if target["kind"] != "definition":
        raise errors.BadRequest("只能修复定义行，空行与注释行无需修复",
                                code="F_LINE_INVALID")
    original_diags = store.load_diagnostics(batch_id)
    reports_text = Path(settings.sample_dir, "report_usage.csv").read_text(
        encoding="utf-8-sig")

    def build(line_fixes, all_fixes, tx_lines):
        return _build_fix(data, target, original_diags, reports_text,
                          line_fixes, all_fixes, tx_lines)

    outcome = store.apply_fix_tx(batch_id, data["line_no"],
                                 expect_revision=data["base_revision"],
                                 raw_after=data["raw_after"], build=build)
    if outcome["outcome"] == "stale":
        reason = reasons.get("F_STALE_FIX")
        raise errors.Conflict(
            f"第 {data['line_no']} 行已修到第 {outcome['effective']} 代，"
            f"基于第 {data['base_revision']} 代的修改被拒绝",
            code="F_STALE_FIX",
            detail={"line_no": data["line_no"], "effective": outcome["effective"],
                    "submitted": data["base_revision"], "hint": reason.hint})
    fix = present_fix(outcome["fix"])
    return {"accepted": fix["status"] == APPLIED,
            "outcome": outcome["outcome"],
            "reused": outcome["outcome"] == "reused",
            "revision": fix["revision"],
            "fix": fix,
            "line": {"line_no": data["line_no"], "state": fix["state_after"],
                     "code": fix["code"]}}


# ---------------- 视图 ----------------
def present_fix(row: dict) -> dict:
    fix = dict(row)
    fix["diags_before"] = json.loads(fix.pop("diags_before_json") or "[]")
    fix["diags_after"] = json.loads(fix.pop("diags_after_json") or "[]")
    return fix


def view(store, batch_id: str) -> dict:
    fixes = [present_fix(r) for r in store.load_fixes(batch_id)]
    lines = store.load_lines(batch_id)
    by_line: dict[int, list[dict]] = {}
    for fix in fixes:
        by_line.setdefault(fix["line_no"], []).append(fix)

    states = []
    for line_no, group in sorted(by_line.items()):
        original = next((r for r in lines if r["line_no"] == line_no), None)
        applied = [f for f in group if f["status"] == APPLIED]
        current = applied[-1] if applied else None
        states.append({
            "line_no": line_no,
            "original_state": original["state"] if original else "",
            "current_state": (current["state_after"] if current
                              else (original["state"] if original else "")),
            "effective_revision": current["revision"] if current else 0,
            "attempts": len(group),
            "last_status": group[-1]["status"],
            "code": (current or group[-1]).get("code"),
        })

    latest_applied = {}
    for fix in fixes:
        if fix["status"] != APPLIED:
            continue
        seen = latest_applied.get(fix["line_no"])
        if seen is None or fix["revision"] > seen["revision"]:
            latest_applied[fix["line_no"]] = fix
    repairable = []
    for line in lines:
        if line["kind"] != "definition":
            continue
        fix = latest_applied.get(line["line_no"])
        state = fix["state_after"] if fix else line["state"]
        if state != "isolated":
            continue
        repairable.append({"line_no": line["line_no"],
                           "raw": fix["raw_after"] if fix else line["raw"],
                           "code": (fix["code"] if fix else line.get("code")) or "",
                           "base_revision": fix["revision"] if fix else 0})
    return {"fixes": fixes, "lines": states, "repairable": repairable,
            "count": len(fixes)}
