#!/usr/bin/env python3
"""Validate the operator-facing monitoring assets (issue #1069).

This script is the part of the `monitoring` CI job that does not need
promtool. It enforces three things:

  1. deploy/grafana/sorotrail-dashboard.json parses as JSON and is a
     Grafana dashboard rather than arbitrary JSON — it asserts the fields
     Grafana actually requires (title, schemaVersion, panels, and a type,
     title, and query per panel). It deliberately does not validate the
     full Grafana schema, which would be a maintenance burden for little
     gain.

  2. Every metric referenced by a rule expression in
     deploy/prometheus/sorotrail-rules.yml exists in the canonical
     exported-metrics list (deploy/prometheus/exported-metrics.txt). A
     rule that references a renamed or deleted metric parses fine and
     never fires, which is exactly the failure this catches.

  3. The same cross-check for every panel query in the dashboard, because
     a panel on a nonexistent metric is a blank panel — less dangerous
     than a dead alert, but still misleading to the operator.

promtool still owns everything PromQL- and YAML-shaped: this script only
scans `expr:` values textually, so it assumes expressions live on the
`expr:` line (or in the folded block right after it), which is all this
repository uses. Run `promtool check rules` first (the Makefile target
does) so a file this scan misunderstands has already been rejected.

Run from the repository root: python3 scripts/validate_monitoring.py
Exits non-zero and lists every offending metric on failure.
"""

import json
import re
import sys
from pathlib import Path

RULES = Path("deploy/prometheus/sorotrail-rules.yml")
DASHBOARD = Path("deploy/grafana/sorotrail-dashboard.json")
EXPORTED = Path("deploy/prometheus/exported-metrics.txt")

# The check is prefix-based (see the issue): anything named sorotrail_* is
# assumed to be one of ours and must be in the exported list. The HTTP
# histogram is the one metric this project exports without the prefix, so
# it is the explicit exception rather than widening the match to every
# identifier in the expression (which would flag PromQL keywords and
# recording-rule names alike).
METRIC_RE = re.compile(r"(?<![A-Za-z0-9_])(?:sorotrail_[A-Za-z0-9_]+|http_request_duration_seconds[A-Za-z0-9_]*)")

# Histograms and HistogramVecs are listed by base name only; expressions
# legitimately mention these derived series.
HISTOGRAM_SUFFIXES = ("_bucket", "_count", "_sum")


def load_exported(path: Path) -> set[str]:
    if not path.is_file():
        fail(f"{path} is missing — it is the canonical metric list this check consumes")
    names = {
        line.split("#", 1)[0].strip()
        for line in path.read_text().splitlines()
    }
    return {name for name in names if name}


def normalize(token: str, exported: set[str]) -> str:
    """Map a derived series name to its base metric, if the base exists."""
    for suffix in HISTOGRAM_SUFFIXES:
        if token.endswith(suffix) and token[: -len(suffix)] in exported:
            return token[: -len(suffix)]
    return token


def extract_rule_exprs(path: Path) -> list[str]:
    """Pull every `expr:` value out of a rule file.

    Handles the single-line form (`expr: rate(x[5m])`) and the folded /
    literal block forms that follow an `expr:` key on its own line.
    """
    exprs = []
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        match = re.match(r"^(\s*)expr:\s*(.*)$", line)
        if not match:
            continue
        value = match.group(2).strip()
        if value in ("", ">", ">-", "|", "|-", ">+", ">+-", "|+", "|+-"):
            indent = len(match.group(1))
            block = []
            for cont in lines[i + 1:]:
                if cont.strip() == "":
                    continue
                if len(cont) - len(cont.lstrip()) <= indent:
                    break
                block.append(cont.strip())
            value = " ".join(block)
        exprs.append(value.strip("\"'"))
    return exprs


def extract_dashboard_exprs(dashboard: dict) -> list[str]:
    """Walk (possibly nested) panels and collect every target expression."""
    exprs = []

    def walk(panels):
        for panel in panels:
            for target in panel.get("targets") or []:
                expr = target.get("expr")
                if isinstance(expr, str) and expr.strip():
                    exprs.append(expr.strip())
            walk(panel.get("panels") or [])

    walk(dashboard.get("panels") or [])
    return exprs


def check_metrics(source: Path, exprs: list[str], exported: set[str]) -> list[str]:
    unknown = sorted(
        {
            normalize(token, exported)
            for expr in exprs
            for token in METRIC_RE.findall(expr)
            if normalize(token, exported) not in exported
        }
    )
    if unknown:
        print(f"FAIL: {source} references metrics that SoroTrail does not export:")
        for name in unknown:
            print(f"  - {name}")
        print(f"  Either fix the reference or add the metric to {EXPORTED}.")
    else:
        print(f"ok:   {source}: all {len(exprs)} expression(s) reference only exported metrics")
    return unknown


def check_dashboard_structure(path: Path) -> tuple[dict | None, list[str]]:
    """Parse the dashboard and assert the fields Grafana requires."""
    problems = []
    try:
        dashboard = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return None, [f"{path} is not valid JSON: {exc}"]
    if not isinstance(dashboard, dict):
        return None, [f"{path} must be a JSON object, got {type(dashboard).__name__}"]

    if not isinstance(dashboard.get("title"), str) or not dashboard["title"].strip():
        problems.append("top-level 'title' must be a non-empty string")
    if not isinstance(dashboard.get("schemaVersion"), int):
        problems.append("top-level 'schemaVersion' must be an integer (Grafana export marker)")
    panels = dashboard.get("panels")
    if not isinstance(panels, list) or not panels:
        problems.append("top-level 'panels' must be a non-empty array")
        return dashboard, problems

    def check_panels(items, where):
        for i, panel in enumerate(items):
            label = f"{where}[{i}]"
            if not isinstance(panel, dict):
                problems.append(f"{label} must be an object")
                continue
            if not isinstance(panel.get("type"), str) or not panel["type"].strip():
                problems.append(f"{label} is missing a 'type'")
            if not isinstance(panel.get("title"), str) or not panel["title"].strip():
                problems.append(f"{label} is missing a 'title'")
            # Row panels are layout-only and legitimately have no query.
            if panel.get("type") != "row":
                targets = panel.get("targets")
                if not isinstance(targets, list) or not targets:
                    problems.append(f"{label} ({panel.get('title', '?')!r}) has no 'targets'")
                elif not any(
                    isinstance(t.get("expr"), str) and t["expr"].strip() for t in targets if isinstance(t, dict)
                ):
                    problems.append(f"{label} ({panel.get('title', '?')!r}) has no target with an 'expr'")
            nested = panel.get("panels")
            if isinstance(nested, list) and nested:
                check_panels(nested, f"{label}.panels")

    check_panels(panels, "panels")
    return dashboard, problems


GRAFANA_DIR = Path("deploy/grafana")
RECORDED_RE = re.compile(r"(?<![A-Za-z0-9_:])sorotrail:[A-Za-z0-9_:]+")


def panel_descriptions_missing(dashboard: dict) -> list[str]:
    """Every non-row panel must say what good looks like (issue #1258)."""
    missing = []

    def walk(panels):
        for panel in panels:
            if panel.get("type") != "row" and not str(panel.get("description", "")).strip():
                missing.append(str(panel.get("title", "?")))
            walk(panel.get("panels") or [])

    walk(dashboard.get("panels") or [])
    return missing


def main() -> int:
    dashboards = sorted(GRAFANA_DIR.glob("*.json"))
    for asset in [RULES, *dashboards]:
        if not asset.is_file():
            print(f"FAIL: {asset} is missing")
            return 1
    if not dashboards:
        print(f"FAIL: no dashboards under {GRAFANA_DIR}")
        return 1

    exported = load_exported(EXPORTED)
    failures = []
    rule_text = RULES.read_text()
    recorded = set(re.findall(r"record:\s*(\S+)", rule_text))
    uids = {}

    for path in dashboards:
        dashboard, problems = check_dashboard_structure(path)
        if problems:
            print(f"FAIL: {path} is not a valid Grafana dashboard:")
            for problem in problems:
                print(f"  - {problem}")
            failures.extend(problems)
        else:
            print(f"ok:   {path}: valid JSON with the required dashboard fields")
        if dashboard is None:
            continue
        uid = dashboard.get("uid")
        if not uid or uid in uids:
            failures.append(f"{path}: uid {uid!r} is empty or duplicates {uids.get(uid)}")
            print(f"FAIL: {failures[-1]}")
        uids[uid] = path
        exprs = extract_dashboard_exprs(dashboard)
        failures.extend(check_metrics(path, exprs, exported))
        unknown = sorted({t for e in exprs for t in RECORDED_RE.findall(e) if t not in recorded})
        if unknown:
            print(f"FAIL: {path} reads recording rules that {RULES} does not define: {unknown}")
            failures.extend(unknown)
        if path.name != "sorotrail-dashboard.json":
            missing = panel_descriptions_missing(dashboard)
            if missing:
                print(f"FAIL: {path} panels without a description: {missing}")
                failures.extend(missing)

    failures.extend(check_metrics(RULES, extract_rule_exprs(RULES), exported))

    alerts = rule_text.count("- alert:")
    if alerts != rule_text.count("dashboard_url:"):
        print("FAIL: every alert must carry a dashboard_url annotation")
        failures.append("dashboard_url")

    if failures:
        return 1
    print("All monitoring asset checks passed.")
    return 0

def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
