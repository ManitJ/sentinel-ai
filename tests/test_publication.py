"""Offline tests for superseding, publication status, history preservation and idempotency.

Spark is not available offline, so plans from ``lib_insights.plan_publication`` are applied to an
in-memory model of the two tables that follows exactly the same rules as the SQL in notebook 05
(supersede -> claims follow -> recover NULL statuses -> insert claims -> insert insights). Static
tests check that the notebook SQL has that shape.
"""

import copy
import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("lib_insights", ROOT / "databricks/notebooks/lib_insights.py")
L = importlib.util.module_from_spec(spec)
spec.loader.exec_module(L)
NB = (ROOT / "databricks/notebooks/05_llm_insights.py").read_text()
SAMPLE = ROOT / "data/bronze/cfpb_credit_reporting_sample/_upload/cfpb_credit_reporting_sample.parquet"
REF_JSON = ROOT / "data/reference/cfpb_api_monthly_credit_reporting.json"

MONTHS = ["2026-08", "2026-09", "2026-10"]
RUN1, RUN2 = datetime(2026, 11, 13, 10), datetime(2026, 11, 14, 10)


# --- in-memory model of the tables (mirrors notebook 05 section 7) ----------------------

def apply(insights, claims, plan, candidate_claims, run_at):
    by_key = {L.row_key(r): r for r in insights}
    for item in plan["supersede"]:                                    # 7.3 insights
        row = by_key[L.row_key(item)]
        if row.get("publication_status") != L.SUPERSEDED:
            row.update(publication_status=L.SUPERSEDED, superseded_at=run_at,
                       superseded_reason=item["superseded_reason"])
    superseded = {k for k, r in by_key.items() if r.get("publication_status") == L.SUPERSEDED}
    for claim in claims:                                              # 7.3 claims follow parent
        if L.row_key(claim) in superseded and claim.get("publication_status") != L.SUPERSEDED:
            claim.update(publication_status=L.SUPERSEDED, superseded_at=run_at)
    for item in plan["set_status"]:                                   # 7.4 recovery
        row = by_key[L.row_key(item)]
        if row.get("publication_status") is None:
            row["publication_status"] = item["publication_status"]
    parents = {k: r["publication_status"] for k, r in by_key.items()
               if r.get("publication_status") in (L.PUBLISHED, L.NOT_PUBLISHED)}
    for claim in claims:
        if claim.get("publication_status") is None and L.row_key(claim) in parents:
            claim["publication_status"] = L.claim_publication_status(claim["status"], parents[L.row_key(claim)])
    claim_keys = {(L.row_key(c), c["claim_index"]) for c in claims}
    for row in plan["insert"]:                                        # 7.5 claims, then insights
        for claim in candidate_claims[L.row_key(row)]:
            if (L.row_key(claim), claim["claim_index"]) not in claim_keys:
                claims.append({**claim, "superseded_at": None,
                               "publication_status": L.claim_publication_status(claim["status"],
                                                                                row["publication_status"])})
    for row in plan["insert"]:
        if L.row_key(row) not in by_key:
            insights.append(dict(row))


def published_insights(insights):                                     # gold_llm_insights_published
    return [r for r in insights if r.get("publication_status") == L.PUBLISHED and r["status"] == "verified"]


def published_claims(claims, insights):                               # gold_llm_insight_claims_published
    live = {L.row_key(r) for r in published_insights(insights)}
    return [c for c in claims if L.row_key(c) in live
            and c.get("publication_status") == L.PUBLISHED and c["status"] == "accepted"]


# --- fixtures ---------------------------------------------------------------------------

def v1_state():
    """What notebook 05 v1 wrote: template rows for 3 months and one October LLM row (6+2 claims)."""
    insights, claims = [], []
    for m in MONTHS:
        insights.append({"month_label": m, "generator": "template", "model": None, "prompt_version": "v1",
                         "validator_version": "v1", "evidence_hash": f"v1-{m}", "status": "verified",
                         "claims_accepted": 2, "summary_markdown": f"v1 template {m}",
                         "generated_at": datetime(2026, 11, 12), "_row_hash": f"sparkhash-{m}"})
        for i in range(2):
            claims.append({"month_label": m, "generator": "template", "prompt_version": "v1",
                           "validator_version": "v1", "evidence_hash": f"v1-{m}", "claim_index": i,
                           "status": "accepted", "claim_text": f"v1 claim {i}", "fact_ids": ["a" * 16]})
    insights.append({"month_label": "2026-10", "generator": "llm", "model": L.MODEL, "prompt_version": "v1",
                     "validator_version": "v1", "evidence_hash": "v1-llm-2026-10", "status": "verified",
                     "claims_accepted": 6, "summary_markdown": "Web submissions represented 100% ...",
                     "response_sha256": "f" * 64, "generated_at": datetime(2026, 11, 12, 21),
                     "_row_hash": "sparkhash-llm"})
    for i in range(8):
        claims.append({"month_label": "2026-10", "generator": "llm", "prompt_version": "v1", "validator_version": "v1",
                       "evidence_hash": "v1-llm-2026-10", "claim_index": i,
                       "status": "accepted" if i < 6 else "rejected",
                       "claim_text": f"llm claim {i}" if i < 6 else None, "fact_ids": ["b" * 16]})
    return insights, claims


def template_candidates(months=MONTHS, status="verified", hashes=None):
    rows, claims = [], {}
    for m in months:
        row = {"month_label": m, "generator": "template", "model": None, "prompt_version": L.PROMPT_VERSION,
               "validator_version": L.VALIDATOR_VERSION, "evidence_hash": (hashes or {}).get(m, f"v2-{m}"),
               "status": status, "claims_accepted": 2 if status == "verified" else 0,
               "summary_markdown": f"v2 template {m}" if status == "verified" else None, "generated_at": RUN1}
        row["_row_hash"] = L.content_hash(row)
        rows.append(row)
        claims[L.row_key(row)] = [
            {"month_label": m, "generator": "template", "prompt_version": L.PROMPT_VERSION,
             "validator_version": L.VALIDATOR_VERSION, "evidence_hash": row["evidence_hash"], "claim_index": i,
             "status": "accepted" if status == "verified" else "rejected", "claim_text": f"v2 claim {i}",
             "fact_ids": ["c" * 16]} for i in range(2)]
    return rows, claims


def current_hashes(months=MONTHS, hashes=None):
    return {(m, g): (hashes or {}).get(m, f"v2-{m}") for m in months for g in ("template", "llm")}


def template_only_run(insights, claims, run_at=RUN1, months=MONTHS, hashes=None):
    cands, cand_claims = template_candidates(months, hashes=hashes)
    plan = L.plan_publication(insights, cands, current_hashes(months, hashes))
    apply(insights, claims, plan, cand_claims, run_at)
    return plan


HISTORY_FIELDS = ("status", "summary_markdown", "model", "evidence_hash", "prompt_version", "validator_version",
                  "generated_at", "response_sha256", "_row_hash", "claims_accepted")


# --- the template-only migration run ----------------------------------------------------

def test_template_only_run_supersedes_v1_and_publishes_v2():
    insights, claims = v1_state()
    plan = template_only_run(insights, claims)
    assert len(plan["supersede"]) == 4 and len(plan["insert"]) == 3 and not plan["conflicts"]
    assert {r["superseded_reason"] for r in plan["supersede"]} == {"prompt v1 -> v2; validator v1 -> v2"}
    llm = next(r for r in insights if r["generator"] == "llm")
    assert llm["publication_status"] == L.SUPERSEDED and llm["superseded_at"] == RUN1
    pub = published_insights(insights)
    assert sorted((r["month_label"], r["generator"], r["prompt_version"]) for r in pub) == [
        (m, "template", "v2") for m in MONTHS]
    assert not [r for r in pub if r["generator"] == "llm"]            # no current October LLM insight


def test_superseded_accepted_claims_never_appear_as_published():
    insights, claims = v1_state()
    template_only_run(insights, claims)
    llm_claims = [c for c in claims if c["generator"] == "llm"]
    assert len(llm_claims) == 8 and all(c["publication_status"] == L.SUPERSEDED for c in llm_claims)
    assert sum(c["status"] == "accepted" for c in llm_claims) == 6     # validation outcome preserved
    published = published_claims(claims, insights)
    assert all(c["prompt_version"] == "v2" for c in published)
    assert len(published) == sum(r["claims_accepted"] for r in published_insights(insights))


def test_history_is_preserved_exactly():
    insights, claims = v1_state()
    before_i, before_c = copy.deepcopy(insights), copy.deepcopy(claims)
    template_only_run(insights, claims)
    assert len(insights) == len(before_i) + 3 and len(claims) == len(before_c) + 6   # nothing deleted
    after = {L.row_key(r): r for r in insights}
    for old in before_i:
        new = after[L.row_key(old)]
        assert {k: new.get(k) for k in HISTORY_FIELDS} == {k: old.get(k) for k in HISTORY_FIELDS}
        assert set(new) - set(old) <= set(L.LIFECYCLE_FIELDS)      # only lifecycle columns added
    claims_after = {(L.row_key(c), c["claim_index"]): c for c in claims}
    for old in before_c:
        new = claims_after[(L.row_key(old), old["claim_index"])]
        assert {k: v for k, v in new.items() if k not in L.LIFECYCLE_FIELDS} == old


def test_plan_never_contains_destructive_actions():
    insights, claims = v1_state()
    plan = L.plan_publication(insights, template_candidates()[0], current_hashes())
    assert set(plan) == {"supersede", "set_status", "insert", "unchanged", "conflicts"}
    assert all(set(item) == set(L.KEY_FIELDS) | {"superseded_reason"} for item in plan["supersede"])


# --- idempotency and recovery -------------------------------------------------------------

def test_second_identical_run_changes_nothing():
    insights, claims = v1_state()
    template_only_run(insights, claims, RUN1)
    snapshot = copy.deepcopy((insights, claims))
    plan = template_only_run(insights, claims, RUN2)
    assert (plan["supersede"], plan["insert"], plan["set_status"], plan["conflicts"]) == ([], [], [], [])
    assert len(plan["unchanged"]) == 3
    assert (insights, claims) == snapshot                              # superseded_at not refreshed


def test_v1_rows_with_null_publication_status_are_handled():
    insights, claims = v1_state()
    assert all(r.get("publication_status") is None for r in insights)
    plan = L.plan_publication(insights, [], {})                        # even with nothing processed
    assert len(plan["supersede"]) == 4                                 # version rule is global


@pytest.mark.parametrize("stop_after", ["supersede", "claims"])
def test_partial_run_is_completed_by_a_rerun(stop_after):
    full_i, full_c = v1_state()
    template_only_run(full_i, full_c)

    insights, claims = v1_state()
    cands, cand_claims = template_candidates()
    plan = L.plan_publication(insights, cands, current_hashes())
    partial = {**plan, "insert": []} if stop_after == "supersede" else plan
    apply(insights, claims, partial, cand_claims, RUN1)
    if stop_after == "claims":                                         # claims inserted, insights not yet
        insights[:] = [r for r in insights if r["prompt_version"] == "v1"]
    # Interrupted state is safe: nothing outdated or orphaned is published.
    assert not published_insights(insights)
    assert not published_claims(claims, insights)
    template_only_run(insights, claims, RUN1)                          # rerun completes the work
    key = lambda rows: sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)  # noqa: E731
    assert key(insights) == key(full_i) and key(claims) == key(full_c)


def test_recovery_sets_missing_status_on_current_rows_and_claims():
    insights, claims = [], []
    cands, cand_claims = template_candidates(["2026-08"])
    row = {**cands[0], "publication_status": None, "superseded_at": None, "superseded_reason": None}
    insights.append(row)
    claims.extend({**c, "publication_status": None} for c in cand_claims[L.row_key(row)])
    plan = L.plan_publication(insights, cands, current_hashes(["2026-08"]))
    assert plan["set_status"][0]["publication_status"] == L.PUBLISHED and not plan["insert"]
    apply(insights, claims, plan, cand_claims, RUN1)
    assert row["publication_status"] == L.PUBLISHED
    assert {c["publication_status"] for c in claims} == {L.PUBLISHED}


# --- same-version changes, conflicts, cache --------------------------------------------------

def test_new_evidence_under_same_version_supersedes_old_row():
    insights, claims = v1_state()
    template_only_run(insights, claims, RUN1)
    plan = template_only_run(insights, claims, RUN2, hashes={"2026-10": "v2-2026-10-revised"})
    assert [(r["month_label"], r["superseded_reason"]) for r in plan["supersede"]] == [("2026-10", "evidence changed")]
    oct_rows = [r for r in insights if r["month_label"] == "2026-10" and r["generator"] == "template"]
    assert [(r["prompt_version"], r["evidence_hash"], r["publication_status"]) for r in oct_rows] == [
        ("v1", "v1-2026-10", L.SUPERSEDED), ("v2", "v2-2026-10", L.SUPERSEDED),
        ("v2", "v2-2026-10-revised", L.PUBLISHED)]


def test_same_key_with_different_content_is_a_conflict_not_an_overwrite():
    insights, claims = v1_state()
    template_only_run(insights, claims)
    cands, _ = template_candidates()
    cands[0] = {**cands[0], "summary_markdown": "different text"}
    cands[0]["_row_hash"] = L.content_hash(cands[0])
    plan = L.plan_publication(insights, cands, current_hashes())
    assert plan["conflicts"] == [L.row_key(cands[0])] and not plan["insert"] and not plan["supersede"]


def test_rejected_template_is_stored_but_not_published():
    cands, cand_claims = template_candidates(["2026-08"], status="rejected")
    insights, claims = [], []
    apply(insights, claims, L.plan_publication([], cands, current_hashes(["2026-08"])), cand_claims, RUN1)
    assert insights[0]["publication_status"] == L.NOT_PUBLISHED and not published_insights(insights)
    assert {c["publication_status"] for c in claims} == {L.NOT_PUBLISHED}


def test_llm_cache_requires_current_versions_and_same_evidence():
    keys = {("2026-10", "llm", "v1", "v1", "h1"), ("2026-10", "llm", "v2", "v2", "h2")}
    assert L.llm_cache_hit(keys, "2026-10", "h2")
    assert not L.llm_cache_hit(keys, "2026-10", "h1")                  # v1 output never reused
    assert not L.llm_cache_hit(keys, "2026-10", "h3")                  # new evidence -> call allowed
    assert not L.llm_cache_hit(keys, "2026-09", "h2")


def test_llm_month_over_cap_with_stale_evidence_is_superseded_not_called():
    insights = [{"month_label": "2026-10", "generator": "llm", "prompt_version": L.PROMPT_VERSION,
                 "validator_version": L.VALIDATOR_VERSION, "evidence_hash": "old", "status": "verified",
                 "publication_status": L.PUBLISHED, "_row_hash": "x"}]
    plan = L.plan_publication(insights, [], {("2026-10", "llm"): "new"})
    assert [r["superseded_reason"] for r in plan["supersede"]] == ["evidence changed"]
    assert L.current_rows_after(insights, plan) == {}


@pytest.mark.parametrize("claim_status, parent, expected", [
    ("accepted", L.PUBLISHED, L.PUBLISHED), ("rejected", L.PUBLISHED, L.NOT_PUBLISHED),
    ("accepted", L.NOT_PUBLISHED, L.NOT_PUBLISHED), ("accepted", L.SUPERSEDED, L.SUPERSEDED),
    ("rejected", L.SUPERSEDED, L.SUPERSEDED)])
def test_claim_publication_status(claim_status, parent, expected):
    assert L.claim_publication_status(claim_status, parent) == expected


def test_content_hash_ignores_lifecycle_and_generation_time():
    row = template_candidates(["2026-08"])[0][0]
    assert L.content_hash({**row, "generated_at": RUN2, "publication_status": "superseded",
                           "superseded_at": RUN2, "superseded_reason": "x"}) == row["_row_hash"]
    assert L.content_hash({**row, "summary_markdown": "changed"}) != row["_row_hash"]


def test_keys_include_versions_and_evidence():
    assert L.KEY_FIELDS == ("month_label", "generator", "prompt_version", "validator_version", "evidence_hash")


# --- notebook 05 static safety checks --------------------------------------------------------

CODE = "\n".join(line for line in NB.splitlines() if not line.lstrip().startswith("#"))
WRITE_PATTERNS = [r"ALTER TABLE", r"CREATE TABLE", r"CREATE OR REPLACE VIEW", r"COMMENT ON TABLE",
                  r"MERGE INTO", r"\bINSERT\b", r"\bUPDATE\b"]


def test_dry_run_exits_before_any_write():
    exit_pos = CODE.index("if DRY_RUN:\n    dbutils.notebook.exit")
    for pattern in WRITE_PATTERNS:
        positions = [m.start() for m in re.finditer(pattern, CODE)]
        assert positions and min(positions) > exit_pos, pattern
    assert "to_call = [] if DRY_RUN else pending[:MAX_CALLS]" in CODE


def test_notebook_has_no_destructive_sql():
    for forbidden in [r"\bDELETE\b", r"\bDROP\b", r"\bTRUNCATE\b", r"NOT MATCHED BY SOURCE", r"UPDATE SET \*",
                      r"INSERT OVERWRITE", r"mode\(\"overwrite\"\)", r"REPLACE TABLE"]:
        assert not re.search(forbidden, CODE, re.IGNORECASE), forbidden


def test_notebook_merges_use_versioned_keys_and_lifecycle_only_updates():
    assert 'KEY_ON = lambda t, s: " AND ".join(f"{t}.`{k}` <=> {s}.`{k}`" for k in KEY_FIELDS)' in CODE
    updates = re.findall(r"UPDATE SET(.*?)(?:\"\"\"|WHEN)", CODE, re.DOTALL)
    assert len(updates) == 4
    for clause in updates:
        # Bare target columns on the left (documented MERGE grammar); qualified names are sources.
        assigned = set(re.findall(r"(?<![.\w])(\w+)\s*=(?!=)", clause))
        assert assigned and assigned <= {"publication_status", "superseded_at", "superseded_reason"}, assigned
        assert not re.search(r"\b[tc]\.\w+\s*=(?!=)", clause.split("CASE")[0]), clause
    assert "WHEN NOT MATCHED THEN INSERT *" in CODE
    # Claims of every superseded insight are superseded too (idempotent, keyed on the parent).
    assert ("spark.table(INSIGHTS).where(\"publication_status = 'superseded'\").select(*KEY_FIELDS).distinct()"
            in CODE)
    assert "MERGE INTO {CLAIMS} c USING sentinel_superseded_parents s ON {KEY_ON('c', 's')}" in CODE
    # Claims are inserted before their insight rows.
    assert CODE.index("sentinel_new_claims") < CODE.index("sentinel_new_insights")
    assert "if c not in spark.table(table).columns" in CODE              # ADD COLUMNS only when missing


def test_published_claims_view_requires_a_published_parent_with_same_versions():
    view = CODE[CODE.index("CREATE OR REPLACE VIEW {CLAIMS_VIEW}"):]
    view = view[:view.index('""")')]
    assert "JOIN {INSIGHTS} i ON {KEY_ON('c', 'i')}" in view
    for condition in ["i.publication_status = 'published'", "i.status = 'verified'",
                      "c.publication_status = 'published'", "c.status = 'accepted'"]:
        assert condition in view
    insights_view = CODE[CODE.index("CREATE OR REPLACE VIEW {INSIGHTS_VIEW}"):]
    assert "WHERE publication_status = 'published' AND status = 'verified'" in insights_view


def test_force_refresh_removed_because_it_would_overwrite_history():
    assert "force_refresh" not in CODE


@pytest.mark.skipif(not (SAMPLE.exists() and REF_JSON.exists()), reason="local data not present")
def test_real_data_template_only_migration_scenario():
    from lakehouse.gold_metrics import build_gold
    from lakehouse.silver_rules import transform_record
    facts = build_gold([transform_record(r) for r in pq.read_table(SAMPLE).to_pylist()],
                       json.loads(REF_JSON.read_text()))["facts"]
    months = sorted({f["period"] for f in facts if re.fullmatch(r"\d{4}-\d{2}", f["period"])})
    insights, claims = v1_state()
    cands, cand_claims, hashes = [], {}, {}
    for m in months:
        evidence, _ = L.select_evidence(facts, m)
        hashes[m] = L.evidence_hash(m, evidence)
        results = L.validate_claims(L.template_claims(m, evidence), evidence, facts)
        row = {"month_label": m, "generator": "template", "model": None, "prompt_version": L.PROMPT_VERSION,
               "validator_version": L.VALIDATOR_VERSION, "evidence_hash": hashes[m],
               "status": L.month_status(None, results),
               "claims_accepted": sum(r["status"] == "accepted" for r in results),
               "summary_markdown": L.summary_markdown(m, results, L.month_context(facts, m)["provisional"])}
        row["_row_hash"] = L.content_hash(row)
        cands.append(row)
        cand_claims[L.row_key(row)] = [{"month_label": m, "generator": "template", "prompt_version": "v2",
                                        "validator_version": "v2", "evidence_hash": hashes[m], **rec}
                                       for rec in L.claim_records(results)]
    plan = L.plan_publication(insights, cands, {(m, g): hashes[m] for m in months for g in ("template", "llm")})
    apply(insights, claims, plan, cand_claims, RUN1)
    pub = {(r["month_label"], r["generator"]): r for r in published_insights(insights)}
    assert len(pub) == 22 and not any(g == "llm" for _m, g in pub)
    assert L.PROVISIONAL_NOTE in pub[("2026-10", "template")]["summary_markdown"]
    assert L.PROVISIONAL_NOTE in pub[("2026-09", "template")]["summary_markdown"]
    assert "on time" not in pub[("2026-10", "template")]["summary_markdown"]
    assert "on time" in pub[("2026-08", "template")]["summary_markdown"]
    assert "711,218" in pub[("2026-10", "template")]["summary_markdown"]
    assert next(r for r in insights if r["generator"] == "llm")["publication_status"] == L.SUPERSEDED
    again = L.plan_publication(insights, cands, {(m, g): hashes[m] for m in months for g in ("template", "llm")})
    assert (again["supersede"], again["insert"], again["set_status"]) == ([], [], [])


def test_sql_uses_documented_forms_only():
    assert "USING (SELECT" not in CODE                     # MERGE sources are staged temp views
    assert "TIMESTAMP '" not in CODE                       # epoch-based timestamps instead of literals
    assert 'RUN_AT_SQL = f"timestamp_seconds({int(RUN_AT.timestamp())})"' in CODE
    assert CODE.count("{RUN_AT_SQL}") == 2
