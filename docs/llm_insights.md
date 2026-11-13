# Evidence-Grounded Monthly Insights

Status: v1 generated one October 2026 report in Databricks (one model call). An audit found two
accepted claims that were numerically correct but misleading; **v2** (prompt and validator version
`v2`) adds the safeguards below. v2 has been tested offline only.

## Overview
`databricks/notebooks/05_llm_insights.py` turns `gold_insight_facts` into short monthly claims and
publishes only claims that pass deterministic validation. All logic that does not need Spark lives in
`databricks/notebooks/lib_insights.py`, which the notebook loads with `%run ./lib_insights` and the
offline tests import directly (one implementation).

| Generator | Model calls | Role |
|---|---|---|
| `template` | 0 | Deterministic fallback; the project works without LLM access |
| `llm` | ≤ `max_calls` per run, ≤ 1 per month | `system.ai.gpt-oss-120b` via `ai_query()` |

## Flow
1. Load one month's facts from `gold_insight_facts` (aggregates only).
2. **Evidence filter:** drop facts from other periods, unreliable estimates, every fact of an entity
   whose exact count is below 10, estimates with fewer than 10 expected sample records, entity strings
   that look like instructions or markup, **percentage changes where either month's count is below
   100**, and — for **provisional months** — all response-status, public-response, timeliness and
   submission-channel facts. No complaint records, complaint IDs, ZIP codes or narratives exist in
   Gold, so none can be sent.
3. **Prompt:** rules + compact JSON evidence (`id`, type, entity, metric, display value, rank, 95% CI).
   Real months produce 19–28k characters (about 5–7k tokens). The model must return only
   `{"claims": [{"text", "fact_ids"}]}`, at most 8 claims.
4. **Cache:** `evidence_hash` = SHA-256 of month, evidence, model and prompt version. A stored `llm` row
   with the **current prompt and validator versions and the same evidence hash** means no call (also
   for earlier failures). There is no force-refresh: regenerating under an identical key would have to
   overwrite stored history (see "Same-version regeneration").
5. **Call:** only when `dry_run = false`, the month is not cached, and the run is under `max_calls`.
   One `ai_query` query with `temperature = 0`, `max_tokens = 6000`, `failOnError => false`.
6. **Validate** every claim (below). The template output goes through the same validator.
7. **Plan publication** (pure Python, `plan_publication`), then — only when `dry_run = false` — apply
   it: supersede outdated rows, insert new rows. See "History and publication".

## Provisional months
A month is **provisional** while at least 1% of its complaints have company response "In progress"
(or when no response facts exist). This is a reporting safeguard, **not proof that a month is
complete**: falling below 1% only means the largest known source of revision has mostly cleared.

| Snapshot 2026-11-12 | In progress | Effect of incomplete responses |
|---|---|---|
| Mar–Aug 2026 | ≈ 0% | "chooses not to provide a public response" stable at 65–67% |
| Sep 2026 | 16.8% | provisional |
| Oct 2026 | 53.2% | provisional; the same category reads 19.5% (−53% MoM) only because responses are pending |

For provisional months, response, public-response, timeliness and channel metrics are withheld from
both the LLM prompt and the template, and the summary starts with a provisional-data disclaimer.
Every summary ends with a note that figures may be revised by later CFPB updates.

## Category meaning
CFPB labels are quoted exactly. "Company has responded to the consumer and the CFPB and chooses not to
provide a public response" means the company **did respond** and declined to publish a statement.
Complaints with **no public-response value** (not sent to the model) are mostly awaiting a response;
they must never be described as companies failing to respond.

## Validation rules (deterministic Python)
A claim is accepted only if all apply:
- **Structure:** response is a single JSON object (optionally fenced); claim text ≤ 400 characters;
  1–8 unique fact IDs; at most 8 claims (extra claims rejected).
- **Citations:** every fact ID exists in that month's evidence; every cited company, state, issue or
  sub-issue is named in the text; no uncited company/state/issue is named.
- **Numbers:** every number must match a cited fact at its displayed precision — counts (also `711k`,
  `0.71 million`), shares and changes as `%`, CI bounds, and percentage-point differences between cited
  shares. Signed changes must have the right sign. `about/more than/under` qualifiers are checked as
  bounds (approximations within 1%). When one entity is named, its numbers must come from that
  entity's facts. Years, `YYYY-MM` labels, "95% CI" and "top N" are not treated as measurements.
- **Comparisons:** "A more/higher … than B", "exceeded", "fewer … than" must agree with the cited values.
- **Rankings:** "largest", "second", "3rd", "smallest" must match the fact's rank in the evidence.
- **Direction:** "rose/increased/fell/declined" must agree with the sign of a cited change fact.
- **Estimates:** claims citing estimated facts must say "estimated" and state both 95% CI bounds;
  exact facts must not be called estimates.
- **Unsupported conclusions:** causal, predictive, legal, advisory or speculative wording is rejected
  (`because`, `due to`, `will`, `likely`, `fraud`, `illegal`, `should`, …).
- **Exact category labels:** cited response, public-response, channel and tag categories must be
  quoted as written (case-insensitive); paraphrases are rejected.
- **No misreading of missing values:** "no (public) response", "failed to respond", "did not
  respond/receive a response", "unanswered" and similar are rejected.
- **Absolute claims:** "100%" or "0%" must be exactly true (a rounded 99.97% or 0.004% is rejected);
  "all", "every", "none", "entirely", "exclusively" require a cited share of exactly 100% or 0%.
  "of all complaints" / "across all states" describe the base and are allowed.
- **Precision:** shares are shown with the fewest decimals that avoid a false 0% or 100%
  (e.g. 99.97%, 0.004%).
- Wording rules (causal, ranking, direction, comparison, misreading) are applied with cited and
  known labels blanked out, so words inside official CFPB labels ("caused", "third") do not trigger them.
- **Privacy / injection:** e-mail addresses, URLs, phone numbers, "complaint ID", "narrative", "ZIP code"
  and instruction-like text are rejected.

**Known limitations**
- A claim that names two entities could swap their two numbers without being detected (each number
  still belongs to a cited fact, and comparison wording is still checked).
- Rule-based wording checks catch listed phrasings, not every possible paraphrase; meaning is
  protected mainly by withholding unreliable evidence and requiring exact labels.
- The provisional threshold uses "In progress" as a proxy; other late revisions (e.g. late-entered
  phone or mail complaints) can still change non-provisional months slightly.
- Month-over-month growth of total volume is shown for provisional months; it is counted at intake
  and was not materially affected, but the month's total can still rise slightly.

## History and publication
### Base tables = append-only audit history
| Table | Key | Content |
|---|---|---|
| `gold_llm_insights` | month, generator, prompt_version, validator_version, evidence_hash | validation `status` (`verified` / `rejected` / `failed`), counts, model, evidence hash, summary, response SHA-256 and length, truncated error, `generated_at`, `publication_status`, `superseded_at`, `superseded_reason` |
| `gold_llm_insight_claims` | same + claim_index | claim `status` (`accepted` / `rejected`), reasons, fact IDs, text for accepted claims, `publication_status`, `superseded_at` |

- Rows are **never deleted** and their content is **never rewritten**. After insertion only
  `publication_status`, `superseded_at` and `superseded_reason` can change, and only once
  (a superseded row is never touched again).
- `status` remains the original validation outcome. `publication_status` is the lifecycle:
  `published` (current and verified / accepted claim of one), `not_published` (current but rejected or
  failed, or a rejected claim) or `superseded` (older version or older evidence).
- A row is **superseded** when its prompt/validator version differs from the current one (any month),
  or when its version is current but its evidence hash differs from the evidence computed now for a
  month processed in this run. Claims are superseded together with their insight row.
- Superseded rows keep their summaries, claims, fact IDs, evidence hashes, model metadata, validation
  outcomes and timestamps for audit. **Base-table rows can show `status = verified` while superseded —
  do not read the base tables as "current insights".**

### Published views = what consumers read
| View | Exposes |
|---|---|
| `gold_llm_insights_published` | rows with `publication_status = 'published'` and `status = 'verified'` (at most one per month and generator) |
| `gold_llm_insight_claims_published` | claims with `status = 'accepted'` and `publication_status = 'published'` whose insight row with the **same month, generator, versions and evidence hash** is published |

Dashboards, reports and any future LLM consumer must read only these views.

### Privacy
Rejected claims keep only a SHA-256 hash, length, reasons and fact IDs; their text is stored only if
`store_rejected_text = true`. Raw model responses are never stored.

### Same-version regeneration (limitation)
History is preserved across **prompt/validator versions** and across **evidence changes**. It is not
possible to keep two different outputs for the same month, generator, versions **and** evidence: such
a run is detected as a **conflict** and stopped before any write, instead of overwriting the stored row.
Template output is deterministic, so this only matters for LLM regeneration. If repeated LLM samples
under identical evidence are ever wanted, the safe approach is an additive `generation_id` column in
the key (plus a rule for which generation is published); this has not been implemented.

### Migration (first `dry_run = false` run of notebook 05 v2)
Additive and idempotent, in this order (each step is safe to repeat; stopping anywhere is safe):
1. `ALTER TABLE … ADD COLUMNS` for missing lifecycle columns only (existing rows get NULLs).
2. `CREATE OR REPLACE VIEW` for the two published views (they never expose NULL-status rows).
3. Supersede outdated insight rows, then all claims of superseded insights.
4. Recover current rows (and their claims) that still have a NULL `publication_status`.
5. Insert new claims, then new insight rows (insert-only `MERGE`).

There is no automatic rollback. Undoing the migration would mean `ALTER TABLE … DROP COLUMNS` (needs
Delta column mapping) and restoring lifecycle values from Delta table history (`RESTORE`/time travel,
limited by retention) — both destructive and not provided. Because nothing is deleted and content is
never rewritten, a failed or partial run is recovered by simply rerunning the notebook.

## Running (when approved)
| Goal | Widgets | Model calls | Writes |
|---|---|---|---|
| Preview prompts and publication plan | `dry_run = true` (default) | 0 | none (no schema change, no views) |
| Migrate + template insights for all months | `dry_run = false`, `max_calls = 0`, `months = all` | 0 | yes |
| One LLM month | `dry_run = false`, `max_calls = 1`, `months = latest` | ≤ 1 | yes |
| Rerun unchanged | same as above | 0 (cached) | no changes |

Version superseding applies to every month, so a template-only run with `months = latest` would leave
the other months without a current template insight until they are processed (the notebook prints a
note); use `months = all` for the migration run.

## Free Edition limits (from Databricks documentation, checked 2026-11-12)
- Free Edition is "available at no cost" under a fair-usage policy; no payment method is required.
- Numeric Free Edition limits for foundation models are not published. Exceeding the quota can shut
  workspace compute down for the rest of the day (in extreme cases the month).
- Documented limitations include "No custom models on GPU or batch inference"; whether this affects
  `ai_query` over a small table is not stated. The notebook sends one row per month called.
- No statement was found that prompts to this model are excluded from training; only aggregate,
  public statistics are sent.

## Tests
`tests/test_publication.py` (offline): superseding, publication status, preservation of historical
fields, idempotency, recovery from interrupted runs, conflicts instead of overwrites, cache keys,
claim/insight consistency, the template-only migration scenario on real data, and static checks that
notebook 05 exits before any write in dry-run mode and contains no destructive SQL.

`tests/test_insights.py` (offline): evidence filtering, prompt contents and size, cache hashing,
malformed responses, prompt injection in data and in output, every rejection rule, the claim limit,
template claims for all 22 real months, storage privacy, and notebook safety defaults.
