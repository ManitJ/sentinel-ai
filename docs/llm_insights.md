# Evidence-Grounded Monthly Insights

Status: **implemented and tested offline; not yet run in Databricks.** No live model call has been made
by this code.

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
   whose exact count is below 10, estimates with fewer than 10 expected sample records, and entity
   strings that look like instructions or markup. No complaint records, complaint IDs, ZIP codes or
   narratives exist in Gold, so none can be sent.
3. **Prompt:** rules + compact JSON evidence (`id`, type, entity, metric, display value, rank, 95% CI).
   Real months produce 19–28k characters (about 5–7k tokens). The model must return only
   `{"claims": [{"text", "fact_ids"}]}`, at most 8 claims.
4. **Cache:** `evidence_hash` = SHA-256 of month, evidence, model and prompt version. A stored `llm` row
   with the same hash means no call (also for earlier failures; `force_refresh = true` overrides).
5. **Call:** only when `dry_run = false`, the month is not cached, and the run is under `max_calls`.
   One `ai_query` query with `temperature = 0`, `max_tokens = 6000`, `failOnError => false`.
6. **Validate** every claim (below). The template output goes through the same validator.
7. **Write** with `MERGE` into `gold_llm_insights` and `gold_llm_insight_claims`.

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
- **Privacy / injection:** e-mail addresses, URLs, phone numbers, "complaint ID", "narrative", "ZIP code"
  and instruction-like text are rejected.

**Known limitation:** a claim that names two entities could swap their two numbers without being
detected (each number still belongs to a cited fact, and comparison wording is still checked).

## Storage and privacy
| Table | Grain | Content |
|---|---|---|
| `gold_llm_insights` | month × generator | status (`verified` / `rejected` / `failed`), counts, model, prompt and validator versions, evidence hash, accepted-claim summary, response SHA-256 and length, truncated error |
| `gold_llm_insight_claims` | month × generator × claim | status, reasons, fact IDs, text **for accepted claims only** |

Rejected claims keep only a SHA-256 hash, length, reasons and fact IDs; their text is stored only if
`store_rejected_text = true`. Raw model responses are never stored.

## Running (when approved)
| Goal | Widgets | Model calls | Writes |
|---|---|---|---|
| Preview prompts and plan | `dry_run = true` (default) | 0 | none |
| Template-only insights | `dry_run = false`, `max_calls = 0` | 0 | yes |
| One LLM month | `dry_run = false`, `max_calls = 1`, `months = latest` | ≤ 1 | yes |
| Rerun unchanged | same as above | 0 (cached) | no changes |

## Free Edition limits (from Databricks documentation, checked 2026-11-12)
- Free Edition is "available at no cost" under a fair-usage policy; no payment method is required.
- Numeric Free Edition limits for foundation models are not published. Exceeding the quota can shut
  workspace compute down for the rest of the day (in extreme cases the month).
- Documented limitations include "No custom models on GPU or batch inference"; whether this affects
  `ai_query` over a small table is not stated. The notebook sends one row per month called.
- No statement was found that prompts to this model are excluded from training; only aggregate,
  public statistics are sent.

## Tests
`tests/test_insights.py` (offline): evidence filtering, prompt contents and size, cache hashing,
malformed responses, prompt injection in data and in output, every rejection rule, the claim limit,
template claims for all 22 real months, storage privacy, and notebook safety defaults.
