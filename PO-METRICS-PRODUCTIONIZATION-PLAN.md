# PO Metrics: Productionization Plan

This document focuses on the rollout plan and stacked-PR strategy.

For implementation details, walkthrough, Q&A, manual validation notes, and the metric mapping
table, see `PO-METRICS-IMPLEMENTATION-FAQ.md`.

---

## Goal

Ship the PO metrics post-commit hook as a clean set of production-ready stacked PRs with
incremental reviewability and low merge risk.

Current prototype defaults:

- no client-side enable flag,
- endpoint derived from `spark.sql.catalog.migration_bugbash.uri` + fixed API suffix,
- client timeout hardcoded to `5000` ms.

---

## Stacked PR Strategy

- Use `git stack` branches for dependency layering.
- Create PRs with the "all target master" approach.
- Use SHA-based incremental `Files changed` links in every PR description.
- Keep each layer independently understandable and testable.

Reference workflow details are in:

- `/home/murali.ramanujam/git-stack-guide.md`
- `/home/murali.ramanujam/GIT_STACK_ADDITIONAL_INFO.md`

---

## Milestones

### Milestone 1: Contract and Gating

- Confirm payload contract shape (`table_id`, `report.commit_report`,
  `file_size_histogram.commit_version`).
- Confirm always-on registration behavior for catalog-backed transactions.
- Validate endpoint derivation from catalog URI and token reuse from
  `spark.sql.catalog.<catalog>.token`.

### Milestone 2: Metric Extraction Correctness

- Isolate and review file-level metrics.
- Isolate and review row metrics (operationMetrics-first, fallback behavior).
- Isolate and review histogram generation.

### Milestone 3: Reliability and Operability

- Preserve strict best-effort behavior (never fail commit path).
- Tighten status-aware logging and timeout clarity.
- Optional follow-up: bounded retry/backoff policy for transient failures.

### Milestone 4: UC Table ID Resolution Hardening

- Preserve authoritative UC ID resolution precedence:
  1. `io.unitycatalog.tableId`
  2. `ucTableId`
  3. `storage.properties["fs.unitycatalog.table.id"]`
  4. `deltaLog.tableId` fallback
- Keep targeted tests for precedence/fallback regressions.

### Milestone 5: Tests and Docs Finalization

- Finalize unit + smoke test coverage and naming clarity.
- Keep integration validation runbook explicit (manual/staging constraints).
- Finalize reviewer docs and rollout checklist.

---

## Quality Gates Per PR

- Scope is tightly focused (one review theme per layer).
- Tests are included with the layer that changes behavior.
- No commit-path regression risk (best-effort preserved).
- Docs are updated for any behavior/config changes.

---

## Suggested Stack Layout

1. `stack/po-metrics-contract-gating`
2. `stack/po-metrics-extraction`
3. `stack/po-metrics-reliability`
4. `stack/po-metrics-uc-tableid-hardening`
5. `stack/po-metrics-tests-docs`

---

## Reviewer Checklist

- Feature gate default is off.
- Payload shape matches server contract.
- Non-2xx/timeout never fail commits.
- Table ID resolution prefers authoritative UC IDs.
- Unit/smoke coverage exists for changed behavior.
- Incremental stacked links in PR descriptions are accurate.
