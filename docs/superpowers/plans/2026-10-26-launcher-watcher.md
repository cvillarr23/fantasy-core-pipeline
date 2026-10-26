# Launcher Watcher Implementation Plan

> **For agentic workers:** This plan implements the issue body in this worktree.

**Goal:** Finalize launcher runs from Kubernetes and inline outcomes, retry retryable failures, settle timeouts and cancellation, and reap completed Jobs.

**Architecture:** Add a polling watcher with injectable Kubernetes operations and persistence through RunStore. RunStore makes terminal and retry transitions durable; the existing outbox and consumer recover retries. The watcher excludes chain-root rows, and cancellation delegates inline process termination to InlineRunner.

**Tech Stack:** Python, Kubernetes Python client, PostgreSQL, pytest.

**Spec:** `gh issue view 344 --repo cvillarr23/fantasy-core-pipeline`

## Global Constraints

- Chain roots (`chain_root_id = id`) have no Job/process and are excluded from orphan reconciliation.
- Retry attempts reuse the run row and durable outbox, incrementing `attempt`.
- Kubernetes requests retain the existing bounded request timeout.
- Keep the shipped job catalog empty.

## Review Focus

- Job list failures must defer reconciliation without falsely ending runs.
- A missing Job is orphaned only after its run deadline plus ten minutes.
- Start timeout deletes the Job and retries only when attempts remain.
- Cancellation must not be overwritten by a late Job condition.
- Retry publication must reset the existing outbox row atomically with the run row.

---

### Task 1: Durable run watcher state transitions

**Files:** `src/launcher/store.py`, `tests/test_launcher_watcher.py`

- [x] Add coverage for watcher selection, terminal finalization, retry update, and cancellation state.
- [x] Implement `RunStore` methods that query/reconcile run rows and atomically retry through `job_run_outbox`.
- [x] Run targeted watcher and outbox tests.

### Task 2: Kubernetes watcher and timeout reconciliation

**Files:** Create `src/launcher/watcher.py`, modify `src/launcher/__main__.py`, `tests/test_launcher_watcher.py`

- [x] Cover succeeded/failed Jobs, deadline timeout, pending start timeout, orphan timeout, chain-root exclusion, and Job deletion.
- [x] Implement bounded Job and Pod listing/deletion and a one-minute watcher loop.
- [x] Start and stop the watcher with the launcher process.
- [x] Run watcher and Kubernetes launcher tests.

### Task 3: Cancel inline and Kubernetes runs; reap finished Jobs

**Files:** `src/launcher/inline.py`, `src/launcher/watcher.py`, `tests/test_launcher_watcher.py`

- [x] Cover inline cancellation and cleanup of terminal Jobs.
- [x] Make inline runner terminate a selected process group and record `cancelled`.
- [x] Have watcher delete pod Jobs for cancelled rows and reap terminal Jobs after settling them.
- [x] Run focused launcher and run-log tests; the full suite was attempted but stalled on the final rerun.
