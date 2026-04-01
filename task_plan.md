# Task Plan

## Goal
- Support per-group execution/reporting while keeping all-run.
- Expose abnormal/failed task visibility and manual retry controls on the tasks page.
- Ensure repeated failures move to a failed list and a daily email still goes out.
- Fix inconsistent network-status popups and align network health logic.

## Phases
- [in_progress] Inspect current task UI, scheduler, reporting, email, and network-check flow.
- [pending] Implement backend changes for per-group runs/reports, abnormal and failed task tracking, retry thresholds, and daily mail behavior.
- [pending] Update task page UI for group actions and abnormal/failed lists.
- [pending] Add or update tests and run verification.

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Windows sandbox CreateProcessAsUserW failed | 1 | Re-ran required read commands with escalation. |
