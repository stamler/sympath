# Compare-By-Path Performance Notes

## Potential Scalability Regression in `compareByPath`

- Status: Addressed
- Priority: P2
- Area: UI compare
- Relevant code: `cmd/sympath/ui_compare.go`, especially `compareByPath`, `compareByPathSQLFirst`, `compareByPathInMemory`, and `shouldUseSQLFirstPathCompare`

### Summary

This issue started as a concern that `compareByPath` had regressed from a SQL-first exact-path compare into a fully materialized in-memory compare. That concern was validated by inspecting a copied local database from `~/.sympath` and benchmarking representative workloads against it.

We have since implemented and benchmarked two remediation steps:

1. a SQL-first exact-path implementation that only materializes raw-unmatched rows for normalization-aware fallback
2. an adaptive production strategy that chooses between the SQL-first path and the in-memory fully materialized path based on workload shape

That means the original concern is no longer just hypothetical, and production behavior is now both safer and closer to the older SQL-first behavior on the exact-heavy workloads that motivated the issue.

### Why This Might Be a Problem

The original risk was real:

- the in-memory implementation loads both filtered sides into memory through `loadCompareEntries`
- each loaded row keeps raw path, normalized compare path, size, and hash
- the compare then allocates additional exact-match maps, fallback group maps, and result slices
- this means heap growth tracks total filtered rows, not just the normalization-relevant remainder

### Expected Symptoms

- higher peak RSS and allocation volume on large exact-heavy compares
- extra GC pressure during compare-heavy UI sessions
- avoidable work when normalization fallback is only needed for a small unmatched subset

### Assessment Completed

Analysis was performed against a copied local database at `/tmp/4H5j2SviZF.sympath.analysis-copy`, created from `~/.sympath` before any code changes.

That copied DB was substantial and representative enough to matter:

- about `49 MB`
- `11` roots
- `11` scans
- `105,658` total `entries`
- real cross-machine media mirrors
- real Unicode normalization differences in stored paths

The benchmark harness now lives in `cmd/sympath/ui_compare_bench_test.go` and covers:

1. whole-tree exact-heavy mirror compares
2. mostly-unmatched whole-tree compares
3. normalization-heavy prefix compares
4. prefix-scoped exact-heavy remap compares

### Key Findings

Representative exact-heavy whole-tree compare:

- scan `10` (`/Volumes/ExternalSSD/media`) vs scan `15` (server `/.../media`)
- `22,605` left rows and `23,083` right rows
- `22,422` exact raw-path matches
- only `844` total raw-unmatched rows
- only `51` unique normalized fallback matches

This confirms the original concern: the fully materialized path was loading about `45k` rows to perform normalization-aware reconciliation on a very small remainder.

Initial SQL-first benchmark outcome:

- dramatically lower allocations on exact-heavy mirror workloads
- but materially slower wall-clock time than the in-memory path on this machine
- catastrophically slower on low-overlap and prefix-remapped workloads

Follow-up remediation outcome:

- the SQL-first path was reworked again to:
  - stream exact-match and raw-unmatched phase-one rows in one pass
  - replace expensive SQL subtree-blocking joins with a cheaper compare-path loading step plus in-Go blocked-dir resolution
- after that change, the exact-heavy whole-tree SQL-first compare moved into the same general range as the old pre-normalization SQL-first implementation on the same copied DB

Reference comparison on the exact-heavy mirrored workload:

- old pre-normalization SQL-first implementation: about `154-157 ms/op`
- current SQL-first implementation after remediation: about `194-214 ms/op`
- adaptive production path: about `220-261 ms/op`

This is no longer a clear architectural regression. The current implementation is somewhat slower than the old simpler SQL-first path, but it is in the same order of magnitude while preserving normalization-aware fallback and compact-view correctness.

Adaptive production strategy outcome:

- uses SQL-first only when compare prefixes align under compare semantics and exact raw-path overlap is high
- falls back to the in-memory path for low-overlap and prefix-remapped compares
- avoids the worst observed regressions while preserving the allocation win where SQL-first is most useful

### Changes Implemented

- added `compareByPathSQLFirst` for SQL-first exact-path work plus unmatched-only normalization fallback
- retained `compareByPathInMemory` as the faster path for low-overlap and prefix-remapped workloads
- made `compareByPath` adaptive via `shouldUseSQLFirstPathCompare`
- preserved compact-view correctness by ensuring subtree blocking in the SQL-first path still consults the full opposite filtered tree under compare semantics
- added benchmark coverage in `cmd/sympath/ui_compare_bench_test.go`
- kept existing compare correctness tests passing with `go test ./...`

### Resolved

- Normalization-aware fallback no longer needs to process the full filtered tree when the SQL-first path is selected.
- Compact-view correctness fixes remain in place.
- We now have representative local-data benchmarks instead of architectural guesswork.
- Production no longer blindly pays the worst cost profile for every compare workload.
- Exact-heavy raw-path compares are back in the same general performance range as the older SQL-first implementation, while preserving the newer correctness behavior.

### Remaining Notes

- Prefix-scoped exact-heavy remap compares still favor the in-memory path strongly enough that the adaptive selector routes around SQL-first entirely.
- The adaptive selector remains intentionally conservative because it is benchmark-driven, not mathematically derived.

### Current Acceptance Status

- Exact raw-path compares should scale roughly with the previous SQL-first behavior:
  - satisfied
- Normalization-aware fallback should only process rows that failed the exact raw-path phase:
  - satisfied when the SQL-first path is selected
- Compact-view behavior must remain correct for partially matched directories, file-directory conflicts, and Unicode normalization cases:
  - satisfied by the current test suite
- Existing normalization compare tests should continue to pass:
  - satisfied

### Optional Follow-Up

- investigate the prefix-scoped SQL-first pathology as a separate optimization task
- add direct tests around the adaptive strategy choice if we want to lock the benchmark-driven routing behavior in place
