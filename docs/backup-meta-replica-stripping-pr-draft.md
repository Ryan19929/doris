# PR1 Draft: Strip Replica Objects from Backup Metadata

This file is the proposed text for updating apache/doris#65321. It assumes the global `DeepCopy` Error propagation
commit is split into a separate prerequisite PR. If that commit remains in PR1, the PR scope and release note must
also describe the global behavior change.

The reviewer-facing explanation for the complete PR1–PR5 series is in
`backup-restore-memory-optimization-review-guide-en.md`.

## Suggested title

```text
[improvement](backup) Reduce backup metadata size by omitting replicas
```

## Suggested PR body

### What problem does this PR solve?

Issue Number: N/A

Related PR: https://github.com/HYDCP/hy-doris/pull/63

Problem Summary:

Backup metadata stores a detached copy of each backed-up table. Because the full catalog object graph is copied,
every tablet's `Replica` objects are retained and serialized through the following hierarchy:

```text
OlapTable -> Partition -> MaterializedIndex -> Tablet -> Replica
```

These Replica objects are not used by RESTORE. Snapshot tasks select a source Replica from the live catalog before
the backup metadata copy is prepared. During RESTORE, Doris replaces the backed-up tablets, assigns new IDs, and
creates new Replica objects from the restored `ReplicaAllocation` and the target cluster's available backends.

For tables with many tablets and replicas, retaining the source Replica objects therefore increases BackupMeta,
BackupJob journal, and FE image size without changing restore behavior.

This PR changes only the detached table copy used by BACKUP:

- omit Replica objects from LocalTablet and CloudTablet by default;
- preserve tablet topology, IDs, ordering, partition metadata, and ReplicaAllocation;
- keep the live catalog and snapshot-task Replica selection unchanged;
- retain the old behavior behind the mutable, master-only FE config
  `backup_meta_reserve_replica_info=true`;
- cover LocalTablet and CloudTablet metadata round trips and a BACKUP/RESTORE-to-alias path using
  `reserve_replica=true`.

The serialized schema and meta version are unchanged. Old backups containing Replica objects remain readable, and
the fallback config can make new backups retain Replica objects again.

In a 200,000-tablet, 3-replica capacity benchmark, stripping Replica objects reduced the BackupJob journal payload
from approximately 119 MB to 23 MB (about 81%). This result measures the retained/persisted object graph. The
current implementation strips Replica objects after `DeepCopy.copy()` completes, so this PR does not eliminate the
initial deep-copy peak and does not claim to solve the complete large-BACKUP OOM problem by itself.

### Release note

Reduce FE retained metadata and persisted backup metadata size by omitting source Replica objects that RESTORE does
not use. Set `backup_meta_reserve_replica_info=true` to retain the previous metadata content.

### Check List (For Author)

- Test
    - [ ] Regression test
        - Added `test_backup_restore_alias`; execution is pending Apache regression CI.
    - [x] Unit Test
        - `BackupMetaTest`
        - `OlapTableTest`
        - `CloudTabletTest`
        - 14/14 focused Replica-stripping FE test executions passed on Linux.
    - [ ] Manual test
    - [ ] No need to test or manual test.

- Behavior changed:
    - [x] Yes. New backup metadata omits source Replica objects by default. The previous behavior can be restored
      dynamically with `backup_meta_reserve_replica_info=true`.
    - [ ] No.

- Does this need documentation?
    - [x] No. This changes internal backup metadata content and adds an internal FE fallback config; it does not add
      a SQL syntax or user-facing property.
    - [ ] Yes.

### Additional validation

- Maven checkstyle passed.
- The 25-module FE Maven reactor completed successfully.
- `git diff --check` passed.
- Full Apache CI and the `backup_restore` regression suite are still pending.

### Check List (For Reviewer who merge this PR)

- [ ] Confirm the release note
- [ ] Confirm test cases
- [ ] Confirm document
- [ ] Add branch pick label

## Before updating the Draft PR

1. Decide whether to split the global `DeepCopy` Error propagation commit.
2. Rebase the PR head onto the current `upstream/master`.
3. Run the focused FE UTs again after the final history is fixed.
4. Run `test_backup_restore_alias` with `run-regression-test.sh` or wait for its Apache CI result.
5. Replace the Draft PR title/body only after the branch and test list match this text.
