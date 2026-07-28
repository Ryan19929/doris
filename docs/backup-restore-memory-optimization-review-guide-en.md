# Backup/Restore Memory Optimization: PR1–PR5 Reviewer Guide

## Purpose

This document explains the design and review boundaries of the five PRs that reduce FE memory amplification in
large BACKUP/RESTORE persistence. It focuses on correctness, on-disk compatibility, rollback behavior, and claims
that each PR may or may not make.

Detailed execution records and benchmark methodology are maintained in
`backup-restore-memory-optimization-development-plan.md`.

## Background and provenance

This is neither a single-point OOM fix nor a redesign of the BACKUP/RESTORE file format. With a large tablet
count, the same FE metadata passes through catalog deep copy, BackupJob/RestoreJob persistence, edit-log size
preflight, checkpoint/image persistence, and BackupMeta file output. The legacy path may retain Replica objects,
a complete JSON DOM, a complete JSON String, and an additional byte buffer at different stages. Consequently,
transient allocation or Old Gen pressure may trigger Full GC or OOM before the final payload itself approaches
the FE heap limit.

The original branch-3.1 investigation covered RestoreJob workloads with more than 50,000 tablets and a production
BACKUP scale of roughly two million tablets with three replicas each. At the latter scale, extrapolation from the
measured legacy payload indicates that a single edit-log entry may exceed BDBJE's one-GiB journal limit. These
numbers explain the motivation; they are not per-PR performance claims for the master branches.

The design evolved through three pull requests:

1. [HYDCP/hy-doris#49](https://github.com/HYDCP/hy-doris/pull/49) first optimized RestoreJob on branch-3.1.
   It introduced streaming Gson paths for `snapshotInfos`, `restoredVersionInfo`, and job polymorphic dispatch,
   then validated cross-mode reads and restart replay. It is the main predecessor of PR3 and PR4.
2. [HYDCP/hy-doris#63](https://github.com/HYDCP/hy-doris/pull/63) extended the branch-3.1 work to BACKUP and
   combined Replica stripping, table-metadata streaming, length-prefixed I/O, and journal size counting. It
   provides end-to-end evidence, but changes too many persistence layers to be an appropriate single master
   review unit.
3. [apache/doris#65321](https://github.com/apache/doris/pull/65321) is the first master-targeted Draft. It
   deliberately keeps only Replica stripping from the detached BackupMeta copy. The remaining work is being
   reshaped as PR1 through PR5 instead of copying #63 wholesale.

The split follows correctness boundaries rather than line count. PR1 must prove that omitted data is not consumed
by RESTORE. PR2 must prove that counting is byte-equivalent to a real journal write. PR3 must preserve JSON schema
and compatibility fallback in shared adapters. PR4 must preserve RestoreJob replay. PR5 must preserve the outer
byte format while bounding reads and managing spill-file lifetime. This makes every change independently
reviewable, testable, and revertible.

The review should establish three outcomes: BACKUP/RESTORE semantics remain unchanged; existing journal, image,
and BackupMeta data remain readable across config mismatch and rollback; and each PR claims only the memory
amplification layer that it actually removes. Changing the BE snapshot format, SQL surface, or job state machine is
out of scope. Default enablement is deferred to PR6. A true CloudRestoreJob plus MetaService end-to-end run remains
optional follow-up validation and is not claimed as completed.

## Review branch snapshot

The following personal-remote heads are the 2026-07-28 review snapshot. They are logical-review inputs, not
merge-ready claims:

| Planned PR | Branch | Snapshot commit | Dependency |
| --- | --- | --- | --- |
| PR1 | `codex/backup-strip-replica-info` | `3ec7ff3f374` | None |
| PR2 | `codex/backup-journal-size-counting` | `f71760e5d7a` | None |
| PR3 | `codex/streaming-gson-foundation` | `bdacd53ee31` | None |
| PR4 | `codex/restore-job-streaming` | `e599cf60f1b` | PR3 |
| PR5 | `codex/backup-meta-streaming` | `a3a5578602e` | PR1, PR3, PR4 |

At this snapshot, all five branches are 75 commits behind the locally tracked `upstream/master`. Reviewers can
evaluate design boundaries and implementation logic now, but final per-PR diffs, CI, and merge decisions must use
the rebased heads. PR1 also contains a global DeepCopy `Error` propagation change that should preferably become a
separate prerequisite PR. The `backup-strip-replica-info` head used by Apache Draft #65321 is a different,
not-yet-updated remote branch and should not be confused with the `codex/` review snapshot.

## Design map

```text
live catalog / BackupJob / RestoreJob
        |
        | PR1: remove Replica objects that RESTORE never consumes
        v
retained persistence object graph
        |
        | PR3/PR4: stream Guava collections and polymorphic dispatch without a full JsonElement DOM
        | PR5: preserve length-prefixed UTF-8 while avoiding a full String/byte[] in heap
        v
journal / image / BackupMeta payload
        |
        | PR2: count the preflight size without retaining a second full payload buffer
        v
BDBJE write
```

| PR | Responsibility | Dependency |
| --- | --- | --- |
| PR1 | Omit source Replica objects from detached backup table metadata | None |
| PR2 | Count JournalEntity bytes through a null sink | None |
| PR3 | Add streaming Guava and polymorphic Gson adapter infrastructure | None |
| PR4 | Apply PR3 to large RestoreJob fields and job subtype dispatch | PR3 |
| PR5 | Add length-prefixed streaming I/O, bounded reads, spill, and table/backup persistence migration | PR1, PR3, PR4 |

PR1 and PR2 can be reviewed independently. The required stacked order is PR3, then PR4, then PR5. Enabling the
streaming configs by default is intentionally deferred to a separate PR6 decision.

## Compatibility invariants

All five PRs must preserve the following properties:

1. No BACKUP/RESTORE state-machine or BE snapshot-format change.
2. No meta-version bump and no new journal opcode.
3. The outer format remains a four-byte signed length followed by UTF-8 JSON.
4. Existing field names, subtype labels, compatible labels, and default-subtype behavior remain readable.
5. Legacy and streaming writers/readers work in all four combinations.
6. A streaming Leader can be replayed by a legacy-config Follower or Observer.
7. A streaming checkpoint image remains loadable after the runtime configs return to false.
8. Invalid lengths, truncated input, serialization errors, and spill failures fail explicitly.
9. Spill files are removed on success and on every failure path.
10. Each PR remains independently reviewable and revertible.

## PR1: omit Replica objects from BackupMeta

BACKUP uses live tablets to select a source Replica for BE snapshot tasks. Separately, it creates a detached table
copy for BackupMeta. PR1 clears Replica objects only from the detached copy when
`backup_meta_reserve_replica_info=false`.

RESTORE does not reuse those objects. `OlapTable.resetIdsForRestore()` replaces backed-up tablets, assigns new IDs,
and creates new Replica objects from ReplicaAllocation and target-cluster backends. The RESTORE property
`reserve_replica` preserves ReplicaAllocation; it does not preserve individual Replica IDs or backend assignments.

Review points:

- verify that `clearReplicasForBackup()` is only called on the detached copy;
- verify that live Replica selection for SnapshotTask is unchanged;
- verify LocalTablet plus both current and legacy CloudTablet fields;
- verify that tablet topology, ordering, IDs, and ReplicaAllocation remain intact;
- do not claim that PR1 removes the initial `DeepCopy.copy()` peak: stripping occurs after that copy.

The fallback config restores the old metadata content. The schema and meta version are unchanged.

Open decision: the current PR1 branch also propagates reflected `Error` causes from the global DeepCopy utility.
That fix is valid, but affects every DeepCopy caller. The preferred review shape is a separate prerequisite PR.

## PR2: count journal size without buffering

The previous preflight check serialized a complete JournalEntity into `DataOutputBuffer` only to inspect its size.
PR2 writes the same JournalEntity into `CountingDataOutputStream(OutputStream.nullOutputStream())`.

Because the same `JournalEntity.write()` method executes, the count includes the opcode, length prefixes, and body.
The one-GiB rule remains strictly `size > limit`; equality is accepted. The counter is a long and serialization
IOExceptions are propagated.

Review points:

- compare the count against an actually buffered JournalEntity;
- test below/equal/above-limit boundaries;
- ensure the sink retains no payload;
- ensure Writable failures remain failures.

A 512-MiB isolated benchmark produced exactly 536,870,914 bytes in both modes. The buffered mode retained about
537.9 MB; the counting mode retained about 7.7 KB. This result measures only the removed preflight buffer.

## PR3: streaming Gson infrastructure

PR3 replaces tree-based Guava Table/Multimap handling with TypeAdapters that read and write directly through
JsonReader/JsonWriter while preserving the existing JSON shape.

For polymorphic values, the legacy factory materializes a JsonElement tree to inject or find the type field. The
streaming writer uses a `TypeFieldInjectingJsonWriter` and emits the type as the first field. Canonical type-first
input then uses `EnteredObjectJsonReader` without materializing a DOM. Historical input with a non-first or missing
type field intentionally falls back through `readLegacyObject()` and materializes that one object for compatibility.
The reader must still support compatible labels and explicit failures for unknown/duplicate types, malformed
values, and truncated JSON. Compatibility with non-canonical input is not claimed to be a zero-DOM path.

High-cardinality metadata exposed two allocation hazards. The final implementation therefore:

- reuses reader/writer wrappers in a per-thread, per-root-stream, LIFO pool capped at 64 cached nesting levels;
- clears payload references on release and weakly references the root stream;
- probes optional Gson 2.10/2.11 stream settings once, caching a Method or no-op copier instead of creating a
  NoSuchMethodException for every metadata object.

Review points:

- config false must use the original tree delegate;
- the type-first streaming path and the non-first/default-subtype legacy fallback must both preserve every field;
- wrappers must be released after success and exceptions;
- the `JsonReaderInternalAccess` hook must only unwrap the custom reader;
- declared byte-compatible paths must compare raw output bytes, not only round-tripped objects.

PR3 changes no Backup/Restore business entry point.

## PR4: stream large RestoreJob fields

PR4 applies PR3 to `snapshotInfos`, `restoredVersionInfo`, and AbstractJob subtype dispatch. The internal volatile
config `enable_backup_restore_job_streaming_json` selects the streaming or legacy adapters for both writes and
reads. The config is an implementation choice, not a format bit.

PR4 still uses the existing outer Text/String persistence entry point. Its scope is to remove the Guava and
polymorphic JsonElement DOMs. PR5 later removes the outer full String.

Review points:

- cover every active and terminal RestoreJob state;
- test legacy/streaming writer and reader in all four combinations;
- test JournalEntity, EditLog replay, BackupHandler replay, and image load rather than only direct Gson calls;
- test streaming Leader to legacy Follower/Observer replay and config reset after restart;
- keep compressed-job and CloudRestoreJob subtype behavior.

For a 79.7-MB RestoreJob containing 600,000 snapshot mappings and 75,000 version mappings, three-fork medians show
an approximately 80.17% lower sampled reader peak and 73.86% lower elapsed time than the legacy reader. Retained
heap is effectively unchanged, which is expected because the final RestoreJob graph is identical.

## PR5: stream table metadata and outer persistence

### Length-prefixed writer

The destination DataOutput usually cannot seek back to fill in the length. Keeping the complete payload in a byte
array recreates the heap problem, while count-then-serialize performs two traversals and can observe a mutable job in
two different states.

PR5 serializes once into a spillable buffer:

1. keep the first 8 MiB in memory;
2. spill larger content under `Config.tmp_dir/backup_restore_json_spill`;
3. after serialization succeeds, write the exact int length and replay the payload in 64-KiB chunks;
4. delete the spill on close and attach cleanup failures as suppressed exceptions.

The eight-MiB value is a memory threshold, not a payload limit. The persisted length remains bounded by the signed
int format.

### Bounded reader

The reader exposes only the declared payload length to Gson and does not allocate a payload-sized contiguous byte
array. Negative lengths, truncation, and unconsumed payload bytes are explicit failures. Closing the bounded reader
does not close the journal/image owner's DataInput.

### Migrated paths

PR5 applies the helper to Table, OlapTable, BackupMeta, BackupJobInfo, AbstractJob, BackupJob, and RestoreJob. It
enables PR3 streaming dispatch for Table/Partition/Tablet/Replica and uses spillable streaming deep copy from
`OlapTable.selectiveCopy()` when `enable_table_meta_streaming_json=true`.

Review points:

- compare raw bytes with `Text.writeString()` for byte-compatible paths;
- preserve and detect the compressed-job marker;
- serialize exactly once and avoid touching the destination before serialization succeeds;
- verify spill cleanup after serializer, destination, directory, and close failures;
- verify config false uses the original Text/String/DeepCopy paths;
- test BackupMeta, BackupJobInfo, journal, image, and selective-copy matrices separately;
- ensure PR1 and PR4 logic is reused rather than duplicated.

## Cross-version and runtime matrix

| Writer | Reader | Required result |
| --- | --- | --- |
| Legacy Text/tree | Legacy | Baseline unchanged |
| Legacy Text/tree | Streaming | Success |
| Streaming length-prefix/adapters | Legacy Text/tree | Success |
| Streaming length-prefix/adapters | Streaming | Success |
| Streaming Leader | Legacy-config Follower/Observer | Live replay succeeds |
| Streaming checkpoint | Restart with configs false | Image load succeeds |
| Compressed RestoreJob | Both readers | Marker and compressed body succeed |
| Replica-free BackupMeta | Both table readers | Topology remains, Replica list is empty |

Tests must validate semantic state, mapping counts, tablet order, ReplicaAllocation, restored data, and spill-file
cleanup. Successful deserialization alone is insufficient.

## Validation summary

- Focused Linux FE tests after the 2026-07-20 master rebase: PR1 16/16, PR2 2/2, PR3 20/20, PR4 7/7,
  PR5 27/27. All five Maven reactors completed and Checkstyle reported no violations.
- MinIO BACKUP/RESTORE, partition, MV/rollup, dynamic partition, colocate, and Replica config paths passed.
- CREATING, SNAPSHOTING, DOWNLOADING, and COMMITTING restart recovery reached FINISHED.
- Streaming Leader to legacy Follower/Observer replay, cross-config checkpoint load, and failover passed.
- Repeated 200,000-tablet benchmarks, a 2,000,000-tablet capacity matrix, and spill-residue checks passed.
- Cloud metadata subtypes have unit compatibility coverage. A real CloudRestoreJob + MetaService E2E is not yet
  claimed.
- Split-PR Apache CI is still pending; combined-branch evidence is not a substitute for each PR's CI.

## Recommended review and merge order

1. PR1: prove that Replica objects are unused and that only detached copies are mutated.
2. PR2: verify exact byte counting and failure propagation.
3. PR3: review JSON shape, subtype replay, wrapper lifetime, and Gson-version behavior.
4. PR4: review RestoreJob state/replay compatibility and node-local config mismatch.
5. PR5: review byte compatibility, single-pass spill, bounded input, and migrated call sites.
6. After PR1–PR5 and CI, decide separately whether PR6 should enable both streaming configs by default.

## Decisions requested from reviewers

1. Split the global DeepCopy Error propagation from PR1, or keep it with an explicitly broader scope?
2. Accept PR3–PR5 with streaming disabled by default and enable later in PR6?
3. Require real CloudRestoreJob + MetaService E2E before merge?
4. Keep rollback configs internal, or document them as supported operational controls?
5. Which benchmark results are accepted as performance claims versus capacity evidence only?

## Review conclusion

The five-PR decomposition follows the actual sources of amplification: retained data, duplicate preflight buffering,
tree adapters, RestoreJob application, and outer table/job persistence. No design-level correctness blocker has been
identified. Remaining merge work is per-PR history/CI, the PR1 DeepCopy scope decision, the default-enable policy,
and optional real Cloud E2E evidence.
