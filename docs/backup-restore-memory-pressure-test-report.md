# Backup/Restore Memory Optimization Pressure Test Report

## 1. Purpose

This report records the pressure test of the P1–P5 Backup/Restore memory
optimization stack. The test focuses on FE metadata serialization and replay,
not bulk data transfer throughput.

The primary questions are:

1. At what metadata size does the 8 MiB streaming buffer spill to disk?
2. How far can Backup and Restore progress with an 8 GiB FE heap?
3. Does a large snapshot remain compatible across the streaming switch?
4. Does cancellation or restart leave catalog, lock, spill-file, or repository
   state behind?

## 2. Test build and environment

| Item | Value |
|---|---|
| Date | 2026-07-28 |
| Host | `192.168.9.44` |
| Worktree | `/home/doris-integration-test` |
| Source branch | `codex/backup-restore-memory-integration` |
| Tested source commit | `fcfd5f5dadb` on the remote test worktree |
| Corresponding personal-remote commit | `f5f7e764b3b5c6c56a307f8dfdd0b9510ac8a9e8` |
| Deployment mode | Single FE + single BE, shared-nothing |
| FE heap | `-Xms8192m -Xmx8192m`, G1 |
| FE ports | HTTP 31030, query 32030, RPC 32020, edit log 32010 |
| BE ports | HTTP 31040, heartbeat 32050, BE 32060, BRPC 31060 |
| Object storage | Existing local MinIO, dedicated repository prefix per run |
| Table layout | Integer range partitions, one row per source table |
| Replica count | 1 |
| Streaming threshold | 8 MiB per serialized JSON payload |
| Default partition limit exercised | 4096 range partitions |

The host was shared with other Doris processes. The target FE and BE were
identified by their absolute output paths, and only their resource usage was
sampled.

Initial target-process state before the pressure stages:

- FE RSS: approximately 2.1 GiB.
- FE live heap: approximately 1.1 GiB.
- BE RSS: approximately 8.3 GiB.
- Host `MemAvailable`: approximately 32 GiB.
- `/home` available space: approximately 7 GiB.

## 3. Safety limits

The test deliberately measured a safe operating boundary instead of forcing an
OOM:

| Resource | Stop condition |
|---|---:|
| FE RSS | 7500 MiB |
| BE RSS | 18000 MiB |
| Host `MemAvailable` | 16 GiB |
| `/home` available space | 4 GiB |
| Single Backup or Restore | 900 seconds |

When a stop condition is met, the runner issues `CANCEL BACKUP` or
`CANCEL RESTORE`, verifies cleanup, and does not continue to a larger stage.

## 4. Workload and sampling method

Each stage performed the following operations:

1. Create a table with `N` integer range partitions.
2. Use one bucket per partition for the 512, 2048, and 4096 Tablet stages.
3. Insert one row, so Restore correctness is not inferred from metadata alone.
4. Run a streaming Backup and wait for `FINISHED`.
5. Run a legacy-path Backup of the same table and wait for `FINISHED`.
6. Restore the streaming snapshot and verify:
   - row count is 1;
   - partition count equals the requested count;
   - the Restore job reaches `FINISHED`.
7. At the next boundary, use 4096 partitions and two buckets, producing 8192
   Tablets.

Resource sampling ran every 20 ms and recorded:

- target FE and BE RSS from `/proc/<pid>/status`;
- host `MemAvailable`;
- `/home` free space;
- open FE file descriptors whose target contains
  `backup_restore_json_`;
- maximum observed spill-file size through `/proc/<fe-pid>/fd`.

The spill directory was also checked after every cancellation and final
cleanup.

## 5. Raw results

RSS values are process high-water values observed during each operation. The
stages ran sequentially, so these values include resident pages touched by
earlier stages and are not an isolated cold-start A/B comparison.

| Partitions | Buckets | Tablets | Operation | Path | Time (s) | FE RSS max (MiB) | BE RSS max (MiB) | Min host available (MiB) | Min `/home` free (GiB) | Spill max (MiB) | Result |
|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 512 | 1 | 512 | Backup | streaming | 25.981 | 2760.3 | 8725.2 | 31628.9 | 6.89 | 0 | FINISHED |
| 512 | 1 | 512 | Backup | legacy | 27.638 | 3041.7 | 8749.2 | 31284.2 | 6.88 | 0 | FINISHED |
| 512 | 1 | 512 | Restore | streaming | 30.643 | 3346.4 | 8927.3 | 30767.3 | 6.87 | 0 | FINISHED; 512 partitions; 1 row |
| 2048 | 1 | 2048 | Backup | streaming | 64.345 | 4222.7 | 9056.0 | 29704.4 | 6.82 | 0 | FINISHED |
| 2048 | 1 | 2048 | Backup | legacy | 59.033 | 4394.3 | 9104.4 | 29475.4 | 6.78 | 0 | FINISHED |
| 2048 | 1 | 2048 | Restore | streaming | 74.216 | 4488.3 | 9340.2 | 29010.1 | 6.74 | 0 | FINISHED; 2048 partitions; 1 row |
| 4096 | 1 | 4096 | Backup | streaming | 110.393 | 5340.1 | 9714.4 | 28117.8 | 6.65 | 0 | FINISHED |
| 4096 | 1 | 4096 | Backup | legacy | 108.457 | 5442.4 | 9587.5 | 27811.5 | 6.58 | 0 | FINISHED |
| 4096 | 1 | 4096 | Restore | streaming | 147.612 | 6315.0 | 9823.9 | 26831.8 | 6.50 | 15.84 | FINISHED; 4096 partitions; 1 row |
| 4096 | 2 | 8192 | Backup | streaming | 204.317 | 6538.4 | 10242.5 | 26039.3 | 6.30 | 8.48 | FINISHED |
| 4096 | 2 | 8192 | Backup | legacy | 184.073 | 7333.0 | 10189.1 | 25416.2 | 5.72 | 0 | FINISHED |
| 4096 | 2 | 8192 | Restore | streaming | not completed | 7500.5 | not retained | above 16 GiB | above 4 GiB | observed before cancellation | Safety cancellation at FE RSS limit |

DDL preparation time:

| Partitions | Buckets | Tablets | Create + insert time (s) |
|---:|---:|---:|---:|
| 512 | 1 | 512 | 3.596 |
| 2048 | 1 | 2048 | 14.726 |
| 4096 | 1 | 4096 | 31.292 |
| 4096 | 2 | 8192 | 32.528 |

## 6. Boundary observations

### 6.1 Backup boundary

- Streaming Backup completed at 8192 Tablets.
- The maximum observed Backup spill file was 8.48 MiB.
- This is direct evidence that a single Backup table-metadata payload crossed
  the 8 MiB in-memory threshold and continued through the spill path.
- The same-size legacy Backup also completed, but its sequential-process RSS
  high-water reached 7333 MiB, close to the 7500 MiB safety limit.

The timing difference between streaming and legacy paths must not be treated as
a clean performance comparison: the operations ran sequentially, object-store
state differed, and the process was not restarted between the two paths.

### 6.2 Restore boundary

- Streaming Restore completed at 4096 Tablets.
- The maximum observed Restore spill file was 15.84 MiB.
- The restored table contained 4096 partitions and the expected source row.
- An 8192 Tablet Restore was attempted after an FE restart.
- It reached 7,680,516 KiB RSS and was cancelled immediately after crossing
  the 7500 MiB safety limit.
- The FE did not crash and did not emit OOM or fatal GC diagnostics.

Therefore, for this specific 8 GiB FE heap and ASAN test deployment:

- Backup is proven through at least 8192 Tablets.
- Restore is proven through 4096 Tablets.
- 8192 Tablet Restore is outside the configured safe memory envelope.

This is an environment-specific safe boundary, not a universal Doris hard
limit.

## 7. Correctness, replay, and cleanup checks

The pressure run was preceded by the functional E2E matrix:

- legacy writer to streaming reader: passed;
- streaming writer to legacy reader: passed;
- Backup and Restore data count/checksum: passed;
- streaming Truncate: passed;
- FE restart and EditLog replay with the switches returning to `false`: passed;
- official `test_backup_restore_db` regression: passed for 10 tables;
- targeted FE tests: 71/71 passed;
- complete FE and BE build: passed.

After the pressure run:

- the safety-cancelled Restore released its job and locks;
- no pressure database remained;
- no pressure repository remained;
- no spill file remained;
- all four MinIO pressure prefixes were removed;
- FE and BE were restarted to release retained JVM/allocator pages;
- Backend returned to `Alive=true`;
- no `OutOfMemoryError`, G1 evacuation failure, to-space exhaustion, or FE
  `FATAL` record was found;
- `/home` available space recovered to approximately 17 GiB.

Post-cleanup target state:

- FE RSS: approximately 2.1 GiB;
- FE live heap: approximately 0.57 GiB;
- BE RSS: approximately 4.8 GiB;
- Backend version: `doris-0.0.0-fcfd5f5dadb`.

## 8. Limitations

1. The deployment is single-FE/single-BE and does not cover a real Cloud
   MetaService cluster.
2. The BE build uses ASAN, so absolute BE memory is not representative of a
   release build.
3. The workload stresses metadata cardinality. It does not measure large data
   object throughput, compression ratio, or network saturation.
4. Only one Backup or Restore ran at a time.
5. RSS is a process resident high-water, not an exact per-operation live-object
   measurement.
6. The 8192 Tablet Restore was intentionally cancelled at the safety threshold;
   no destructive OOM search was performed.

## 9. Baseline comparison

### 9.1 Release package and isolated deployment

The unoptimized baseline used the release package requested for this host:

| Item | Value |
|---|---|
| Download | `https://download.selectdb.com/apache-doris-4.1.3-bin-x64-noavx2.tar.gz` |
| Compressed size | 3,597,300,943 bytes |
| SHA-512 | `40254ee5201c74b89d376d23d30ecbda2f563a2509399ca98c9ed1108b2d8de6a5df650a27f6fe61be2409094f4ff70007d4fe3a0278f2195fc9998072bf1bf9` |
| Version reported by FE and BE | `doris-4.1.3-rc02-7126cf65d96` |
| Deployment directory | `/home/doris-4.1.3-baseline` |
| FE ports | HTTP 37030, query 38030, RPC 38020, edit log 38010 |
| BE ports | HTTP 37040, heartbeat 38050, BE 38060, BRPC 37060 |
| FE heap | `-Xms8192m -Xmx8192m`, G1 |
| Object storage | Same MinIO service, unique repository prefix per run |

The package URL is named 4.1.3, while its runtime version string contains
`4.1.3-rc02`. Both values are recorded to avoid silently treating the runtime
commit as a different build.

The baseline ran on the same host and used the same table schema, one source
row, replica count, MinIO service, 20 ms sampler, timeout, and safety limits as
the optimized run. It used a separate FE metadata directory, BE storage
directory, process set, and port range.

The baseline BE is a release binary, whereas the optimized BE is an ASAN
build. Absolute BE RSS and operation time are therefore recorded but are not
used as direct optimization claims. FE heap size is the same in both runs.

Baseline host state before the first measured stage:

- CPU: 32 logical CPUs, Intel Xeon E5-2470 at 2.30 GHz.
- Memory: 96,476 MiB total and 27,533 MiB available.
- Swap: disabled.
- `/home`: approximately 19.84 GiB available after package extraction.
- FE RSS: 1547.1 MiB; FE live heap: 206,214 KiB.
- BE RSS: 969.8 MiB.

### 9.2 Baseline smoke test

A 16-partition/16-Tablet Backup and Restore was run before the pressure
stages:

| Operation | Time (s) | FE RSS max (MiB) | BE RSS max (MiB) | Result |
|---|---:|---:|---:|---|
| Backup | 16.076 | 1432.5 | 958.9 | FINISHED |
| Restore | 20.118 | 1514.4 | 965.9 | FINISHED; 16 partitions; 16 Tablets; 1 row |

The smoke-test database, repository, and MinIO prefix were removed before the
measured stages.

### 9.3 Sequential baseline results

The first pass reused one FE process and increased metadata cardinality in the
same order as the optimized pressure run. Every stage used a new database,
repository, and object-store prefix, and those objects were removed when the
stage ended.

| Partitions | Buckets | Tablets | Operation | Time (s) | FE RSS at stage start (MiB) | FE RSS max (MiB) | BE RSS max (MiB) | Min host available (MiB) | Min `/home` free (GiB) | Result |
|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| 512 | 1 | 512 | Backup | 21.679 | 1554.7 | 1640.0 | 1008.8 | 27198.6 | 19.82 | FINISHED |
| 512 | 1 | 512 | Restore | 21.566 | same stage | 1733.0 | 992.2 | 27121.1 | 19.81 | FINISHED; 512 partitions; 512 Tablets; 1 row |
| 2048 | 1 | 2048 | Backup | 40.839 | 1750.0 | 3142.2 | 1122.9 | 25561.0 | 19.77 | FINISHED |
| 2048 | 1 | 2048 | Restore | 38.076 | same stage | 4155.1 | 1060.9 | 24578.1 | 19.73 | FINISHED; 2048 partitions; 2048 Tablets; 1 row |
| 4096 | 1 | 4096 | Backup | 51.689 | 4197.8 | 6937.5 | 1314.3 | 21621.4 | 19.61 | FINISHED |
| 4096 | 1 | 4096 | Restore | 57.113 | same stage | 7286.9 | 1158.4 | 21292.2 | 19.51 | FINISHED; 4096 partitions; 4096 Tablets; 1 row |
| 4096 | 2 | 8192 | Backup | 87.451 | 7292.8 | 7498.0 | 1432.4 | 20953.2 | 19.36 | FINISHED |
| 4096 | 2 | 8192 | Restore | not completed | same stage | at least 7503.0 | not retained | above 16 GiB | above 4 GiB | Safety cancellation at 7,683,088 KiB FE RSS |

DDL preparation time:

| Partitions | Buckets | Tablets | Create + insert time (s) |
|---:|---:|---:|---:|
| 512 | 1 | 512 | 1.206 |
| 2048 | 1 | 2048 | 4.016 |
| 4096 | 1 | 4096 | 8.442 |
| 4096 | 2 | 8192 | 8.922 |

After the sequential 4096-Tablet stage, FE RSS remained at 7,467,092 KiB even
though `jcmd GC.heap_info` reported only 1,035,083 KiB of used Java heap. This
large difference demonstrates why sequential RSS is a resident-page
high-water and why it must not be interpreted as the live object size of the
next operation.

### 9.4 Cold-FE boundary checks

Two additional runs restarted only the baseline FE before creating the source
table. They separate one-operation allocation pressure from the resident-page
history of the sequential pass.

| Partitions | Buckets | Tablets | Operation | Time (s) | FE RSS at stage start (MiB) | FE RSS after DDL (MiB) | FE RSS max (MiB) | Result |
|---:|---:|---:|---|---:|---:|---:|---|
| 4096 | 1 | 4096 | Backup | 59.101 | 1816.1 | 2027.0 | 3226.3 | FINISHED |
| 4096 | 1 | 4096 | Restore | 53.113 | same stage | same stage | 4699.7 | FINISHED; 4096 partitions; 4096 Tablets; 1 row |
| 4096 | 2 | 8192 | Backup | not completed | 2819.1 | 3366.8 | at least 7695.9 | Safety cancellation at 7,880,588 KiB FE RSS |
| 4096 | 2 | 8192 | Restore | not started | — | — | — | Backup did not complete |

The cold 8192-Tablet run crossed the safety threshold during Backup, before a
Restore could be submitted. The FE remained alive, accepted cleanup
statements, and contained no actual `OutOfMemoryError`, G1 evacuation failure,
to-space exhaustion, or fatal process record. The database, repository, and
MinIO prefix were removed successfully.

### 9.5 Optimized versus release boundary

The directly observed safe boundaries with an 8 GiB FE heap were:

| Path and process state | Backup boundary | Restore boundary |
|---|---|---|
| Optimized sequential run | 8192 Tablets completed; 8.48 MiB spill observed | 4096 completed; 15.84 MiB spill observed; 8192 safety-cancelled |
| 4.1.3 release sequential run | 8192 completed at 7498.0 MiB RSS | 4096 completed; 8192 safety-cancelled |
| 4.1.3 release cold-FE check | 4096 completed; 8192 Backup safety-cancelled | 4096 completed; 8192 not reached |

The optimized run proves that Backup and Restore can cross the 8 MiB payload
threshold without retaining the entire serialized JSON payload in one heap
buffer. The release cold-FE run provides an actual unoptimized failure point:
8192-Tablet Backup crossed the 7.5 GiB RSS safety line, while optimized
streaming Backup completed the 8192-Tablet stage and used an 8.48 MiB spill
file.

This is evidence of a wider safe Backup envelope in the tested setup, but it
is not a clean throughput benchmark or a universal percentage reduction:

1. The release and optimized binaries are different source commits.
2. The optimized BE is ASAN and the release BE is not.
3. The host was shared, and initial host memory and disk state differed.
4. Sequential RSS includes previously touched pages.
5. The optimized 8192 Backup result and release cold-FE 8192 result have
   different FE process histories.

Restore at 8192 Tablets remains outside the 8 GiB safe envelope in both tested
paths. Streaming removes the full JSON string/buffer peak, but it does not
remove all catalog-copy, table reconstruction, replay, and job-state memory.

### 9.6 Baseline cleanup and raw-data retention

After the baseline tests:

- every test database and repository was absent;
- all seven baseline MinIO prefixes, including the smoke-test prefix, were
  removed;
- both safety-cancelled jobs released their database state and allowed
  cleanup;
- the baseline FE and BE were stopped;
- the isolated installation, archive, metadata, and BE storage directories
  were deleted;
- `/home` available space returned from approximately 20 GiB to 29 GiB;
- the optimized test cluster and its ports were not modified.

The baseline raw-data archive contains 15 logs plus:

- the package SHA-512 file;
- a SHA-256 manifest for every raw log;
- pre-test and post-test environment snapshots;
- per-stage `CONTEXT`, `STAGE`, and `RESULT` JSON lines;
- post-stage RSS, `jcmd GC.heap_info`, disk, backend, and error-scan output.

The local archive is:

`/private/tmp/doris-4.1.3-baseline-raw`

All entries in `MANIFEST.sha256` were verified after copying the archive from
the test host.
