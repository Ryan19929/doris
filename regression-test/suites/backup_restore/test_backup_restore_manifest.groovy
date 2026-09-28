// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import groovy.json.JsonSlurper

// A backup records the manifest of each tablet snapshot (file names, sizes and SHA-256), and the restore checks
// the downloaded snapshots against it on BE. The result is shown in the ManifestCheck column of SHOW RESTORE.
suite("test_backup_restore_manifest", "backup_restore") {
    String suiteName = "test_backup_restore_manifest"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    int numPartitions = 3
    int numBuckets = 2
    // replication_num is 1 and reserve_replica is set, each tablet is downloaded once
    int numTablets = numPartitions * numBuckets
    int numRowsPerPartition = 5
    int numRows = numPartitions * numRowsPerPartition

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def createTable = { String db ->
        sql "DROP TABLE IF EXISTS ${db}.${tableName} FORCE"
        sql """
            CREATE TABLE ${db}.${tableName} (
                `id` INT NOT NULL,
                `value` VARCHAR(32) NULL
            )
            DUPLICATE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS ${numBuckets}
            PROPERTIES
            (
                "replication_num" = "1"
            )
            """
        List<String> values = []
        for (int p = 0; p < numPartitions; ++p) {
            for (int i = 0; i < numRowsPerPartition; ++i) {
                int id = p * 10 + i
                values.add("(${id}, 'v${id}')")
            }
        }
        sql "INSERT INTO ${db}.${tableName} VALUES ${values.join(",")}"
        // a second rowset in p1
        sql "INSERT INTO ${db}.${tableName} VALUES (1, 'again')"
        sql "sync"
    }

    def assertRowCount = { String db, int expected ->
        def result = sql "SELECT COUNT(*) FROM ${db}.${tableName}"
        assertEquals(expected, result[0][0] as int)
    }

    def lastRestoreJob = { String db ->
        def restoreResult = sql_return_maparray """ SHOW RESTORE FROM ${db} """
        def restoreJob = restoreResult.last()
        logger.info("restore job of ${db}: ${restoreJob}")
        assertEquals("FINISHED", restoreJob.State)
        return restoreJob
    }

    def manifestCheck = { restoreJob ->
        String shown = restoreJob.ManifestCheck as String
        assertTrue(shown != null && shown != "\\N", "ManifestCheck: ${shown}")
        return new JsonSlurper().parseText(shown)
    }

    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    createTable(srcDbName)

    // 1. Repository path: the digests are computed when uploading.
    sql """
        BACKUP SNAPSHOT ${srcDbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """
    syncer.waitSnapshotFinish(srcDbName)
    String timestamp = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(timestamp != null)

    def restoreFromRepo = {
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON (${tableName})
            PROPERTIES
            (
                "backup_timestamp" = "${timestamp}",
                "reserve_replica" = "true"
            )
        """
        syncer.waitAllRestoreFinish(dbName)
        def restoreJob = lastRestoreJob(dbName)
        assertRowCount(dbName, numRows + 1)
        return manifestCheck(restoreJob)
    }

    // 1.1 default: the set of files and the sizes are checked for all tablets, the digests are not.
    def check = restoreFromRepo()
    assertEquals(1, check.version)
    assertEquals("sha256", check.algo)
    assertEquals(numTablets, check.verified_tablets)
    assertEquals(0, check.unverified_tablets)
    assertEquals(false, check.digest_checked)

    // 1.2 with the digest check.
    setBeConfigTemporary([restore_manifest_digest_check: true]) {
        check = restoreFromRepo()
        assertEquals(numTablets, check.verified_tablets)
        assertEquals(0, check.unverified_tablets)
        assertEquals(true, check.digest_checked)
    }

    // 1.3 the check disabled on BE: nothing is verified, and the restore still succeeds as before.
    setBeConfigTemporary([restore_manifest_check: false]) {
        check = restoreFromRepo()
        assertEquals(0, check.verified_tablets)
        assertEquals(numTablets, check.unverified_tablets)
        assertEquals(false, check.digest_checked)
    }

    // 2. Http path (a backup kept on local, as CCR does), restored into the suite db.
    String localDb = context.dbName
    createTable(localDb)
    def restoreLocalSnapshot = { String label, String properties ->
        sql """
            BACKUP SNAPSHOT ${localDb}.${label}
            TO `__keep_on_local__`
            ON (${tableName})
            ${properties}
        """
        syncer.waitSnapshotFinish(localDb)
        assertTrue(syncer.getSnapshot(label, tableName))
        assertTrue(syncer.restoreSnapshot())
        syncer.waitAllRestoreFinish(localDb)
        def restoreJob = lastRestoreJob(localDb)
        assertRowCount(localDb, numRows + 1)
        return manifestCheck(restoreJob)
    }

    // 2.1 default: sizes only.
    check = restoreLocalSnapshot("${suiteName}_local_1", "")
    assertEquals(1, check.version)
    assertEquals("none", check.algo)
    assertEquals(numTablets, check.verified_tablets)
    assertEquals(0, check.unverified_tablets)

    // 2.2 manifest_digest: the snapshot computes the digests, and the files linked from the local tablet instead of
    // downloading are checked too.
    setBeConfigTemporary([restore_manifest_digest_check: true]) {
        check = restoreLocalSnapshot("${suiteName}_local_2", """PROPERTIES ("manifest_digest" = "true")""")
        assertEquals("sha256", check.algo)
        assertEquals(numTablets, check.verified_tablets)
        assertEquals(0, check.unverified_tablets)
        assertEquals(true, check.digest_checked)
    }

    // 3. SHOW BRIEF RESTORE has no ManifestCheck.
    def brief = sql_return_maparray """ SHOW BRIEF RESTORE FROM ${dbName} """
    assertFalse(brief.last().containsKey("ManifestCheck"))

    sql "DROP TABLE ${localDb}.${tableName} FORCE"
    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
