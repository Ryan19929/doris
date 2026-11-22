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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PartitionStateTokenLog;
import org.apache.doris.transaction.FakeTransactionIDGenerator;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

/** The changes of a table outside of a transaction that invalidate the state tokens, and the backup that names. */
public class PartitionStateTokenInvalidateTest {
    private FakeEnv fakeEnv;
    private FakeEditLog fakeEditLog;
    private FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private Env env;
    private Database db;
    private OlapTable table;
    private Partition partition;

    @BeforeEach
    public void setUp() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        env = CatalogTestUtil.createTestCatalog();
        // the logs written are verified on a mock (FakeEditLog only makes the edit log of the test catalog)
        env.setEditLog(Mockito.mock(EditLog.class));
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        FakeEnv.setEnv(env);
        db = env.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        partition = table.getPartition(CatalogTestUtil.testPartitionId1);
    }

    @AfterEach
    public void tearDown() {
        Config.restore_state_token_track = false;
    }

    private void name(String token) {
        partition.setStateToken(new PartitionStateToken(token, partition.getVisibleVersion(),
                partition.getVisibleVersionTime()));
        Assertions.assertEquals(token, PartitionStateToken.effective(partition));
    }

    @Test
    public void testBackupNamesThePartitionsWithoutToken() {
        partition.setStateToken(PartitionStateToken.invalid());
        // the tokens are not tracked: nothing is named
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNull(PartitionStateToken.effective(partition));

        Config.restore_state_token_track = true;
        env.namePartitionsWithoutStateToken(db, table);
        String named = PartitionStateToken.effective(partition);
        Assertions.assertNotNull(named);
        Assertions.assertEquals(partition.getVisibleVersion(), partition.getStateToken().getBoundVersion());
        // a partition that has a token keeps it
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertEquals(named, PartitionStateToken.effective(partition));
        // a partition that never had a token and has a visible version is named
        partition.setStateToken(null);
        Assertions.assertNull(PartitionStateToken.effective(partition));
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNotNull(PartitionStateToken.effective(partition));
        Assertions.assertNotEquals(named, PartitionStateToken.effective(partition));
        // a partition at the initial version is EMPTY and is not named
        Partition empty = addPartition(80001L);
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNull(empty.getStateToken());
        Assertions.assertEquals(PartitionStateToken.EMPTY, PartitionStateToken.effective(empty));
    }

    @Test
    public void testBackupNamesInLogsOfBoundedSize() {
        Config.restore_state_token_track = true;
        int added = 2 * Env.MAX_PARTITIONS_PER_STATE_TOKEN_LOG + 500;
        for (int i = 0; i < added; i++) {
            addPartition(80001L + i).updateVisibleVersionAndTime(2, 1000);
        }
        env.namePartitionsWithoutStateToken(db, table);
        ArgumentCaptor<PartitionStateTokenLog> logs = ArgumentCaptor.forClass(PartitionStateTokenLog.class);
        Mockito.verify(env.getEditLog(), Mockito.times(3)).logSetPartitionStateToken(logs.capture());
        int named = 0;
        for (PartitionStateTokenLog log : logs.getAllValues()) {
            Assertions.assertTrue(log.getTokens().size() <= Env.MAX_PARTITIONS_PER_STATE_TOKEN_LOG);
            named += log.getTokens().size();
        }
        // the added ones and the partition of the test table, which has a version and no token
        Assertions.assertEquals(added + 1, named);
        for (Partition p : table.getPartitions()) {
            Assertions.assertNotNull(PartitionStateToken.effective(p));
        }
    }

    @Test
    public void testReplayBindsToTheLocalVersionTime() throws Exception {
        // the master named the partition at its version time; this frontend has another one for the same version
        // (a restore and ADMIN SET PARTITION VERSION take the current time of each frontend)
        long version = partition.getVisibleVersion();
        partition.updateVisibleVersionAndTime(version, 7777L);
        Map<Long, PartitionStateToken> tokens = Maps.newHashMap();
        tokens.put(partition.getId(), new PartitionStateToken("dddd", version, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertEquals("dddd", PartitionStateToken.effective(partition));
        Assertions.assertEquals(7777L, partition.getStateToken().getBoundVersionTime());
        // at another version: no name
        tokens.put(partition.getId(), new PartitionStateToken("eeee", version - 1, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertNull(PartitionStateToken.effective(partition));
        // an invalid or tainted token as it is
        PartitionStateToken.taint(partition);
        PartitionStateToken tainted = partition.getStateToken();
        name("ffff");
        tokens.put(partition.getId(), tainted);
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // a partition that does not exist is skipped, not the others
        tokens.clear();
        tokens.put(99999L, new PartitionStateToken("gggg", version, 1234L));
        tokens.put(partition.getId(), new PartitionStateToken("hhhh", version, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertEquals("hhhh", PartitionStateToken.effective(partition));
    }

    // a new partition of the test table, at the initial version
    private Partition addPartition(long id) {
        Partition p = new Partition(id, "p" + id, new MaterializedIndex(id + 100000L,
                MaterializedIndex.IndexState.NORMAL), new RandomDistributionInfo(1));
        table.addPartition(p);
        return p;
    }

    @Test
    public void testReplayOfTheNames() throws Exception {
        Map<Long, PartitionStateToken> tokens = Maps.newHashMap();
        tokens.put(partition.getId(), new PartitionStateToken("cccc", partition.getVisibleVersion(),
                partition.getVisibleVersionTime()));
        // through the serialized form, as a follower reads it
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new PartitionStateTokenLog(db.getId(), table.getId(), tokens).write(new DataOutputStream(bytes));
        PartitionStateTokenLog log = PartitionStateTokenLog.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertEquals(db.getId(), log.getDbId());
        Assertions.assertEquals(table.getId(), log.getTableId());

        partition.setStateToken(PartitionStateToken.invalid());
        env.replaySetPartitionStateToken(log);
        Assertions.assertEquals("cccc", PartitionStateToken.effective(partition));
    }
}
