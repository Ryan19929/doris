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

package org.apache.doris.journal.bdbje;

import org.apache.doris.catalog.Env;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.RollbackException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class BDBJournalCursorTest {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironmentTest.class);
    private static List<String> tmpDirs = new ArrayList<>();

    public static String createTmpDir() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        Path mockDir = Paths.get(dorisHome, "fe", "mocked");
        if (!Files.exists(mockDir)) {
            Files.createDirectories(mockDir);
        }
        UUID uuid = UUID.randomUUID();
        File dir = Files.createDirectories(Paths.get(dorisHome, "fe", "mocked", "BDBEnvironmentTest-" + uuid.toString())).toFile();
        if (LOG.isDebugEnabled()) {
            LOG.debug("createTmpDir path {}", dir.getAbsolutePath());
        }
        tmpDirs.add(dir.getAbsolutePath());
        return dir.getAbsolutePath();
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (String dir : tmpDirs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("deleteTmpDir path {}", dir);
            }
            FileUtils.deleteDirectory(new File(dir));
        }
    }

    private int findValidPort() {
        int port = 0;
        for (int i = 0; i < 65535; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    LOG.info("The port {} is invalid and try another port", port);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port");
            }
        }
        Preconditions.checkArgument(((port > 0) && (port < 65536)));
        return port;
    }

    @RepeatedTest(1)
    public void testNormal() throws Exception {
        Assertions.assertTrue(BDBJournalCursor.getJournalCursor(null, -1, 20) == null);
        Assertions.assertTrue(BDBJournalCursor.getJournalCursor(null, 21, 20) == null);

        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        if (LOG.isDebugEnabled()) {
            LOG.debug("selfNodeName:{}, selfNodeHostPort:{}", selfNodeName, selfNodeHostPort);
        }

        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(new File(createTmpDir()), selfNodeName, selfNodeHostPort, selfNodeHostPort);

        Database db = bdbEnvironment.openDatabase("1");
        db.close();

        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(bdbEnvironment, 1, 10);
        Assertions.assertTrue(bdbJournalCursor != null);
        Assertions.assertTrue(bdbJournalCursor.next() == null);

        bdbEnvironment.close();

        bdbJournalCursor = BDBJournalCursor.getJournalCursor(bdbEnvironment, 1, 10);
        Assertions.assertTrue(bdbJournalCursor == null);
    }

    @RepeatedTest(1)
    public void testGetJournalCursorNotSwallowRestartRequiredException() {
        BDBEnvironment env = Mockito.mock(BDBEnvironment.class);
        RollbackException rollbackEx = Mockito.mock(RollbackException.class);
        Mockito.when(env.getDatabaseNames()).thenThrow(rollbackEx);

        // Mock a checkpoint thread so that exitOnRestartRequired() rethrows the exception
        // instead of exiting the test JVM. The point is: RestartRequiredException must NOT be
        // swallowed by the generic catch and turned into a null cursor.
        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::isCheckpointThread).thenReturn(true);
            Assertions.assertThrows(RollbackException.class,
                    () -> BDBJournalCursor.getJournalCursor(env, 1, 10));
        }
    }

    @RepeatedTest(1)
    public void testGetJournalCursorNullOnInsufficientLogException() {
        BDBEnvironment env = Mockito.mock(BDBEnvironment.class);
        InsufficientLogException insufficientLogEx = Mockito.mock(InsufficientLogException.class);
        Mockito.when(env.getDatabaseNames()).thenThrow(insufficientLogEx);

        // Keep the legacy behavior: a null cursor, then the replayer will retry in the next
        // cycle and trigger NetworkRestore in BDBJEJournal.getDatabaseNames().
        Assertions.assertNull(BDBJournalCursor.getJournalCursor(env, 1, 10));
    }

    @RepeatedTest(1)
    public void testNextNotSwallowRestartRequiredException() throws Exception {
        BDBEnvironment env = Mockito.mock(BDBEnvironment.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(env.getDatabaseNames()).thenReturn(Arrays.asList(1L));
        Mockito.when(env.openDatabase("1")).thenReturn(database);

        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getForceSkipJournalIds()).thenReturn(new ArrayList<>());

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            mockedEnvStatic.when(Env::isCheckpointThread).thenReturn(true);

            BDBJournalCursor cursor = BDBJournalCursor.getJournalCursor(env, 1, 10);
            Assertions.assertNotNull(cursor);

            // RestartRequiredException must not be swallowed and turned into a null entry
            RollbackException rollbackEx = Mockito.mock(RollbackException.class);
            Mockito.doThrow(rollbackEx).when(database)
                    .get(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Assertions.assertThrows(RollbackException.class, cursor::next);

            // While InsufficientLogException keeps the legacy behavior: return null and let the
            // replayer trigger NetworkRestore in the next cycle.
            InsufficientLogException insufficientLogEx = Mockito.mock(InsufficientLogException.class);
            Mockito.doThrow(insufficientLogEx).when(database)
                    .get(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Assertions.assertNull(cursor.next());
        }
    }
}
