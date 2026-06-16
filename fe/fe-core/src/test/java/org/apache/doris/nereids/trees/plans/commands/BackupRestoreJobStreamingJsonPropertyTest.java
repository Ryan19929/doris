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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.backup.AbstractJob;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class BackupRestoreJobStreamingJsonPropertyTest {
    private static final LabelNameInfo LABEL = new LabelNameInfo("db1", "label1");

    @Test
    public void testBackupCommandParsesJobStreamingJsonProperty() throws Exception {
        BackupCommand disabled = new BackupCommand(LABEL, "repo", Lists.newArrayList(),
                properties("false"), false);
        analyzeBackupProperties(disabled);
        Assertions.assertEquals(Boolean.FALSE, disabled.getJobStreamingJson());

        BackupCommand enabled = new BackupCommand(LABEL, "repo", Lists.newArrayList(),
                properties("true"), false);
        analyzeBackupProperties(enabled);
        Assertions.assertEquals(Boolean.TRUE, enabled.getJobStreamingJson());

        BackupCommand defaultMode = new BackupCommand(LABEL, "repo", Lists.newArrayList(),
                properties("default"), false);
        analyzeBackupProperties(defaultMode);
        Assertions.assertNull(defaultMode.getJobStreamingJson());
    }

    @Test
    public void testRestoreCommandParsesJobStreamingJsonProperty() throws Exception {
        RestoreCommand disabled = new RestoreCommand(LABEL, "repo", Lists.newArrayList(),
                restoreProperties("false"), false);
        disabled.analyzeProperties();
        Assertions.assertEquals(Boolean.FALSE, disabled.getJobStreamingJson());

        RestoreCommand defaultMode = new RestoreCommand(LABEL, "repo", Lists.newArrayList(),
                restoreProperties("default"), false);
        defaultMode.analyzeProperties();
        Assertions.assertNull(defaultMode.getJobStreamingJson());
    }

    @Test
    public void testInvalidJobStreamingJsonProperty() throws Exception {
        BackupCommand command = new BackupCommand(LABEL, "repo", Lists.newArrayList(),
                properties("bad"), false);
        try {
            analyzeBackupProperties(command);
            Assertions.fail("expect AnalysisException");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains(AbstractJob.PROP_JOB_STREAMING_JSON), e.getMessage());
        }
    }

    private static Map<String, String> properties(String mode) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(AbstractJob.PROP_JOB_STREAMING_JSON, mode);
        return properties;
    }

    private static Map<String, String> restoreProperties(String mode) {
        Map<String, String> properties = properties(mode);
        properties.put("backup_timestamp", "2026-06-16 17:00:00");
        return properties;
    }

    private static void analyzeBackupProperties(BackupCommand command) throws Exception {
        Method method = BackupCommand.class.getDeclaredMethod("analyzeProperties");
        method.setAccessible(true);
        try {
            method.invoke(command);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof AnalysisException) {
                throw (AnalysisException) e.getCause();
            }
            throw e;
        }
    }
}
