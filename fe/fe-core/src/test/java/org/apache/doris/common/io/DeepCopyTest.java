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

package org.apache.doris.common.io;

import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.TableInfo;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeepCopyTest {

    @Test
    public void test() {
        TableInfo info = TableInfo.createForTableRename(1, 2, "newTbl");
        TableInfo copied = DeepCopy.copy(info, TableInfo.class, FeConstants.meta_version);
        Assert.assertEquals(1, copied.getDbId());
        Assert.assertEquals(2, copied.getTableId());
        Assert.assertEquals("newTbl", copied.getNewTableName());
    }

    @Test
    public void testReadOutOfMemoryIsPropagated() {
        OutOfMemoryError expected = ReadOutOfMemoryWritable.READ_FAILURE;

        OutOfMemoryError actual = Assert.assertThrows(OutOfMemoryError.class,
                () -> DeepCopy.copy(new ReadOutOfMemoryWritable(), ReadOutOfMemoryWritable.class,
                        FeConstants.meta_version));

        Assert.assertSame(expected, actual);
    }

    public static class ReadOutOfMemoryWritable implements Writable {
        private static final OutOfMemoryError READ_FAILURE = new OutOfMemoryError("expected read failure");

        @Override
        public void write(DataOutput out) throws IOException {
        }

        public static ReadOutOfMemoryWritable read(DataInput in) {
            throw READ_FAILURE;
        }
    }
}
