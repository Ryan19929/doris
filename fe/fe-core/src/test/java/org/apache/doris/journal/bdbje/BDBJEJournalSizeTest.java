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

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class BDBJEJournalSizeTest { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
    private static final short OP_CODE = OperationType.OP_START_ROLLUP;

    @Test
    public void testJournalSizeCountingAndBoundary() throws IOException {
        Writable writable = out -> {
            out.writeBoolean(true);
            out.writeLong(123456789L);
            Text.writeString(out, "journal size counting");
        };
        int bufferedSize = getBufferedJournalSize(writable);

        Assertions.assertEquals(bufferedSize, BDBJEJournal.countJournalSize(OP_CODE, writable));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(OP_CODE, writable, bufferedSize + 1L));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(OP_CODE, writable, bufferedSize));
        Assertions.assertTrue(BDBJEJournal.exceedMaxJournalSize(OP_CODE, writable, bufferedSize - 1L));
    }

    @Test
    public void testJournalSizeCountingPropagatesWritableException() {
        IOException expected = new IOException("expected serialization failure");
        Writable writable = out -> {
            out.writeInt(1);
            throw expected;
        };

        IOException actual = Assertions.assertThrows(IOException.class,
                () -> BDBJEJournal.exceedMaxJournalSize(OP_CODE, writable, 1024L));
        Assertions.assertSame(expected, actual);
    }

    private int getBufferedJournalSize(Writable writable) throws IOException {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(OP_CODE);
        entity.setData(writable);
        try (DataOutputBuffer buffer = new DataOutputBuffer()) {
            entity.write(buffer);
            return buffer.getLength();
        }
    }
}
