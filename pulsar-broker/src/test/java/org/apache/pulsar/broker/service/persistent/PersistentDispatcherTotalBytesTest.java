/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import java.util.ArrayList;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentDispatcherTotalBytesTest {

    @DataProvider(name = "entryCounts")
    public Object[][] entryCounts() {
        return new Object[][] {
                {0},
                {1},
                {32},
                {1024}
        };
    }

    @Test(dataProvider = "entryCounts")
    public void testGetTotalBytesSize(int entryCount) {
        EntriesAndExpectedSize entries = entriesWithVaryingPayloadSizes(entryCount);
        try {
            assertEquals(AbstractPersistentDispatcherMultipleConsumers.getTotalBytesSize(entries.entries),
                    entries.expectedSize);
        } finally {
            entries.release();
        }
    }

    private static EntriesAndExpectedSize entriesWithVaryingPayloadSizes(int entryCount) {
        EntriesAndExpectedSize entries = new EntriesAndExpectedSize(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int payloadSize = payloadSize(i);
            entries.entries.add(EntryImpl.create(1, i, new byte[payloadSize]));
            entries.expectedSize += payloadSize;
        }
        return entries;
    }

    private static int payloadSize(int index) {
        return index % 97 == 0 ? 4096 : 1 + ((index * 31) & 1023);
    }

    private static final class EntriesAndExpectedSize {
        private final ArrayList<Entry> entries;
        private long expectedSize;

        private EntriesAndExpectedSize(int entryCount) {
            this.entries = new ArrayList<>(entryCount);
        }

        private void release() {
            entries.forEach(Entry::release);
        }
    }
}
