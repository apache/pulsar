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
package org.apache.bookkeeper.mledger.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

@Slf4j
public class ManagedLedgerImplUtilsTest extends MockedBookKeeperTestCase {

    @Test
    public void testGetLastValidPosition() throws Exception {
        final int maxEntriesPerLedger = 5;

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(maxEntriesPerLedger);
        managedLedgerConfig.setRetentionSizeInMB(10);
        managedLedgerConfig.setRetentionTime(5, TimeUnit.MINUTES);
        ManagedLedger ledger = factory.open("testReverseFindPositionOneByOne", managedLedgerConfig);

        String matchEntry = "match-entry";
        String noMatchEntry = "nomatch-entry";
        Predicate<Entry> predicate = entry -> {
            String entryValue = entry.getDataBuffer().toString(UTF_8);
            return matchEntry.equals(entryValue);
        };

        // New ledger will return the last position, regardless of whether the conditions are met or not.
        Position position = ManagedLedgerImplUtils.asyncGetLastValidPosition((ManagedLedgerImpl) ledger,
                predicate, ledger.getLastConfirmedEntry()).get();
        assertEquals(ledger.getLastConfirmedEntry(), position);

        for (int i = 0; i < maxEntriesPerLedger - 1; i++) {
            ledger.addEntry(matchEntry.getBytes(StandardCharsets.UTF_8));
        }
        Position lastMatchPosition = ledger.addEntry(matchEntry.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            ledger.addEntry(noMatchEntry.getBytes(StandardCharsets.UTF_8));
        }

        // Returns last position of entry is "match-entry"
        position = ManagedLedgerImplUtils.asyncGetLastValidPosition((ManagedLedgerImpl) ledger,
                predicate, ledger.getLastConfirmedEntry()).get();
        assertEquals(position, lastMatchPosition);

        ledger.close();
    }

}
