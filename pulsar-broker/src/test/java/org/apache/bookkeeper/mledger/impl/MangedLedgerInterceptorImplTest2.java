/**
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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.apache.pulsar.broker.intercept.MangedLedgerInterceptorImplTest.TestPayloadProcessor;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.broker.intercept.MangedLedgerInterceptorImplTest;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/***
 * Differ to {@link MangedLedgerInterceptorImplTest}, this test can call {@link ManagedLedgerImpl}'s methods modified
 * by "default".
 */
@Slf4j
@Test(groups = "broker")
public class MangedLedgerInterceptorImplTest2 extends MockedBookKeeperTestCase {

    public static void switchLedgerManually(ManagedLedgerImpl ledger){
        LedgerHandle originalLedgerHandle = ledger.currentLedger;
        ledger.ledgerClosed(ledger.currentLedger);
        ledger.createLedgerAfterClosed();
        Awaitility.await().until(() -> {
            return ledger.state == ManagedLedgerImpl.State.LedgerOpened && ledger.currentLedger != originalLedgerHandle;
        });
    }

    @Test
    public void testCurrentLedgerSizeCorrectIfHasInterceptor() throws Exception {
        final String mlName = "ml1";
        final String cursorName = "cursor1";

        // Registry interceptor.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(), processors);
        config.setManagedLedgerInterceptor(interceptor);
        config.setMaxEntriesPerLedger(100);

        // Add one entry.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(mlName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
        ledger.addEntry(new byte[1]);

        // Mark "currentLedgerSize" and switch ledger.
        long currentLedgerSize = ledger.getCurrentLedgerSize();
        switchLedgerManually(ledger);

        // verify.
        assertEquals(currentLedgerSize, MangedLedgerInterceptorImplTest.calculatePreciseSize(ledger));

        // cleanup.
        cursor.close();
        ledger.close();
        factory.getEntryCacheManager().clear();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }
}
