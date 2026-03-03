
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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test class to demonstrate bugs in PersistentMessageExpiryMonitor using mocks.
 */
public class PersistentMessageExpiryMonitorMockTest {

    private PersistentTopic mockTopic;
    private ManagedCursor mockCursor;
    private ManagedLedger mockManagedLedger;
    private BrokerService mockBrokerService;

    @BeforeMethod
    public void setup() {
        mockTopic = mock(PersistentTopic.class);
        mockCursor = mock(ManagedCursor.class);
        mockManagedLedger = mock(ManagedLedger.class);
        mockBrokerService = mock(BrokerService.class);

        when(mockTopic.getName()).thenReturn("test-topic");
        when(mockTopic.getBrokerService()).thenReturn(mockBrokerService);
        when(mockCursor.getManagedLedger()).thenReturn(mockManagedLedger);

        PulsarService mockPulsarService = mock(PulsarService.class);
        ServiceConfiguration config = new ServiceConfiguration();
        when(mockBrokerService.pulsar()).thenReturn(mockPulsarService);
        when(mockPulsarService.getConfig()).thenReturn(config);
    }

    /**
     * Ensure that mark delete short circuit resets expirationCheckInProgress flag.
     */
    @Test
    public void testExpireMessagesWithMarkDeleteShortCircuitResetsExpirationCheckInProgressFlag() throws Exception {
        // Setup: Create a scenario where mark delete position is already ahead
        Position markDeletedPosition = PositionFactory.create(2, 100);
        Position positionToExpire = PositionFactory.create(2, 50);  // Earlier than markDeletedPosition

        when(mockCursor.getMarkDeletedPosition()).thenReturn(markDeletedPosition);
        when(mockCursor.getManagedLedger()).thenReturn(mockManagedLedger);
        when(mockCursor.getName()).thenReturn("test-cursor");

        // Mock the asyncFindNewestMatching call to return positionToExpire
        doAnswer(invocation -> {
            AsyncCallbacks.FindEntryCallback callback = invocation.getArgument(4);
            Object ctx = invocation.getArgument(5);
            callback.findEntryComplete(positionToExpire, ctx);
            return null;
        }).when(mockCursor).asyncFindNewestMatching(
                any(ManagedCursor.FindPositionConstraint.class),
                any(),  // Predicate<Entry>
                any(),  // startPosition
                any(),  // endPosition
                any(AsyncCallbacks.FindEntryCallback.class),
                any(),  // ctx
                anyBoolean()  // isFindFromLedger
        );

        // Setup ledger info with expired ledger
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfo = new TreeMap<>();
        MLDataFormats.ManagedLedgerInfo.LedgerInfo expiredLedger =
            MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder()
                .setLedgerId(2)
                .setEntries(60)
                .setTimestamp(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10))  // 10 seconds old
                .build();
        ledgerInfo.put(2L, expiredLedger);

        when(mockManagedLedger.getLedgersInfo()).thenReturn(ledgerInfo);
        when(mockManagedLedger.getLastConfirmedEntry()).thenReturn(PositionFactory.create(2, 200));

        PersistentMessageExpiryMonitor monitor =
                new PersistentMessageExpiryMonitor(mockTopic, "test-subscription", mockCursor, null);

        // First call should return true
        boolean firstCallResult = monitor.expireMessages(5);
        assertTrue(firstCallResult, "First expireMessages call should return true");

        // Second call should return true since false would be returned if expirationCheckInProgress was not reset
        boolean secondCallResult = monitor.expireMessages(5);
        assertTrue(secondCallResult, "Second expireMessages call should also return true");

        // All subsequent calls will also return true
        boolean thirdCallResult = monitor.expireMessages(5);
        assertTrue(thirdCallResult, "Third expireMessages call should also return true");
    }
}