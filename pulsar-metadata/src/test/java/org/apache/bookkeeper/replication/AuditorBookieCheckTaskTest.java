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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link AuditorBookieCheckTask}.
 */
public class AuditorBookieCheckTaskTest {

    private AuditorStats auditorStats;
    private BookKeeperAdmin admin;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager underreplicationManager;
    private BookieLedgerIndexer ledgerIndexer;
    private AuditorBookieCheckTask bookieCheckTask;
    private final AtomicBoolean shutdownCompleted = new AtomicBoolean(false);
    private final AuditorTask.ShutdownTaskHandler shutdownTaskHandler = () -> shutdownCompleted.set(true);
    private long startLedgerId = 0;

    @BeforeMethod
    public void setup() {
        ServerConfiguration conf = mock(ServerConfiguration.class);
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        final AuditorStats auditorStats = new AuditorStats(statsLogger);
        this.auditorStats = spy(auditorStats);
        admin = mock(BookKeeperAdmin.class);
        ledgerManager = mock(LedgerManager.class);
        underreplicationManager = mock(LedgerUnderreplicationManager.class);
        ledgerIndexer = mock(BookieLedgerIndexer.class);
        AuditorBookieCheckTask bookieCheckTask1 = new AuditorBookieCheckTask(
                conf, this.auditorStats, admin, ledgerManager, underreplicationManager,
                shutdownTaskHandler, ledgerIndexer, null, null);
        bookieCheckTask = spy(bookieCheckTask1);
    }

    @Test
    public void testShutdownAuditBookiesException()
            throws BKException, ReplicationException.BKAuditException, InterruptedException {
        doThrow(new ReplicationException.BKAuditException("test failed"))
                .when(bookieCheckTask)
                .auditBookies();
        bookieCheckTask.startAudit(true);

        assertTrue("shutdownTaskHandler should be execute.", shutdownCompleted.get());
    }

    @Test
    public void testAuditBookies()
            throws ReplicationException.UnavailableException, ReplicationException.BKAuditException, BKException {
        final String bookieId1 = "127.0.0.1:1000";
        final String bookieId2 = "127.0.0.1:1001";
        final long bookie1LedgersCount = 10;
        final long bookie2LedgersCount = 20;

        final Map<String, Set<Long>> bookiesAndLedgers = new HashMap<>();
        bookiesAndLedgers.put(bookieId1, getLedgers(bookie1LedgersCount));
        bookiesAndLedgers.put(bookieId2, getLedgers(bookie2LedgersCount));
        when(ledgerIndexer.getBookieToLedgerIndex()).thenReturn(bookiesAndLedgers);
        when(underreplicationManager.isLedgerReplicationEnabled()).thenReturn(true);

        CompletableFuture<Versioned<LedgerMetadata>> metaPromise = new CompletableFuture<>();
        final LongVersion version = mock(LongVersion.class);
        final LedgerMetadata metadata = mock(LedgerMetadata.class);
        metaPromise.complete(new Versioned<>(metadata, version));
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(metaPromise);

        CompletableFuture<Void> markPromise = new CompletableFuture<>();
        markPromise.complete(null);
        when(underreplicationManager.markLedgerUnderreplicatedAsync(anyLong(), anyCollection()))
                .thenReturn(markPromise);

        OpStatsLogger numUnderReplicatedLedgerStats = mock(OpStatsLogger.class);
        when(auditorStats.getNumUnderReplicatedLedger()).thenReturn(numUnderReplicatedLedgerStats);

        final List<BookieId> availableBookies = new ArrayList<>();
        final List<BookieId> readOnlyBookies = new ArrayList<>();
        // test bookie1 lost
        availableBookies.add(BookieId.parse(bookieId2));
        when(admin.getAvailableBookies()).thenReturn(availableBookies);
        when(admin.getReadOnlyBookies()).thenReturn(readOnlyBookies);
        bookieCheckTask.startAudit(true);
        verify(numUnderReplicatedLedgerStats, times(1))
                .registerSuccessfulValue(eq(bookie1LedgersCount));

        // test bookie2 lost
        numUnderReplicatedLedgerStats = mock(OpStatsLogger.class);
        when(auditorStats.getNumUnderReplicatedLedger()).thenReturn(numUnderReplicatedLedgerStats);
        availableBookies.clear();
        availableBookies.add(BookieId.parse(bookieId1));
        bookieCheckTask.startAudit(true);
        verify(numUnderReplicatedLedgerStats, times(1))
                .registerSuccessfulValue(eq(bookie2LedgersCount));

    }

    private Set<Long> getLedgers(long count) {
        final Set<Long> ledgers = new HashSet<>();
        for (int i = 0; i < count; i++) {
            ledgers.add(i + startLedgerId++);
        }
        return ledgers;
    }
}
