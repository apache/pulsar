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
import static org.testng.AssertJUnit.assertEquals;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link AuditorReplicasCheckTask}.
 */
public class AuditorReplicasCheckTaskTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorReplicasCheckTaskTest.class);

    private BookKeeperAdmin admin;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;

    public AuditorReplicasCheckTaskTest() throws Exception {
        super(3);
        baseConf.setPageLimit(1);
        baseConf.setAutoRecoveryDaemonEnabled(false);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseClientConf.setMetadataServiceUri(
                metadataServiceUri.replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        final BookKeeper bookKeeper = registerCloseable(new BookKeeper(baseClientConf));
        admin = new BookKeeperAdmin(bookKeeper, NullStatsLogger.INSTANCE, new ClientConfiguration(baseClientConf));
        LedgerManagerFactory ledgerManagerFactory = registerCloseable(bookKeeper.getLedgerManagerFactory());
        ledgerManager = ledgerManagerFactory.newLedgerManager();
        ledgerUnderreplicationManager = ledgerManagerFactory.newLedgerUnderreplicationManager();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        if (ledgerManager != null) {
            ledgerManager.close();
        }
        if (ledgerUnderreplicationManager != null) {
            ledgerUnderreplicationManager.close();
        }
        if (admin != null) {
            admin.close();
        }
        super.tearDown();
    }

    @Test
    public void testReplicasCheck() throws BKException, InterruptedException {

        // 1. create ledgers
        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        // 2. init auditorReplicasCheckTask
        final TestStatsProvider statsProvider = new TestStatsProvider();
        final TestStatsProvider.TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        final AuditorStats auditorStats = new AuditorStats(statsLogger);
        AuditorReplicasCheckTask auditorReplicasCheckTask = new AuditorReplicasCheckTask(
                baseConf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, null, (flag, throwable) -> flag.set(false));

        // 3. replicasCheck
        auditorReplicasCheckTask.runTask();

        // 4. verify
        assertEquals("REPLICAS_CHECK_TIME", 1, ((TestStatsProvider.TestOpStatsLogger)
                statsLogger.getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME)).getSuccessCount());
    }
}
