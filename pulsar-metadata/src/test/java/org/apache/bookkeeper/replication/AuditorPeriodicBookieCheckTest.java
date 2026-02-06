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

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.testng.AssertJUnit.assertEquals;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Cleanup;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client.
 */
public class AuditorPeriodicBookieCheckTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicBookieCheckTest.class);

    private AuditorElector auditorElector = null;

    private static final int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicBookieCheckTest() throws Exception {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAuditorPeriodicBookieCheckInterval(CHECK_INTERVAL);

        conf.setMetadataServiceUri(
                metadataServiceUri.replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        conf.setProperty("clientConnectTimeoutMillis", 500);
        String addr = addressByIndex(0).toString();

        auditorElector = new AuditorElector(addr, conf);
        auditorElector.start();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        auditorElector.shutdown();
        super.tearDown();
    }

    /**
     * Test that the periodic bookie checker works.
     */
    @Test
    public void testPeriodicBookieCheckInterval() throws Exception {
        confByIndex(0).setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        runFunctionWithLedgerManagerFactory(confByIndex(0), mFactory -> {
            try (LedgerManager ledgerManager = mFactory.newLedgerManager()) {
                @Cleanup final LedgerUnderreplicationManager underReplicationManager =
                        mFactory.newLedgerUnderreplicationManager();
                long ledgerId = 12345L;
                ClientUtil.setupLedger(bkc.getLedgerManager(), ledgerId,
                        LedgerMetadataBuilder.create().withEnsembleSize(3)
                                .withWriteQuorumSize(3).withAckQuorumSize(3)
                                .newEnsembleEntry(0L, Lists.newArrayList(
                                        new BookieSocketAddress("192.0.2.1", 1000).toBookieId(),
                                        getBookie(0),
                                        getBookie(1))));
                long underReplicatedLedger = -1;
                for (int i = 0; i < 10; i++) {
                    underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
                    if (underReplicatedLedger != -1) {
                        break;
                    }
                    Thread.sleep(CHECK_INTERVAL * 1000);
                }
                assertEquals("Ledger should be under replicated", ledgerId, underReplicatedLedger);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
            return null;
        });
    }
}
