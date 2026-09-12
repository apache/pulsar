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
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.TestCallbacks;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test auditor behaviours during a rolling restart.
 */
public class AuditorRollingRestartTest extends BookKeeperClusterTestCase {

    public AuditorRollingRestartTest() throws Exception {
        super(3, 600);
        // run the daemon within the bookie
        setAutoRecoveryEnabled(true);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected void startBKCluster(String metadataServiceUri) throws Exception {
        super.startBKCluster(metadataServiceUri.replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
    }

    /**
     * Test no auditing during restart if disabled.
     */
    @Test
    public void testAuditingDuringRollingRestart() throws Exception {
        confByIndex(0).setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        runFunctionWithLedgerManagerFactory(
                confByIndex(0),
                mFactory -> {
                    try {
                        testAuditingDuringRollingRestart(mFactory);
                    } catch (Exception e) {
                        throw new UncheckedExecutionException(e.getMessage(), e);
                    }
                    return null;
                }
        );
    }

    private void testAuditingDuringRollingRestart(LedgerManagerFactory mFactory) throws Exception {
        final LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 10; i++) {
            lh.asyncAddEntry("foobar".getBytes(), new TestCallbacks.AddCallbackFuture(i), null);
        }
        lh.addEntry("foobar".getBytes());
        lh.close();

        assertEquals("shouldn't be anything under replicated",
                underReplicationManager.pollLedgerToRereplicate(), -1);
        underReplicationManager.disableLedgerReplication();

        @Cleanup
        LedgerAuditorManager lam = mFactory.newLedgerAuditorManager();
        BookieId auditor = lam.getCurrentAuditor();
        ServerConfiguration conf = killBookie(auditor);
        Thread.sleep(2000);
        startBookie(conf);
        Thread.sleep(2000); // give it time to run
        assertEquals("shouldn't be anything under replicated", -1,
                underReplicationManager.pollLedgerToRereplicate());
    }
}
