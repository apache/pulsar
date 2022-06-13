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

import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryChangeLedgerPathTest extends BookKeeperClusterTestCase {

    public ManagedLedgerFactoryChangeLedgerPathTest() {
        super(2);
    }

    @Override
    protected String changeLedgerPath() {
        return "/test";
    }

    @Test(timeOut = 60000)
    public void testChangeZKPath() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration();
        String zkConnectString = zkUtil.getZooKeeperConnectString() + "/test";
        configuration.setMetadataServiceUri("zk://" + zkConnectString + "/ledgers");
        configuration.setUseV2WireProtocol(true);
        configuration.setEnableDigestTypeAutodetection(true);
        configuration.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, configuration);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1)
            .setWriteQuorumSize(1)
            .setAckQuorumSize(1)
            .setMetadataAckQuorumSize(1)
            .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("test-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("test-c1" + testName);

        for (int i = 0; i < 10; i++) {
            String entry = "entry" + i;
            ledger.addEntry(entry.getBytes("UTF8"));
        }

        List<Entry> entryList = cursor.readEntries(10);
        Assert.assertEquals(10, entryList.size());

        for (int i = 0; i < 10; i++) {
            Entry entry = entryList.get(i);
            Assert.assertEquals(("entry" + i).getBytes("UTF8"), entry.getData());
        }
    }
    @Test(timeOut = 60000)
    public void testChangeZKPath2() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration();
        String zkConnectString = zkUtil.getZooKeeperConnectString() + "/test";
        configuration.setMetadataServiceUri("zk://" + zkConnectString + "/ledgers");
        configuration.setUseV2WireProtocol(true);
        configuration.setEnableDigestTypeAutodetection(true);
        configuration.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, configuration,
                managedLedgerFactoryConfig);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1)
                .setWriteQuorumSize(1)
                .setAckQuorumSize(1)
                .setMetadataAckQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("test-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("test-c1" + testName);

        for (int i = 0; i < 10; i++) {
            String entry = "entry" + i;
            ledger.addEntry(entry.getBytes("UTF8"));
        }

        List<Entry> entryList = cursor.readEntries(10);
        Assert.assertEquals(10, entryList.size());

        for (int i = 0; i < 10; i++) {
            Entry entry = entryList.get(i);
            Assert.assertEquals(("entry" + i).getBytes("UTF8"), entry.getData());
        }
    }
}
