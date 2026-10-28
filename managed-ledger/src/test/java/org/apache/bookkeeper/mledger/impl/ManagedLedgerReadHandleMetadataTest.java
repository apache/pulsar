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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.PulsarBookKeeperTestClient;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Checks that the read handles cached by a managed ledger follow the changes of their ledger's metadata, which this
 * test needs its own bookies for, as it stops some of them.
 */
public class ManagedLedgerReadHandleMetadataTest extends BookKeeperClusterTestCase {

    public ManagedLedgerReadHandleMetadataTest() {
        super(4);
    }

    /**
     * The metadata service URI of the client the managed ledger reads through, whose metadata driver notifies the
     * read handles of the ledger metadata updates.
     */
    protected String getReaderMetadataServiceUri() throws Exception {
        return baseClientConf.getMetadataServiceUri();
    }

    protected Class<? extends MetadataClientDriver> getReaderMetadataDriverClass() {
        return ZKMetadataClientDriver.class;
    }

    @Test(timeOut = 60000)
    public void testCachedReadHandleReadsFromReReplicatedBookies() throws Exception {
        ClientConfiguration readerConf = new ClientConfiguration(baseClientConf);
        readerConf.setMetadataServiceUri(getReaderMetadataServiceUri());
        @Cleanup
        BookKeeper reader = new PulsarBookKeeperTestClient(readerConf);
        assertThat(reader.getMetadataClientDriver()).isInstanceOf(getReaderMetadataDriverClass());
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, reader);
        ManagedLedgerConfig config = defaultConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setMaxEntriesPerLedger(5);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cached-read-handle-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("c1", InitialPosition.Earliest);
        for (int i = 0; i < 5; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        Awaitility.await().untilAsserted(() -> assertThat(ledger.getLedgersInfo()).hasSize(2));
        long ledgerId = ledger.getLedgersInfo().firstKey();

        ReadHandle readHandle = ledger.getLedgerHandle(ledgerId).get();
        Set<BookieId> originalBookies = new HashSet<>(readHandle.getLedgerMetadata().getEnsembleAt(0));

        // Move the ledger off its bookies through another client, as the auto-recovery does on a decommission
        @Cleanup
        BookKeeperAdmin admin = new BookKeeperAdmin(bkc, baseClientConf);
        admin.recoverBookieData(ledgerId, originalBookies, false, false);
        Awaitility.await().untilAsserted(() -> assertThat(readHandle.getLedgerMetadata().getEnsembleAt(0))
                .doesNotContainAnyElementsOf(originalBookies));
        for (BookieId bookie : originalBookies) {
            killBookie(bookie);
        }

        factory.getEntryCacheManager().clear();
        List<Entry> entries = cursor.readEntries(5);
        try {
            assertThat(entries).extracting(entry -> new String(entry.getData(), StandardCharsets.UTF_8))
                    .containsExactly("entry-0", "entry-1", "entry-2", "entry-3", "entry-4");
        } finally {
            entries.forEach(Entry::release);
        }
        // The entries were read through the cached handle, not one opened again after a read error
        assertThat(ledger.ledgerCache.get(ledgerId).getNow(null)).isSameAs(readHandle);
    }
}
