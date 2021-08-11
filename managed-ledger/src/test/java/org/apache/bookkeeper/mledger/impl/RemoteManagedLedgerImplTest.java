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

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.client.api.CursorClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RemoteManagedLedgerImplTest extends MockedBookKeeperTestCase {

    public static RemoteManagedLedgerImpl openRemoteManagedLedger(ManagedLedgerFactoryImpl factory,
                                                                  TopicName readerTopic,
                                                                  TopicName writerTopic)
            throws ManagedLedgerException, InterruptedException {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setWriterTopic(writerTopic);
        ManagedLedger readerMl = factory.open(readerTopic.getPersistenceNamingEncoding(), config);
        Assert.assertTrue(readerMl instanceof RemoteManagedLedgerImpl);
        return (RemoteManagedLedgerImpl) readerMl;
    }

    private static Supplier<PulsarClient> mockClientSupplier() {
        return () -> {
            PulsarClient client = mock(PulsarClient.class);
            CursorClientBuilder ccb = mock(CursorClientBuilder.class);
            when(ccb.topic(any())).thenReturn(ccb);
            when(ccb.createAsync()).thenReturn(CompletableFuture.completedFuture(null));
            when(client.newCursorClient()).thenReturn(ccb);
            return client;
        };
    }

    @Test
    public void testLacSyncInit() throws Exception {
        TopicName writerTopic = TopicName.get("persistent://test/writer/testLacSyncInit");
        ManagedLedger writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("data1".getBytes());
        writerMl.addEntry("data2".getBytes());
        writerMl.addEntry("data3".getBytes());
        writerMl.close(); //this ledger is closed.

        writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("data4".getBytes());
        writerMl.addEntry("data5".getBytes());//this ledger is active

        factory.setClientSupplier(mockClientSupplier());

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testLacSyncInit");
        RemoteManagedLedgerImpl rml = openRemoteManagedLedger(factory, readerTopic, writerTopic);

        Assert.assertEquals(rml.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());
    }

    @Test
    public void testLacSyncDynamicEntries() throws Exception {
        TopicName writerTopic = TopicName.get("persistent://test/writer/testLacSyncDynamicEntries");
        ManagedLedger writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("data1".getBytes());
        writerMl.addEntry("data2".getBytes());
        writerMl.addEntry("data3".getBytes());
        writerMl.close(); // ledger 1 is closed.

        writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("data4".getBytes());
        writerMl.addEntry("data5".getBytes());//this ledger is active

        factory.setClientSupplier(mockClientSupplier());

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testLacSyncDynamicEntries");
        RemoteManagedLedgerImpl rml = openRemoteManagedLedger(factory, readerTopic, writerTopic);

        Assert.assertEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());

        //write new entry in writer.
        writerMl.addEntry("new entry".getBytes());

        //Lac in reader falls behind.
        Assert.assertNotEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());

        //update lac
        rml.asyncUpdateLastConfirmedEntry().get();

        //check asyncUpdateLastConfirmedEntry works.
        Assert.assertEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());
    }


    @Test
    public void testLacSyncDynamicLedgers() throws Exception {
        TopicName writerTopic = TopicName.get("persistent://test/writer/testLacSyncDynamicLedgers");
        ManagedLedger writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("data1".getBytes());
        writerMl.addEntry("data2".getBytes());
        writerMl.addEntry("data3".getBytes());
        writerMl.close(); // ledger 1 is closed.

        factory.setClientSupplier(mockClientSupplier());

        TopicName readerTopic = TopicName.get("persistent://test/readonly/testLacSyncDynamicLedgers");
        RemoteManagedLedgerImpl rml = openRemoteManagedLedger(factory, readerTopic, writerTopic);

        Assert.assertEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());

        //2. it will work with new ledgers.
        writerMl = factory.open(writerTopic.getPersistenceNamingEncoding());
        writerMl.addEntry("new ledger".getBytes());
        rml.asyncUpdateLastConfirmedEntry().get();
        //check asyncUpdateLastConfirmedEntry works.
        Assert.assertEquals(rml.getLastConfirmedEntry(), writerMl.getLastConfirmedEntry());
    }


}