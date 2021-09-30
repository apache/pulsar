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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

@Test(groups = "broker")
public class TransactionMarkerDeleteTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setTransactionCoordinatorEnabled(true);
        super.baseSetup();
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));

        admin.namespaces().createNamespace("public/default");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMarkerDeleteTimes() throws Exception {
        ManagedLedgerImpl managedLedger = spy((ManagedLedgerImpl) pulsar.getManagedLedgerFactory().open("test"));
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = mock(ServiceConfiguration.class);
        doReturn(brokerService).when(topic).getBrokerService();
        doReturn(pulsarService).when(brokerService).getPulsar();
        doReturn(configuration).when(pulsarService).getConfig();
        doReturn(false).when(configuration).isTransactionCoordinatorEnabled();
        doReturn(managedLedger).when(topic).getManagedLedger();
        ManagedCursor cursor = managedLedger.openCursor("test");
        PersistentSubscription persistentSubscription = spy(new PersistentSubscription(topic, "test",
                cursor, false));
        Position position = managedLedger.addEntry("test".getBytes());
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position),
                AckType.Individual, Collections.emptyMap());
        verify(managedLedger, times(0)).asyncReadEntry(any(), any(), any());
    }


    @Test
    public void testMarkerDelete() throws Exception {

        MessageMetadata msgMetadata = new MessageMetadata().clear()
                .setPublishTime(1)
                .setProducerName("test")
                .setSequenceId(0);

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(0);

        payload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                msgMetadata, payload);

        ManagedLedger managedLedger = pulsar.getManagedLedgerFactory().open("test");
        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn(pulsar.getBrokerService()).when(topic).getBrokerService();
        doReturn(managedLedger).when(topic).getManagedLedger();
        doReturn("test").when(topic).getName();
        ManagedCursor cursor = managedLedger.openCursor("test");
        PersistentSubscription persistentSubscription = new PersistentSubscription(topic, "test",
                managedLedger.openCursor("test"), false);

        byte[] payloadBytes = toBytes(payload);
        Position position1 = managedLedger.addEntry(payloadBytes);
        Position markerPosition1 = managedLedger.addEntry(toBytes(Markers
                .newTxnCommitMarker(1, 1, 1)));

        Position position2 = managedLedger.addEntry(payloadBytes);
        Position markerPosition2 = managedLedger.addEntry(toBytes(Markers
                .newTxnAbortMarker(1, 1, 1)));

        Position position3 = managedLedger.addEntry(payloadBytes);

        assertEquals(cursor.getNumberOfEntriesInBacklog(true), 5);
        assertTrue(((PositionImpl) cursor.getMarkDeletedPosition()).compareTo((PositionImpl) position1) < 0);

        // ack position1, markerDeletePosition to markerPosition1
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position1),
                AckType.Individual, Collections.emptyMap());

        // ack position1, markerDeletePosition to markerPosition1
        Awaitility.await().during(1, TimeUnit.SECONDS).until(() ->
                ((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition())
                        .compareTo((PositionImpl) markerPosition1) == 0);

        // ack position2, markerDeletePosition to markerPosition2
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position2),
                AckType.Individual, Collections.emptyMap());

        Awaitility.await().until(() ->
                ((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition())
                        .compareTo((PositionImpl) markerPosition2) == 0);

        // add consequent marker
        managedLedger.addEntry(toBytes(Markers
                .newTxnCommitMarker(1, 1, 1)));

        managedLedger.addEntry(toBytes(Markers
                .newTxnAbortMarker(1, 1, 1)));

        Position markerPosition3 = managedLedger.addEntry(toBytes(Markers
                .newTxnAbortMarker(1, 1, 1)));

        // ack with transaction, then commit this transaction
        persistentSubscription.transactionIndividualAcknowledge(new TxnID(0, 0),
                Collections.singletonList(MutablePair.of((PositionImpl) position3, 0))).get();

        persistentSubscription.endTxn(0, 0, 0, 0).get();

        // ack with transaction, then commit this transaction
        Awaitility.await().until(() ->
                ((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition())
                        .compareTo((PositionImpl) markerPosition3) == 0);

    }

    static byte[] toBytes(ByteBuf byteBuf) {
        byte[] buf = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(buf);
        byteBuf.release();
        return buf;
    }
}
