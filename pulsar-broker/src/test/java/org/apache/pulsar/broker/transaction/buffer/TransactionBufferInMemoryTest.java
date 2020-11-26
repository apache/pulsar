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
package org.apache.pulsar.broker.transaction.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TransactionBufferInMemoryTest extends BrokerTestBase {

    private final static String TOPIC = "persistent://prop/ns-abc/test";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration configuration = getDefaultConf();
        configuration.setTransactionCoordinatorEnabled(true);
        super.baseSetup(configuration);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionBufferCommitAndAbort() throws Exception {
        admin.topics().createNonPartitionedTopicAsync(TOPIC);
        admin.topics().createSubscription(TOPIC, "test", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        TopicTransactionBuffer topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }
        TxnID txnID1 = new TxnID(1, 1);
        TxnID txnID2 = new TxnID(2, 2);
        topicTransactionBuffer.appendBufferToTxn(txnID1, 1,Unpooled.buffer()).get();
        topicTransactionBuffer.appendBufferToTxn(txnID1, 1,Unpooled.buffer()).get();
        topicTransactionBuffer.appendBufferToTxn(txnID1, 1,Unpooled.buffer()).get();
        topicTransactionBuffer.appendBufferToTxn(txnID2, 1,Unpooled.buffer()).get();
        topicTransactionBuffer.appendBufferToTxn(txnID2, 1,Unpooled.buffer()).get();
        topicTransactionBuffer.appendBufferToTxn(txnID2, 1,Unpooled.buffer()).get();

        Field field = TopicTransactionBuffer.class.getDeclaredField("txnBufferCache");
        field.setAccessible(true);
        ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>> txnBufferCache =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>>) field.get(topicTransactionBuffer);
        assertTrue(txnBufferCache.containsKey(txnID1));
        assertTrue(txnBufferCache.containsKey(txnID2));
        field = TopicTransactionBuffer.class.getDeclaredField("positionsSort");
        field.setAccessible(true);

        ConcurrentSkipListSet<PositionImpl> positionsSort =
                (ConcurrentSkipListSet<PositionImpl>) field.get(topicTransactionBuffer);

        assertTrue(positionsSort.containsAll(txnBufferCache.get(txnID1).values()));
        assertTrue(positionsSort.containsAll(txnBufferCache.get(txnID2).values()));

        ConcurrentSkipListSet<PositionImpl> comparePositionsSort = new ConcurrentSkipListSet<>();
        comparePositionsSort.addAll(txnBufferCache.get(txnID2).values());
        comparePositionsSort.addAll(txnBufferCache.get(txnID1).values());

        assertEquals(comparePositionsSort.first(), comparePositionsSort.first());

        ConcurrentOpenHashSet<PositionImpl> txn1Positions = txnBufferCache.get(txnID1);
        ConcurrentOpenHashSet<PositionImpl> txn2Positions = txnBufferCache.get(txnID2);
        topicTransactionBuffer.commitTxn(txnID1, null).get();

        assertFalse(txnBufferCache.containsKey(txnID1));
        assertTrue(txnBufferCache.containsKey(txnID2));

        assertEquals(txn1Positions.size(), 3);
        assertEquals(txn2Positions.size(), 3);
        for (int i = 0; i < txn1Positions.size(); i++) {
            assertFalse(positionsSort.contains(txn1Positions.values().get(i)));
            assertTrue(positionsSort.contains(txn2Positions.values().get(i)));
        }

        topicTransactionBuffer.abortTxn(txnID2, null).get();
        assertFalse(txnBufferCache.containsKey(txnID2));

        for (int i = 0; i < txn1Positions.size(); i++) {
            assertFalse(positionsSort.contains(txn2Positions.values().get(i)));
        }
    }

    @Test
    public void testTransactionBufferReplay() throws Exception {

        admin.topics().createNonPartitionedTopicAsync(TOPIC);
        admin.topics().createSubscription(TOPIC, "test", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        TopicTransactionBuffer topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }

        TxnID txnID1 = new TxnID(1, 1);
        TxnID txnID2 = new TxnID(2, 2);

        topicTransactionBuffer.appendBufferToTxn(txnID1, 1, getTxnMetadataByteBuf(1, 1)).get();
        topicTransactionBuffer.appendBufferToTxn(txnID1, 2, getTxnMetadataByteBuf(1, 1)).get();
        topicTransactionBuffer.appendBufferToTxn(txnID1, 3, getTxnMetadataByteBuf(1, 1)).get();

        topicTransactionBuffer.appendBufferToTxn(txnID2, 1, getTxnMetadataByteBuf(2, 2)).get();
        topicTransactionBuffer.appendBufferToTxn(txnID2, 1, getTxnMetadataByteBuf(2, 2)).get();
        topicTransactionBuffer.appendBufferToTxn(txnID2, 1, getTxnMetadataByteBuf(2, 2)).get();

        //test replay
        admin.topics().unload(TOPIC);

        topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();

        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }

        Field field = TopicTransactionBuffer.class.getDeclaredField("txnBufferCache");
        field.setAccessible(true);
        ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>> txnBufferCache =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>>) field.get(topicTransactionBuffer);
        assertTrue(txnBufferCache.containsKey(txnID1));
        assertTrue(txnBufferCache.containsKey(txnID2));
        field = TopicTransactionBuffer.class.getDeclaredField("positionsSort");
        field.setAccessible(true);

        ConcurrentSkipListSet<PositionImpl> positionsSort =
                (ConcurrentSkipListSet<PositionImpl>) field.get(topicTransactionBuffer);

        assertTrue(positionsSort.containsAll(txnBufferCache.get(txnID1).values()));
        assertTrue(positionsSort.containsAll(txnBufferCache.get(txnID2).values()));

        ConcurrentSkipListSet<PositionImpl> comparePositionsSort = new ConcurrentSkipListSet<>();
        comparePositionsSort.addAll(txnBufferCache.get(txnID2).values());
        comparePositionsSort.addAll(txnBufferCache.get(txnID1).values());

        assertEquals(comparePositionsSort.first(), comparePositionsSort.first());


        ConcurrentOpenHashSet<PositionImpl> txn1Positions = txnBufferCache.get(txnID1);
        ConcurrentOpenHashSet<PositionImpl> txn2Positions = txnBufferCache.get(txnID2);

        //test commit replay
        topicTransactionBuffer.commitTxn(txnID1, null).get();

        admin.topics().unload(TOPIC);

        topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();

        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }

        field = TopicTransactionBuffer.class.getDeclaredField("txnBufferCache");
        field.setAccessible(true);
        txnBufferCache =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>>) field.get(topicTransactionBuffer);

        field = TopicTransactionBuffer.class.getDeclaredField("positionsSort");
        field.setAccessible(true);

        positionsSort = (ConcurrentSkipListSet<PositionImpl>) field.get(topicTransactionBuffer);


        assertFalse(txnBufferCache.containsKey(txnID1));
        assertTrue(txnBufferCache.containsKey(txnID2));

        assertEquals(txn1Positions.size(), 3);
        assertEquals(txn2Positions.size(), 3);
        for (int i = 0; i < txn1Positions.size(); i++) {
            assertFalse(positionsSort.contains(txn1Positions.values().get(i)));
            assertTrue(positionsSort.contains(txn2Positions.values().get(i)));
        }

        topicTransactionBuffer.abortTxn(txnID2, null).get();

        //test offload
        admin.topics().unload(TOPIC);

        topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();
        field = TopicTransactionBuffer.class.getDeclaredField("positionsSort");
        field.setAccessible(true);

        positionsSort = (ConcurrentSkipListSet<PositionImpl>) field.get(topicTransactionBuffer);

        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }

        field = TopicTransactionBuffer.class.getDeclaredField("txnBufferCache");
        field.setAccessible(true);
        txnBufferCache =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>>) field.get(topicTransactionBuffer);

        assertFalse(txnBufferCache.containsKey(txnID2));

        for (int i = 0; i < txn1Positions.size(); i++) {
            assertFalse(positionsSort.contains(txn2Positions.values().get(i)));
        }
    }

    @Test
    public void testReplayIndex() throws Exception {

        admin.topics().createNonPartitionedTopicAsync(TOPIC);
        admin.topics().createSubscription(TOPIC, "test", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        TopicTransactionBuffer topicTransactionBuffer =
                (TopicTransactionBuffer) topic.getTransactionBuffer(true).get();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(100);
            if (topicTransactionBuffer.getState() == TopicTransactionBufferState.State.Ready) {
                break;
            }
        }

        TxnID txnID1 = new TxnID(1, 1);
        TxnID txnID2 = new TxnID(2, 2);

        for (int i = 0; i < 5000; i++) {
            topicTransactionBuffer.appendBufferToTxn(txnID1, 1, getTxnMetadataByteBuf(1, 1)).get();
        }

        topicTransactionBuffer.commitTxn(txnID1, null).get();

        PositionImpl position = null;
        for (int i = 0; i < 5001; i++) {
            if (i == 0) {
                position = (PositionImpl) topicTransactionBuffer
                        .appendBufferToTxn(txnID2, 1, getTxnMetadataByteBuf(2, 2)).get();
            } else {
                topicTransactionBuffer.appendBufferToTxn(txnID2, 1, getTxnMetadataByteBuf(2, 2)).get();
            }
        }
        PositionImpl storePosition = PositionImpl.convertStringToPosition(topic.getManagedLedger()
                .getProperties().get(topic.getName() + "-txnOnGoingPosition"));
        assertEquals(storePosition, position);

        //unload check the replay index
        admin.topics().unload(TOPIC);
        topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), true).get().get();
        storePosition = PositionImpl.convertStringToPosition(topic.getManagedLedger()
                .getProperties().get(topic.getName() + "-txnOnGoingPosition"));
        assertEquals(storePosition, position);

    }

    private ByteBuf getTxnMetadataByteBuf(long txnIdMostBits, long txnIdLeastBits) throws IOException {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        builder.setPublishTime(System.currentTimeMillis());
        builder.setSequenceId(1);
        builder.setProducerName("test");
        builder.setTxnidLeastBits(txnIdLeastBits);
        builder.setTxnidMostBits(txnIdMostBits);
        MessageMetadata messageMetadata = builder.build();
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeInt(messageMetadata.getSerializedSize());
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(byteBuf);
        messageMetadata.writeTo(outStream);
        outStream.recycle();
        return byteBuf;
    }
}
