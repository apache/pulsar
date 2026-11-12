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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-impl")
public class BatchMessageIndexAckTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        doReturn(CompletableFuture.completedFuture(new LedgerMetadata() {
            @Override
            public long getLedgerId() {
                return 0;
            }

            @Override
            public int getEnsembleSize() {
                return 0;
            }

            @Override
            public int getWriteQuorumSize() {
                return 0;
            }

            @Override
            public int getAckQuorumSize() {
                return 0;
            }

            @Override
            public long getLastEntryId() {
                return 0;
            }

            @Override
            public long getLength() {
                return 0;
            }

            @Override
            public boolean hasPassword() {
                return false;
            }

            @Override
            public byte[] getPassword() {
                return new byte[0];
            }

            @Override
            public DigestType getDigestType() {
                return null;
            }

            @Override
            public long getCtime() {
                return 0;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public Map<String, byte[]> getCustomMetadata() {
                return null;
            }

            @Override
            public List<BookieId> getEnsembleAt(long entryId) {
                return null;
            }

            @Override
            public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
                return null;
            }

            @Override
            public State getState() {
                return null;
            }

            @Override
            public String toSafeString() {
                return null;
            }

            @Override
            public int getMetadataFormatVersion() {
                return 0;
            }

            @Override
            public long getCToken() {
                return 0;
            }
        })).when(pulsarTestContext.getBookKeeperClient()).getLedgerMetadata(anyLong());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForSharedSubscription(boolean ackReceiptEnabled) throws Exception {
        final String topic = "testBatchMessageIndexAckForSharedSubscription";
        final String subscriptionName = "sub";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName(subscriptionName)
            .receiverQueueSize(100)
            .isAckReceiptEnabled(ackReceiptEnabled)
            .subscriptionType(SubscriptionType.Shared)
            .negativeAckRedeliveryDelay(2, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        // A negative ack redelivers the whole batch, with the indexes not acknowledged when the broker dispatches it:
        // a redelivery that crosses an ack on its way to the broker delivers the acknowledged message again. Ack half
        // of the batch, and nack only once the broker has those acks.
        List<Message<Integer>> batch = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            batch.add(consumer.receive());
        }
        List<MessageId> acked = new ArrayList<>(50);
        for (int i = 0; i < messages; i += 2) {
            consumer.acknowledge(batch.get(i));
            acked.add(batch.get(i).getMessageId());
        }
        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) acked.get(0);
        Position batchPosition = PositionFactory.create(batchMessageId.getLedgerId(), batchMessageId.getEntryId());
        ManagedCursor cursor = ((PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(TopicName.get(topic).toString()).orElseThrow()
                .getSubscription(subscriptionName)).getCursor();
        Awaitility.await().untilAsserted(() -> {
            long[] unackedIndexes = cursor.getDeletedBatchIndexesAsLongArray(batchPosition);
            Assert.assertNotNull(unackedIndexes);
            Assert.assertEquals(BitSet.valueOf(unackedIndexes).cardinality(), 50);
        });
        // the batch comes back with the 50 messages not acknowledged
        consumer.negativeAcknowledge(batch.get(1));

        List<MessageId> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        Assert.assertEquals(received.size(), 50);
        acked.retainAll(received);
        Assert.assertEquals(acked.size(), 0);

        for (MessageId messageId : received) {
            consumer.acknowledge(messageId);
        }

        // check the mark delete position was changed, once the broker has the acks
        BatchMessageIdImpl ackedMessageId = (BatchMessageIdImpl) received.get(0);
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic, false);
            String markDeletePosition = stats.cursors.get(subscriptionName).markDeletePosition;
            Assert.assertEquals(ackedMessageId.ledgerId + ":" + ackedMessageId.entryId, markDeletePosition);
        });

        consumer.redeliverUnacknowledgedMessages();

        Message<Integer> moreMessage = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForExclusiveSubscription(boolean ackReceiptEnabled) throws
            PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "testBatchMessageIndexAckForExclusiveSubscription";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .isAckReceiptEnabled(ackReceiptEnabled)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < messages; i++) {
            if (i == 49) {
                consumer.acknowledgeCumulative(consumer.receive());
            } else {
                consumer.receive();
            }
        }

        //Wait ack send.
        Thread.sleep(1000);
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .subscribe();

        List<Message<Integer>> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        Assert.assertEquals(received.size(), 50);

        Message<Integer> moreMessage = consumer.receive(1, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test
    public void testDoNotRecycleAckSetMultipleTimes() throws Exception  {
        final String topic = "persistent://my-property/my-ns/testSafeAckSetRecycle";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .batchingMaxMessages(10)
                .blockIfQueueFull(true).topic(topic)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.sendAsync("Hello Pulsar".getBytes());
        }

        // Should not throw an exception.
        for (int i = 0; i < messages; i++) {
            consumer.acknowledgeCumulative(consumer.receive());
            // make sure the group ack flushed.
            Thread.sleep(2);
        }

        producer.close();
        consumer.close();
    }
}
