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
package org.apache.pulsar.broker.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.PersistentTransactionBuffer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
public class TransactionProduceTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String CLUSTER_NAME = "test";
    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_INPUT_1 = NAMESPACE1 + "/input1";
    private final static String TOPIC_INPUT_2 = NAMESPACE1 + "/input2";
    private final static String TOPIC_OUTPUT_1 = NAMESPACE1 + "/output1";
    private final static String TOPIC_OUTPUT_2 = NAMESPACE1 + "/output2";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        int webServicePort = getServiceConfigurationList().get(0).getWebServicePort().get();
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_1, 3);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_2, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_1, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_2, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        int brokerPort = getServiceConfigurationList().get(0).getBrokerServicePort().get();
        pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:" + brokerPort)
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void produceAndCommitTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        long txnIdMostBits = ((TransactionImpl) tnx).getTxnIdMostBits();
        long txnIdLeastBits = ((TransactionImpl) tnx).getTxnIdLeastBits();
        Assert.assertTrue(txnIdMostBits > -1);
        Assert.assertTrue(txnIdLeastBits > -1);

        @Cleanup
        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(TOPIC_OUTPUT_1)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();

        int messageCntPerPartition = 3;
        int messageCnt = TOPIC_PARTITION * messageCntPerPartition;
        String content = "Hello Txn - ";
        Set<String> messageSet = new HashSet<>();
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();
        for (int i = 0; i < messageCnt; i++) {
            String msg = content + i;
            messageSet.add(msg);
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(tnx).value(msg.getBytes(UTF_8)).sendAsync();
            futureList.add(produceFuture);
        }

        // the target topic hasn't the commit marker before commit
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(TOPIC_OUTPUT_1, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertFalse(originTopicCursor.hasMoreEntries());
            originTopicCursor.close();
        }

        // the messageId callback can't be called before commit
        futureList.forEach(messageIdFuture -> {
            try {
                messageIdFuture.get(1, TimeUnit.SECONDS);
                Assert.fail("MessageId shouldn't be get before txn commit.");
            } catch (Exception e) {
                if (e instanceof TimeoutException) {
                    log.info("This is a expected exception.");
                } else {
                    log.error("This exception is not expected.", e);
                    Assert.fail("This exception is not expected.");
                }
            }
        });

        tnx.commit().get();

        Thread.sleep(3000L);

        // the messageId callback should be called after commit
        futureList.forEach(messageIdFuture -> {
            try {
                MessageId messageId = messageIdFuture.get(1, TimeUnit.SECONDS);
                Assert.assertNotNull(messageId);
                log.info("Tnx commit success! messageId: {}", messageId);
            } catch (Exception e) {
                log.error("Tnx commit failed! tnx: " + tnx, e);
                Assert.fail("Tnx commit failed! tnx: " + tnx);
            }
        });

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            // the target topic partition received the commit marker
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(TOPIC_OUTPUT_1, i);
            Assert.assertNotNull(originTopicCursor);
            Assert.assertTrue(originTopicCursor.hasMoreEntries());
            List<Entry> entries = originTopicCursor.readEntries((int) originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(1, entries.size());
            PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(0).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            long commitMarkerLedgerId = entries.get(0).getLedgerId();
            long commitMarkerEntryId = entries.get(0).getEntryId();

            // the target topic transactionBuffer should receive the transaction messages,
            // committing marker and commit marker
            ReadOnlyCursor tbTopicCursor = getTBTopicCursor(TOPIC_OUTPUT_1, i);
            Assert.assertNotNull(tbTopicCursor);
            Assert.assertTrue(tbTopicCursor.hasMoreEntries());
            long tbEntriesCnt = tbTopicCursor.getNumberOfEntries();
            log.info("transaction buffer entries count: {}", tbEntriesCnt);
            Assert.assertEquals(tbEntriesCnt, messageCntPerPartition + 2);

            entries = tbTopicCursor.readEntries((int) tbEntriesCnt);
            // check the messages
            for (int j = 0; j < messageCntPerPartition; j++) {
                messageMetadata = Commands.parseMessageMetadata(entries.get(j).getDataBuffer());
                Assert.assertEquals(messageMetadata.getTxnidMostBits(), txnIdMostBits);
                Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txnIdLeastBits);

                byte[] bytes = new byte[entries.get(j).getDataBuffer().readableBytes()];
                entries.get(j).getDataBuffer().readBytes(bytes);
                System.out.println(new String(bytes));
                Assert.assertTrue(messageSet.remove(new String(bytes)));
            }

            // check committing marker
            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMITTING_VALUE, messageMetadata.getMarkerType());

            // check commit marker, committedAtLedgerId and committedAtEntryId
            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition + 1).getDataBuffer());
            Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            PulsarMarkers.TxnCommitMarker commitMarker = Markers.parseCommitMarker(entries.get(messageCntPerPartition + 1).getDataBuffer());
            Assert.assertEquals(commitMarkerLedgerId, commitMarker.getMessageId().getLedgerId());
            Assert.assertEquals(commitMarkerEntryId, commitMarker.getMessageId().getEntryId());
        }

        Assert.assertEquals(0, messageSet.size());
        System.out.println("finish test");
    }

    private ReadOnlyCursor getTBTopicCursor(String topic, int partition) {
        try {
            String tbTopicName = PersistentTransactionBuffer.getTransactionBufferTopicName(
                    TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition);

            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(tbTopicName).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get transaction buffer topic readonly cursor.", e);
            Assert.fail("Failed to get transaction buffer topic readonly cursor.");
            return null;
        }
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            String partitionTopic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(partitionTopic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get origin topic readonly cursor.", e);
            Assert.fail("Failed to get origin topic readonly cursor.");
            return null;
        }
    }

}
