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
package org.apache.pulsar.broker.transaction.topictb;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Pulsar client transaction test.
 */
@Slf4j
public class TransactionProduceTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String PRODUCE_COMMIT_TOPIC = NAMESPACE1 + "/produce-commit";
    private final static String PRODUCE_ABORT_TOPIC = NAMESPACE1 + "/produce-abort";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(PRODUCE_COMMIT_TOPIC, 3);
        admin.topics().createPartitionedTopic(PRODUCE_ABORT_TOPIC, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
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
        produceTest(true);
    }

    @Test
    public void produceAndAbortTest() throws Exception {
        produceTest(false);
    }

    // endAction - commit: true, endAction - abort: false
    private void produceTest(boolean endAction) throws Exception {
        final String topic = endAction ? PRODUCE_COMMIT_TOPIC : PRODUCE_ABORT_TOPIC;
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
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
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

        checkMessageId(futureList, true);

        PulsarApi.MessageMetadata messageMetadata;

        // the target topic hasn't the commit marker before commit
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_COMMIT_TOPIC, i);
            Assert.assertNotNull(originTopicCursor);
            log.info("entries count: {}", originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(messageCntPerPartition, originTopicCursor.getNumberOfEntries());

            List<Entry> entries = originTopicCursor.readEntries(messageCnt);
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
            originTopicCursor.close();
        }

        if (endAction) {
            tnx.commit().get();
        } else {
            tnx.abort().get();
        }

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            // the target topic partition received the commit marker
            ReadOnlyCursor originTopicCursor = getOriginTopicCursor(PRODUCE_COMMIT_TOPIC, i);
            List<Entry> entries = originTopicCursor.readEntries((int) originTopicCursor.getNumberOfEntries());
            Assert.assertEquals(messageCntPerPartition + 1, entries.size());

            messageMetadata = Commands.parseMessageMetadata(entries.get(messageCntPerPartition).getDataBuffer());
            if (endAction) {
                Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
            } else {
                Assert.assertEquals(PulsarMarkers.MarkerType.TXN_ABORT_VALUE, messageMetadata.getMarkerType());
            }
        }

        Assert.assertEquals(0, messageSet.size());
        log.info("produce and {} test finished.", endAction ? "commit" : "abort");
    }

    private void checkMessageId(List<CompletableFuture<MessageId>> futureList, boolean isFinished) {
        futureList.forEach(messageIdFuture -> {
            try {
                MessageId messageId = messageIdFuture.get(5, TimeUnit.SECONDS);
                if (isFinished) {
                    Assert.assertNotNull(messageId);
                    log.info("Tnx finished success! messageId: {}", messageId);
                } else {
                    Assert.fail("MessageId shouldn't be get before txn abort.");
                }
            } catch (Exception e) {
                if (!isFinished) {
                    if (e instanceof TimeoutException) {
                        log.info("This is a expected exception.");
                    } else {
                        log.error("This exception is not expected.", e);
                        Assert.fail("This exception is not expected.");
                    }
                } else {
                    log.error("Tnx commit failed!", e);
                    Assert.fail("Tnx commit failed!");
                }
            }
        });
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            if (partition >= 0) {
                topic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            }
            return getPulsarServiceList().get(0).getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(topic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get origin topic readonly cursor.", e);
            Assert.fail("Failed to get origin topic readonly cursor.");
            return null;
        }
    }

}
