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

import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

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
    private final static String ACK_COMMIT_TOPIC = NAMESPACE1 + "/ack-commit";
    private final static String ACK_ABORT_TOPIC = NAMESPACE1 + "/ack-abort";

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
        admin.topics().createPartitionedTopic(ACK_COMMIT_TOPIC, 3);
        admin.topics().createPartitionedTopic(ACK_ABORT_TOPIC, 3);

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

    private void checkMessageId(List<CompletableFuture<MessageId>> futureList, boolean isFinished) {
        futureList.forEach(messageIdFuture -> {
            try {
                MessageId messageId = messageIdFuture.get(1, TimeUnit.SECONDS);
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

    // TODO flaky https://github.com/apache/pulsar/issues/8070
    // @Test
    public void ackCommitTest() throws Exception {
        final String subscriptionName = "ackCommitTest";
        Transaction txn = ((PulsarClientImpl) pulsarClient)
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        log.info("init transaction {}.", txn);

        Producer<byte[]> incomingProducer = pulsarClient.newProducer()
                .topic(ACK_COMMIT_TOPIC)
                .batchingMaxMessages(1)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();
        int incomingMessageCnt = 10;
        for (int i = 0; i < incomingMessageCnt; i++) {
            incomingProducer.newMessage().value("Hello Txn.".getBytes()).sendAsync();
        }
        log.info("prepare incoming messages finished.");

        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(ACK_COMMIT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        Thread.sleep(1000);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.commit().get();

        Thread.sleep(1000);

        // After commit, the pending messages count should be 0
        Assert.assertEquals(getPendingAckCount(ACK_COMMIT_TOPIC, subscriptionName), 0);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        log.info("finish test ackCommitTest");
    }

    // TODO flaky https://github.com/apache/pulsar/issues/8070
    // @Test
    public void ackAbortTest() throws Exception {
        final String subscriptionName = "ackAbortTest";
        Transaction txn = ((PulsarClientImpl) pulsarClient)
                .newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        log.info("init transaction {}.", txn);

        Producer<byte[]> incomingProducer = pulsarClient.newProducer()
                .topic(ACK_ABORT_TOPIC)
                .batchingMaxMessages(1)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();
        int incomingMessageCnt = 10;
        for (int i = 0; i < incomingMessageCnt; i++) {
            incomingProducer.newMessage().value("Hello Txn.".getBytes()).sendAsync();
        }
        log.info("prepare incoming messages finished.");

        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(ACK_ABORT_TOPIC)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            log.info("receive messageId: {}", message.getMessageId());
            consumer.acknowledgeAsync(message.getMessageId(), txn);
        }

        Thread.sleep(1000);

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }

        // The pending messages count should be the incomingMessageCnt
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), incomingMessageCnt);

        txn.abort().get();

        Thread.sleep(1000);

        // After commit, the pending messages count should be 0
        Assert.assertEquals(getPendingAckCount(ACK_ABORT_TOPIC, subscriptionName), 0);

        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < incomingMessageCnt; i++) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            log.info("second receive messageId: {}", message.getMessageId());
        }

        log.info("finish test ackAbortTest");
    }

    private int getPendingAckCount(String topic, String subscriptionName) throws Exception {
        Class<PersistentSubscription> clazz = PersistentSubscription.class;
        Field field = clazz.getDeclaredField("pendingAckMessages");
        field.setAccessible(true);

        int pendingAckCount = 0;
        for (PulsarService pulsarService : getPulsarServiceList()) {
            for (String key : pulsarService.getBrokerService().getTopics().keys()) {
                if (key.contains(topic)) {
                    PersistentSubscription subscription =
                            (PersistentSubscription) pulsarService.getBrokerService()
                                    .getTopics().get(key).get().get().getSubscription(subscriptionName);
                    ConcurrentOpenHashSet<Position> set = (ConcurrentOpenHashSet<Position>) field.get(subscription);
                    if (set != null) {
                        pendingAckCount += set.size();
                    }
                }
            }
        }
        log.info("subscriptionName: {}, pendingAckCount: {}", subscriptionName, pendingAckCount);
        return pendingAckCount;
    }


}
