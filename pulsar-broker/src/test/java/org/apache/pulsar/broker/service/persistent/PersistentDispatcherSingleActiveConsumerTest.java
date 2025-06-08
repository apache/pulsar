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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherSingleActiveConsumerTest extends ProducerConsumerBase {
    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
//        serviceConfiguration.setTopicLevelPoliciesEnabled(false);
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
        super.internalSetup(serviceConfiguration);
        super.producerBaseSetup();

        // Setup namespaces
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));
    }

    @DataProvider(name = "enableBatching")
    public static Object[][] enableBatching() {
        return new Object[][]{
                {true},
                {false}
        };
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSkipReadEntriesFromCloseCursor() throws Exception {
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://public/default/testSkipReadEntriesFromCloseCursor");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            producer.send("message-" + i);
        }
        producer.close();

        // Get the dispatcher of the topic.
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).join().get();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(subscription).when(cursor).getName();
        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(sub).getTopic();
        // Mock the dispatcher.
        PersistentDispatcherSingleActiveConsumer dispatcher =
                Mockito.spy(new PersistentDispatcherSingleActiveConsumer(cursor, CommandSubscribe.SubType.Exclusive, 0,
                        topic, sub));

        // Mock a consumer
        Consumer consumer = Mockito.mock(Consumer.class);
        consumer.getAvailablePermits();
        Mockito.doReturn(10).when(consumer).getAvailablePermits();
        Mockito.doReturn(10).when(consumer).getAvgMessagesPerEntry();
        Mockito.doReturn("test").when(consumer).consumerName();
        Mockito.doReturn(true).when(consumer).isWritable();
        Mockito.doReturn(false).when(consumer).readCompacted();

        // Make the consumer as the active consumer.
        Mockito.doReturn(consumer).when(dispatcher).getActiveConsumer();

        // Make the count + 1 when call the scheduleReadEntriesWithDelay(...).
        AtomicInteger callScheduleReadEntriesWithDelayCnt = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callScheduleReadEntriesWithDelayCnt.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).scheduleReadEntriesWithDelay(Mockito.eq(consumer), Mockito.anyLong());

        // Make the count + 1 when call the readEntriesFailed(...).
        AtomicInteger callReadEntriesFailed = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callReadEntriesFailed.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).readEntriesFailed(Mockito.any(), Mockito.any());

        Mockito.doReturn(false).when(cursor).isClosed();

        // Mock the readEntriesOrWait(...) to simulate the cursor is closed.
        Mockito.doAnswer(inv -> {
            PersistentDispatcherSingleActiveConsumer dispatcher1 = inv.getArgument(2);
            dispatcher1.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                    null);
            return null;
        }).when(cursor).asyncReadEntriesOrWait(Mockito.anyInt(), Mockito.anyLong(), Mockito.eq(dispatcher),
                Mockito.any(), Mockito.any());

        dispatcher.readMoreEntries(consumer);

        // Verify: the readEntriesFailed should be called once and the scheduleReadEntriesWithDelay should not be
        // called.
        Assert.assertTrue(callReadEntriesFailed.get() == 1 && callScheduleReadEntriesWithDelayCnt.get() == 0);

        // Verify: the topic can be deleted successfully.
        admin.topics().delete(topicName, false);
    }

    @Test(dataProvider = "enableBatching")
    public void testUnackedMessages(boolean enableBatching) throws Exception {
        long timeMillis = System.currentTimeMillis();
        String topicNamePrefix = "testUnackedMessages-" + timeMillis;
        String subscriptionNamePrefix = "testUnackedMessages-sub-" + timeMillis;
        int totalProducedMessage = 100;
        // we will check unacked messages every 20 messages
        int checkStep = 20;

        for (SubscriptionType subscription : List.of(SubscriptionType.Exclusive, SubscriptionType.Failover)) {
            String topicName = topicNamePrefix + subscription.name() + "-" + enableBatching;
            String subscriptionName = subscriptionNamePrefix + subscription.name();
            @Cleanup
            org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName).subscriptionName(subscriptionName)
                    .subscriptionType(subscription)
                    .receiverQueueSize(totalProducedMessage)
                    .ackTimeout(1000, TimeUnit.MILLISECONDS)
                    .negativeAckRedeliveryDelay(0, TimeUnit.SECONDS)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .maxAcknowledgmentGroupSize(1)
                    .subscribe();

            @Cleanup
            Producer<String> producer =
                    pulsarClient.newProducer(Schema.STRING).enableBatching(enableBatching).topic(topicName).create();

            for (int i = 0; i < totalProducedMessage; ++i) {
                producer.sendAsync(Integer.toString(i));
            }

            // 1) check unacked message at begin
            checkUnackedMessage(topicName, subscriptionName, totalProducedMessage);

            // 2) check unacked message every checkStep
            for (int i = 0; i < totalProducedMessage; ++i) {
                Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
                consumer.acknowledge(msg.getMessageId());
                if ((i + 1) % checkStep == 0) {
                    checkUnackedMessage(topicName, subscriptionName, totalProducedMessage - i - 1);
                }
            }

            // 3) check unacked message at end
            checkUnackedMessage(topicName, subscriptionName, 0);

            // 4) test negative ack
            for (int i = 0; i < totalProducedMessage; ++i) {
                producer.sendAsync(Integer.toString(i));
            }
            Message<String> msg = null;
            checkUnackedMessage(topicName, subscriptionName, totalProducedMessage);
            for (int i = 0; i < totalProducedMessage; ++i) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
            }
            consumer.negativeAcknowledge(msg);
            // sleep for a while to wait for the negative ack to be processed
            Thread.sleep(1000);
            TopicStats topicStats = admin.topics().getStats(topicName);
            assertThat(topicStats.getSubscriptions().get(subscriptionName).getUnackedMessages()).isEqualTo(
                    totalProducedMessage);

            // 5) test cumulative ack or negative ack mode
            consumer.acknowledgeCumulative(msg);
            checkUnackedMessage(topicName, subscriptionName, 0);

            // 6) after cumulation ack, consumer can receive message again
            for (int i = 0; i < totalProducedMessage; ++i) {
                producer.sendAsync(Integer.toString(i));
            }

            for (int i = 0; i < totalProducedMessage; ++i) {
                msg = consumer.receive(5, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
            }
            checkUnackedMessage(topicName, subscriptionName, totalProducedMessage);
        }
    }

    @Test(dataProvider = "enableBatching")
    public void testUnackedMessagesWithTransaction(boolean enableBatching) throws Exception {
        long timeMillis = System.currentTimeMillis();
        String topicNamePrefix = "testUnackedMessagesWithTrn-" + timeMillis;
        String subscriptionNamePrefix = "testUnackedMessagesWithTrn-sub-" + timeMillis;
        int totalProducedMessage = 100;

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .enableTransaction(true)
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .connectionsPerBroker(100)
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        for (SubscriptionType subscription : List.of(SubscriptionType.Exclusive, SubscriptionType.Failover)) {
            String inputTopicName = topicNamePrefix + subscription.name() + "-input-" + enableBatching;
            String outputTopicName = topicNamePrefix + subscription.name() + "-output-" + enableBatching;
            String subscriptionName = subscriptionNamePrefix + subscription.name();
            @Cleanup
            org.apache.pulsar.client.api.Consumer<String> inputConsumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(inputTopicName).subscriptionName(subscriptionName)
                    .subscriptionType(subscription)
                    .receiverQueueSize(totalProducedMessage)
                    .ackTimeout(1000, TimeUnit.MILLISECONDS)
                    .negativeAckRedeliveryDelay(0, TimeUnit.SECONDS)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .maxAcknowledgmentGroupSize(1)
                    .subscribe();
            @Cleanup
            org.apache.pulsar.client.api.Consumer<String> outputConsumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(outputTopicName).subscriptionName(subscriptionName)
                    .subscriptionType(subscription)
                    .receiverQueueSize(totalProducedMessage)
                    .ackTimeout(1000, TimeUnit.MILLISECONDS)
                    .negativeAckRedeliveryDelay(0, TimeUnit.SECONDS)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .maxAcknowledgmentGroupSize(1)
                    .subscribe();

            @Cleanup
            Producer<String> inputProducer =
                    pulsarClient.newProducer(Schema.STRING).enableBatching(enableBatching).topic(inputTopicName)
                            .create();

            @Cleanup
            Producer<String> outputProducer =
                    pulsarClient.newProducer(Schema.STRING).enableBatching(enableBatching).topic(outputTopicName)
                            .create();

            for (int i = 0; i < totalProducedMessage; ++i) {
                inputProducer.sendAsync(Integer.toString(i));
            }

            // 1) check unacked message for inputTopic before transaction
            checkUnackedMessage(inputTopicName, subscriptionName, totalProducedMessage);

            Transaction trn =
                    pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();

            // 2) check unacked message after transaction committed
            for (int i = 0; i < totalProducedMessage; ++i) {
                Message<String> msg = inputConsumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
                outputProducer.newMessage(trn).value(msg.getValue()).sendAsync();
                inputConsumer.acknowledgeAsync(msg.getMessageId(), trn);
            }
            trn.commit().get();

            checkUnackedMessage(inputTopicName, subscriptionName, 0);
            checkUnackedMessage(outputTopicName, subscriptionName, totalProducedMessage);

            // 3) check unacked message after transaction aborted
            trn = pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            for (int i = 0; i < totalProducedMessage; ++i) {
                Message<String> msg = outputConsumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
                outputConsumer.acknowledgeAsync(msg.getMessageId(), trn);
            }

            trn.abort().get();
            checkUnackedMessage(outputTopicName, subscriptionName, totalProducedMessage);

            // 4) check unacked message after transaction commit
            trn = pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            for (int i = 0; i < totalProducedMessage; ++i) {
                Message<String> msg = outputConsumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
                outputConsumer.acknowledgeAsync(msg.getMessageId(), trn);
            }

            trn.commit().get();
            checkUnackedMessage(outputTopicName, subscriptionName, 0);

            // 5) check cumulative ack
            for (int i = 0; i < totalProducedMessage; ++i) {
                outputProducer.sendAsync(Integer.toString(i));
            }

            Message<String> msg = null;
            for (int i = 0; i < totalProducedMessage; ++i) {
                msg = outputConsumer.receive(1, TimeUnit.SECONDS);
                if (Objects.isNull(msg)) {
                    Assert.fail("Expected message not received");
                }
            }
            trn = pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            outputConsumer.acknowledgeCumulativeAsync(msg.getMessageId(), trn);
            trn.abort().get();
            // broker won't redelivery message after  transaction abort.
            // refer to: https://github.com/apache/pulsar/pull/14371
            outputConsumer.redeliverUnacknowledgedMessages();
            checkUnackedMessage(outputTopicName, subscriptionName, totalProducedMessage);

            trn = pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            outputConsumer.acknowledgeCumulativeAsync(msg.getMessageId(), trn);
            trn.commit().get();
            checkUnackedMessage(outputTopicName, subscriptionName, 0);
        }
    }


    private void checkUnackedMessage(String topicName, String subscriptionName, int expectedUnackedMessage) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getStats(topicName);
            assertThat(topicStats.getSubscriptions().get(subscriptionName).getUnackedMessages()).isEqualTo(
                    expectedUnackedMessage);
        });
    }

}
