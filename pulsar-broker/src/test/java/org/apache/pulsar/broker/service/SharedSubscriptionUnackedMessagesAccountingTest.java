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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumersClassic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Consumer removal must debit both subscription and broker unacknowledged-message counters exactly once.
 * Each dispatcher variant owns its broker so configuration changes cannot affect the shared test cluster.
 */
@Test(groups = "broker-api")
public class SharedSubscriptionUnackedMessagesAccountingTest extends ProducerConsumerBase {
    private static final String SUBSCRIPTION = "shared-churn-sub";
    private static final int UNACKED_MESSAGES = 10;
    private final boolean classic;

    @Factory
    public static Object[] createTestInstances() {
        return new Object[] {new SharedSubscriptionUnackedMessagesAccountingTest(false),
                new SharedSubscriptionUnackedMessagesAccountingTest(true)};
    }

    public SharedSubscriptionUnackedMessagesAccountingTest(boolean classic) {
        this.classic = classic;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setSubscriptionSharedUseClassicPersistentImplementation(classic);
        conf.setSubscriptionKeySharedUseClassicPersistentImplementation(classic);
        conf.setMaxUnackedMessagesPerBroker(1000);
    }

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testRemovingSameConsumerTwiceDebitsUnackedMessagesOnce() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> departing = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName)
                     .subscriptionName(SUBSCRIPTION)
                     .subscriptionType(SubscriptionType.Shared)
                     .consumerName("departing")
                     .receiverQueueSize(5)
                     .subscribe()) {
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send("unacked-" + i);
            }
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                assertThat(departing.receive(2, TimeUnit.SECONDS))
                        .as("delivery %s to leave unacknowledged", i).isNotNull();
            }

            BrokerService brokerService = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) brokerService.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            assertThat(dispatcher).as("configured dispatcher implementation").isInstanceOf(classic
                    ? PersistentDispatcherMultipleConsumersClassic.class : PersistentDispatcherMultipleConsumers.class);
            Consumer brokerConsumer = dispatcher.getConsumers().get(0);

            // Serialize with dispatch so both aggregate credits have completed before checking their values.
            synchronized (dispatcher) {
                assertThat(brokerConsumer.getUnackedMessages()).as("departing consumer balance")
                        .isEqualTo(UNACKED_MESSAGES);
                assertThat(dispatcher.getTotalUnackedMessages()).as("subscription balance before removal")
                        .isEqualTo(UNACKED_MESSAGES);
                assertThat(brokerService.getTotalUnackedMessages()).as("broker balance before removal")
                        .isEqualTo(UNACKED_MESSAGES);

                // With no survivor there is no replay to race with these assertions. Checking the broker as well
                // also rejects a fix that merely resets the subscription counter when the last consumer leaves.
                dispatcher.removeConsumer(brokerConsumer);
                assertUnackedMessagesCleared(dispatcher, brokerService, "first removal");
                dispatcher.removeConsumer(brokerConsumer);
                assertUnackedMessagesCleared(dispatcher, brokerService, "repeated removal");
            }
        }
    }

    @DataProvider
    public Object[][] subscriptionTypes() {
        return new Object[][] {{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 60_000)
    public void testStaleRemovalDoesNotRemoveEqualReplacement(SubscriptionType subscriptionType) throws Exception {
        String topicName = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).enableBatching(false).create();
             org.apache.pulsar.client.api.Consumer<String> client = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName).subscriptionName(SUBSCRIPTION).subscriptionType(subscriptionType)
                     .consumerName("replacement-test").subscribe()) {
            producer.send("unacked");
            assertThat(client.receive(5, TimeUnit.SECONDS)).isNotNull();
            BrokerService brokerService = pulsar.getBrokerService();
            PersistentTopic topic = (PersistentTopic) brokerService.getTopicReference(topicName).orElseThrow();
            AbstractPersistentDispatcherMultipleConsumers dispatcher =
                    (AbstractPersistentDispatcherMultipleConsumers) topic.getSubscription(SUBSCRIPTION).getDispatcher();
            Consumer original = dispatcher.getConsumers().get(0);
            synchronized (dispatcher) {
                assertThat(original.getUnackedMessages()).isEqualTo(1);
                dispatcher.removeConsumer(original);
                assertUnackedMessagesCleared(dispatcher, brokerService, "original removal");
            }
            // Use the production subscribe path and the same live connection/protocol identity. No mocked
            // consumer or manually populated dispatcher collections are needed to create the replacement.
            Consumer replacement = topic.subscribe(SubscriptionOption.builder()
                    .cnx(original.cnx()).consumerId(original.consumerId()).consumerName(original.consumerName())
                    .subscriptionName(SUBSCRIPTION).subType(original.subType()).isDurable(true)
                    .startMessageId(MessageId.latest).initialPosition(InitialPosition.Latest)
                    .metadata(Collections.emptyMap()).subscriptionProperties(Optional.empty())
                    .keySharedMeta(new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT))
                    .build()).get(10, TimeUnit.SECONDS);
            try {
                assertThat(replacement).isNotSameAs(original).isEqualTo(original);
                assertThat(replacement.hashCode()).isEqualTo(original.hashCode());
                synchronized (dispatcher) {
                    dispatcher.removeConsumer(original);
                    assertThat(dispatcher.getConsumers()).singleElement().isSameAs(replacement);
                    assertUnackedMessagesCleared(dispatcher, brokerService, "stale removal");
                }
                // This also checks Key_Shared selector membership, not just the consumer list.
                replacement.flowPermits(1);
                assertThat(client.receive(5, TimeUnit.SECONDS)).as("replacement receives replay").isNotNull();
                synchronized (dispatcher) {
                    assertThat(replacement.getUnackedMessages()).isEqualTo(1);
                    assertThat(dispatcher.getTotalUnackedMessages()).isEqualTo(1);
                    assertThat(brokerService.getTotalUnackedMessages()).isEqualTo(1);
                }
            } finally {
                replacement.close();
            }
            assertUnackedMessagesCleared(dispatcher, brokerService, "replacement removal");
        }
    }

    private void assertUnackedMessagesCleared(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                              BrokerService brokerService, String removal) {
        assertThat(dispatcher.getTotalUnackedMessages()).as("subscription balance after %s", removal).isZero();
        assertThat(brokerService.getTotalUnackedMessages()).as("broker balance after %s", removal).isZero();
    }
}
