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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.event.data.ProducerDisconnectEventData;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.broker.service.TopicEventsListener.EventContext;
import org.apache.pulsar.broker.service.TopicEventsListener.EventStage;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class TopicEventsListenerTest extends BrokerTestBase {

    private final Queue<String> events = new ConcurrentLinkedQueue<>();
    volatile String topicNameToWatch;
    String namespace;

    @DataProvider(name = "topicType")
    public static Object[][] topicType() {
        return new Object[][] {
                {"persistent", "partitioned", true},
                {"persistent", "non-partitioned", true},
                {"non-persistent", "partitioned", true},
                {"non-persistent", "non-partitioned", true},
                {"persistent", "partitioned", false},
                {"persistent", "non-partitioned", false},
                {"non-persistent", "partitioned", false},
                {"non-persistent", "non-partitioned", false}
        };
    }

    @DataProvider(name = "topicTypeNoDelete")
    public static Object[][] topicTypeNoDelete() {
        return new Object[][] {
                {"persistent", "partitioned"},
                {"persistent", "non-partitioned"},
                {"non-persistent", "partitioned"},
                {"non-persistent", "non-partitioned"}
        };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        pulsar.getBrokerService().addTopicEventListener(new TopicEventsListener() {
            @Override
            public void handleEvent(String topic, TopicEvent event, EventStage stage, Throwable t) {
                log.info("got event {}__{} for topic {}", event, stage, topic);
                if (topic.equals(topicNameToWatch)) {
                    if (log.isDebugEnabled()) {
                        log.debug("got event {}__{} for topic {} with detailed stack",
                                event, stage, topic, new Exception("tracing event source"));
                    }
                    events.add(event.toString() + "__" + stage.toString());
                }
            }
        });
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    protected void setupTest() throws Exception {
        namespace = "prop/" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        assertTrue(admin.namespaces().getNamespaces("prop").contains(namespace));
        admin.namespaces().setRetention(namespace, new RetentionPolicies(3, 10));
        try (PulsarAdmin admin2 = createPulsarAdmin()) {
            Awaitility.await().untilAsserted(() ->
                    assertEquals(admin2.namespaces().getRetention(namespace), new RetentionPolicies(3, 10)));
        }

        events.clear();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setManagedLedgerMaxEntriesPerLedger(1);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(1);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanupTest() throws Exception {
        deleteNamespaceWithRetry(namespace, true);
    }

    @Test(dataProvider = "topicType")
    public void testEvents(String topicTypePersistence, String topicTypePartitioned,
                           boolean forceDelete) throws Exception {
        String topicName = topicTypePersistence + "://" + namespace + "/" + "topic-" + UUID.randomUUID();

        createTopicAndVerifyEvents(topicTypePersistence, topicTypePartitioned, topicName);

        events.clear();
        if (topicTypePartitioned.equals("partitioned")) {
            admin.topics().deletePartitionedTopic(topicName, forceDelete);
        } else {
            admin.topics().delete(topicName, forceDelete);
        }

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(events).containsAll(Arrays.asList("DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"))
        );
    }

    @Test(dataProvider = "topicType")
    public void testEventsWithUnload(String topicTypePersistence, String topicTypePartitioned,
                                     boolean forceDelete) throws Exception {
        String topicName = topicTypePersistence + "://" + namespace + "/" + "topic-" + UUID.randomUUID();

        createTopicAndVerifyEvents(topicTypePersistence, topicTypePartitioned, topicName);

        events.clear();
        admin.topics().unload(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(events.toArray()).containsAll(Arrays.asList("UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"))
        );

        events.clear();
        if (topicTypePartitioned.equals("partitioned")) {
            admin.topics().deletePartitionedTopic(topicName, forceDelete);
        } else {
            admin.topics().delete(topicName, forceDelete);
        }

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(events.toArray()).containsAll(Arrays.asList("DELETE__BEFORE",
                        "DELETE__SUCCESS"))
        );
    }

    @Test(dataProvider = "topicType", groups = "flaky")
    public void testEventsActiveSub(String topicTypePersistence, String topicTypePartitioned,
                                    boolean forceDelete) throws Exception {
        String topicName = topicTypePersistence + "://" + namespace + "/" + "topic-" + UUID.randomUUID();

        createTopicAndVerifyEvents(topicTypePersistence, topicTypePartitioned, topicName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("sub").subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            producer.send("hello".getBytes());
        }
        consumer.receive();

        events.clear();
        try {
            if (topicTypePartitioned.equals("partitioned")) {
                admin.topics().deletePartitionedTopic(topicName, forceDelete);
            } else {
                admin.topics().delete(topicName, forceDelete);
            }
        } catch (PulsarAdminException e) {
            if (forceDelete) {
                throw e;
            }
            assertTrue(e.getMessage().contains("Topic has active producers/subscriptions")
                    || e.getMessage().contains("connected producers/consumers"));
        }

        final String[] expectedEvents;

        if (forceDelete) {
            expectedEvents = new String[]{
                    "DELETE__BEFORE",
                    "POLICIES_APPLY__SUCCESS",
                    "PRODUCER_DISCONNECT__SUCCESS",
                    "UNLOAD__BEFORE",
                    "UNLOAD__SUCCESS",
                    "DELETE__SUCCESS",
            };
        } else {
            expectedEvents = new String[]{
                    "POLICIES_APPLY__SUCCESS",
                    "DELETE__BEFORE",
                    "DELETE__FAILURE"
            };
        }

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(events.toArray(new String[0])).containsAll(Arrays.stream(expectedEvents).toList());
        });

        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "topicTypeNoDelete")
    public void testTopicAutoGC(String topicTypePersistence, String topicTypePartitioned) throws Exception {
        String topicName = topicTypePersistence + "://" + namespace + "/" + "topic-" + UUID.randomUUID();

        createTopicAndVerifyEvents(topicTypePersistence, topicTypePartitioned, topicName);

        admin.namespaces().setInactiveTopicPolicies(namespace,
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        // Remove retention
        admin.namespaces().setRetention(namespace, new RetentionPolicies());
        try (PulsarAdmin admin2 = createPulsarAdmin()) {
            Awaitility.await().untilAsserted(() ->
                    assertEquals(admin2.namespaces().getRetention(namespace), new RetentionPolicies()));
        }

        events.clear();

        runGC();

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() ->
                assertThat(events.toArray()).isEqualTo(new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                })
        );
    }

    @Test
    public void testTopicEventContextSerialization() throws IOException {
        ObjectMapper objectMapper = ObjectMapperFactory.getMapper().getObjectMapper();
        EventContext eventContext = EventContext.builder()
                .brokerId("broker-1")
                .proxyRole("proxy-role")
                .clientRole("client-role")
                .topicName("persistent://prop/namespace/topic")
                .stage(EventStage.SUCCESS)
                .data(ProducerDisconnectEventData.builder()
                        .id(1)
                        .address("localhost:1234")
                        .name("abc")
                        .build())
                .build();
        byte[] bytes = objectMapper.writeValueAsBytes(eventContext);
        EventContext deserializedEventContext = objectMapper.readValue(bytes, EventContext.class);
        assertEquals(eventContext, deserializedEventContext);
    }

    private void createTopicAndVerifyEvents(String topicDomain, String topicTypePartitioned, String topicName) throws Exception {
        final String[] expectedEvents;
        if (topicDomain.equalsIgnoreCase("persistent") || topicTypePartitioned.equals("partitioned")) {
            if (topicTypePartitioned.equals("partitioned")) {
                expectedEvents = new String[]{
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOOKUP__SUCCESS",
                        "LOAD__BEFORE",
                        "LOAD__SUCCESS",
                        "PRODUCER_CONNECT__SUCCESS",
                        "PRODUCER_DISCONNECT__SUCCESS",
                        "LOOKUP__SUCCESS",
                        "CONSUMER_CONNECT__SUCCESS",
                        "CONSUMER_DISCONNECT__SUCCESS",
                };
            } else {
                expectedEvents = new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS",
                        "LOOKUP__SUCCESS",
                        "CONSUMER_CONNECT__SUCCESS",
                        "CONSUMER_DISCONNECT__SUCCESS",
                };
            }
        } else {
            expectedEvents = new String[]{
                    // Before https://github.com/apache/pulsar/pull/21995, Pulsar will skip create topic if the topic
                    //   was already exists, and the action "check topic exists" will try to load Managed ledger,
                    //   the check triggers two exrtra events: [LOAD__BEFORE, LOAD__FAILURE].
                    //   #21995 fixed this wrong behavior, so remove these two events.
                    "LOAD__BEFORE",
                    "LOAD__FAILURE",
                    "LOAD__BEFORE",
                    "CREATE__BEFORE",
                    "CREATE__SUCCESS",
                    "LOAD__SUCCESS",
                    "LOOKUP__SUCCESS",
                    "CONSUMER_CONNECT__SUCCESS",
                    "CONSUMER_DISCONNECT__SUCCESS",
            };
        }
        if (topicTypePartitioned.equals("partitioned")) {
            topicNameToWatch = topicName + "-partition-1";
            admin.topics().createPartitionedTopic(topicName, 2);
            triggerPartitionsCreation(topicName);
        } else {
            topicNameToWatch = topicName;
            admin.topics().createNonPartitionedTopic(topicName);
        }
        createConsumer(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(events.toArray()).isEqualTo(expectedEvents));
    }

    @Test(dataProvider = "topicType")
    public void testEventsOnSubscription(String topicTypePersistence, String topicTypePartitioned, boolean forceDelete)
            throws Exception {
        String topicName = topicTypePersistence + "://" + namespace + "/" + "topic-" + UUID.randomUUID();

        createTopicAndVerifyEvents(topicTypePersistence, topicTypePartitioned, topicName);

        events.clear();

        if (topicTypePersistence.equals("persistent")) {
            admin.topics().createSubscription(topicName, "test-sub", MessageId.earliest);
            admin.topics().deleteSubscription(topicName, "test-sub", forceDelete);

            Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(events.stream().toList())
                            .isEqualTo(Arrays.asList("SUBSCRIPTION_CREATE__SUCCESS", "SUBSCRIPTION_DELETE__SUCCESS"))
            );
        }
    }

    @Test
    public void testTtlAndRetentionEvent()
            throws PulsarAdminException, PulsarClientException, ExecutionException, InterruptedException {
        String topicName = TopicName.get( namespace + "/testTtlEvent-" + UUID.randomUUID()).toString();
        admin.topics().createNonPartitionedTopic(topicName);

        admin.topicPolicies().setRetention(topicName, new RetentionPolicies(0, 0));

        String subscriptionName = "testTtlEvent";
        // Create consumer for ttl
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
        consumer.close();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();
        for (int i = 0; i < 30; i++) {
            producer.send(("message-" + i).getBytes(StandardCharsets.UTF_8));
            Thread.sleep(500);
        }

        // Unload topic for trim ledgers
        admin.topics().unload(topicName);

        int ttl = 3;
        Thread.sleep(ttl * 1000);

        this.topicNameToWatch = topicName;
        events.clear();

        CompletableFuture<Optional<Topic>> topicIfExists = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertThat(topicIfExists).succeedsWithin(3, TimeUnit.SECONDS);
        Optional<Topic> topic = topicIfExists.get();
        assertThat(topic).isPresent();
        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subscriptionName);
        subscription.expireMessages(ttl);
        Awaitility.await().untilAsserted(() -> assertThat(events).contains(
                "MESSAGE_EXPIRE__SUCCESS"
        ));
        pulsar.getBrokerService().checkConsumedLedgers();
        Awaitility.await().untilAsserted(() -> assertThat(events).contains(
                "MESSAGE_PURGE__SUCCESS"
        ));
    }

    private PulsarAdmin createPulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString())
                .build();
    }

    private void createConsumer(String topicName) throws PulsarClientException {
        Consumer<byte[]> consumer =
                pulsarClient.newConsumer().topic(topicName).subscriptionMode(SubscriptionMode.NonDurable)
                        .subscriptionName("create-consumer").subscribe();
        consumer.close();
    }

    private void triggerPartitionsCreation(String topicName) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        producer.close();
    }

}
