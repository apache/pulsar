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
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class TopicEventsListenerTest extends BrokerTestBase {

    final static Queue<String> events = new ConcurrentLinkedQueue<>();
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

    @Test(dataProvider = "topicType")
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
                    "UNLOAD__BEFORE",
                    "UNLOAD__SUCCESS",
                    "DELETE__SUCCESS",
            };
        } else {
            expectedEvents = new String[]{
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

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                })
        );
    }

    private void createTopicAndVerifyEvents(String topicDomain, String topicTypePartitioned, String topicName) throws Exception {
        final String[] expectedEvents;
        if (topicDomain.equalsIgnoreCase("persistent") || topicTypePartitioned.equals("partitioned")) {
            if (topicTypePartitioned.equals("partitioned")) {
                expectedEvents = new String[]{
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                };
            } else {
                expectedEvents = new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
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
                    "LOAD__SUCCESS"
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

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(events.toArray()).containsAll(Arrays.stream(expectedEvents).toList()));
    }

    private PulsarAdmin createPulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString())
                .build();
    }

    private void triggerPartitionsCreation(String topicName) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        producer.close();
    }

}
