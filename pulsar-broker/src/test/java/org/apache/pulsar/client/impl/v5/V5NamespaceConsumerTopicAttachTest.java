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
package org.apache.pulsar.client.impl.v5;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ScalableTopicsWatcherSession;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * How a namespace consumer attaches the topics of its matching set when attaching one doesn't simply
 * succeed:
 * <ul>
 *   <li>a topic whose first attach fails is retried in the background and attached once it succeeds,
 *       including a topic of the initial snapshot;</li>
 *   <li>a topic that leaves the matching set while it is being attached, because its properties stop
 *       matching the filter or because it is deleted, is not attached once the attach completes, and is
 *       not retried.</li>
 * </ul>
 *
 * <p>The tests inject the faults into the segment subscriptions of the v4 client the consumer runs on.
 */
public class V5NamespaceConsumerTopicAttachTest extends V5ClientBaseTest {

    /**
     * A v4 client that fails the next segment subscription to the topics the test picks, or holds them back
     * until the test allows them. It records the segment subscriptions and lookups of each scalable topic,
     * and keeps the namespace watchers registered on its connections.
     */
    @CustomLog
    private static final class FaultyClient extends PulsarClientImpl {

        private final List<ScalableTopicsWatcher> watchers;
        private final Set<String> failingTopics = ConcurrentHashMap.newKeySet();
        private final Set<String> heldTopics = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<Void> attachAllowed = new CompletableFuture<>();
        private final Map<String, AtomicInteger> attachRequests = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> settledAttaches = new ConcurrentHashMap<>();
        private final Map<String, List<Consumer<?>>> segmentConsumers = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> lookups = new ConcurrentHashMap<>();

        static FaultyClient create(String serviceUrl) throws Exception {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setServiceUrl(serviceUrl);
            conf.setStatsIntervalSeconds(0);
            EventLoopGroup eventLoopGroup =
                    EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("faulty-client-io"));
            List<ScalableTopicsWatcher> watchers = new CopyOnWriteArrayList<>();
            ConnectionPool connectionPool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoopGroup,
                    () -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                        @Override
                        public void registerScalableTopicsWatcher(long watchId,
                                                                  ScalableTopicsWatcherSession watcher) {
                            watchers.add((ScalableTopicsWatcher) watcher);
                            super.registerScalableTopicsWatcher(watchId, watcher);
                        }
                    }, null);
            return new FaultyClient(conf, eventLoopGroup, connectionPool, watchers);
        }

        private FaultyClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                             ConnectionPool connectionPool, List<ScalableTopicsWatcher> watchers)
                throws Exception {
            super(conf, eventLoopGroup, connectionPool);
            this.watchers = watchers;
        }

        void failNextAttach(String topic) {
            failingTopics.add(topic);
        }

        void hold(String topic) {
            heldTopics.add(topic);
        }

        void allowAttach() {
            attachAllowed.complete(null);
        }

        /** Fail the segment subscriptions held back so far, and any later one to a held topic. */
        void failHeldAttaches() {
            attachAllowed.completeExceptionally(
                    new PulsarClientException.ConnectException("Injected segment subscribe failure"));
        }

        /** How many segment subscriptions to the topic were requested, whether held, failed or not. */
        int attachRequests(String topic) {
            return count(attachRequests, topic);
        }

        /** How many segment subscriptions to the topic completed, successfully or not. */
        int settledAttaches(String topic) {
            return count(settledAttaches, topic);
        }

        /** How many times the client looked up the scalable topic. */
        int lookups(String topic) {
            return count(lookups, topic);
        }

        /** The scalable topics with a segment subscription still open. */
        Set<String> attachedTopics() {
            Set<String> attached = new HashSet<>();
            segmentConsumers.forEach((topic, consumers) -> {
                if (consumers.stream().anyMatch(Consumer::isConnected)) {
                    attached.add(topic);
                }
            });
            return attached;
        }

        /** The watcher of the namespace consumer: the only one this client opens. */
        ScalableTopicsWatcher watcher() {
            Awaitility.await().until(() -> !watchers.isEmpty());
            return watchers.get(0);
        }

        private static int count(Map<String, AtomicInteger> counters, String topic) {
            AtomicInteger counter = counters.get(topic);
            return counter == null ? 0 : counter.get();
        }

        private static void increment(Map<String, AtomicInteger> counters, String topic) {
            counters.computeIfAbsent(topic, __ -> new AtomicInteger()).incrementAndGet();
        }

        // The v4 Schema is qualified: the V5 one is imported.
        @Override
        public <T> CompletableFuture<Consumer<T>> subscribeSegmentAsync(
                ConsumerConfigurationData<T> conf, org.apache.pulsar.client.api.Schema<T> schema) {
            String topic = SegmentTopicName.getParentTopicName(TopicName.get(conf.getSingleTopic())).toString();
            increment(attachRequests, topic);
            CompletableFuture<Consumer<T>> attach;
            if (failingTopics.remove(topic)) {
                attach = CompletableFuture.failedFuture(
                        new PulsarClientException.ConnectException("Injected segment subscribe failure"));
            } else {
                CompletableFuture<Void> allowed =
                        heldTopics.contains(topic) ? attachAllowed : CompletableFuture.completedFuture(null);
                attach = allowed.thenCompose(__ -> super.subscribeSegmentAsync(conf, schema))
                        .thenApply(consumer -> {
                            segmentConsumers.computeIfAbsent(topic, __ -> new CopyOnWriteArrayList<>())
                                    .add(consumer);
                            return consumer;
                        });
            }
            return attach.whenComplete((__, ___) -> increment(settledAttaches, topic));
        }

        @Override
        public CompletableFuture<ClientCnx> getConnection(String topic) {
            increment(lookups, topic);
            return super.getConnection(topic);
        }

        // The connection pool and its I/O threads were passed in, so the client doesn't close them.
        @Override
        public CompletableFuture<Void> closeAsync() {
            return super.closeAsync().whenComplete((__, ___) -> {
                try {
                    getCnxPool().close();
                } catch (Exception e) {
                    log.warn().exception(e).log("Failed to close the connection pool");
                }
                eventLoopGroup.shutdownGracefully();
            });
        }
    }

    @Test
    public void testQueueConsumerRetriesTopicWhoseFirstAttachFailed() throws Exception {
        String healthy = newScalableTopic(1);
        String failing = newScalableTopic(1);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        faulty.failNextAttach(failing);

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Awaitility.await().untilAsserted(() ->
                assertThat(((MultiTopicQueueConsumer<String>) consumer).attachedTopicsForTesting())
                        .as("topics the consumer is attached to")
                        .containsExactlyInAnyOrder(healthy, failing));
        send(failing, "after-failed-attach");
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("message sent to the topic whose first attach failed").isNotNull();
        assertThat(msg.value()).isEqualTo("after-failed-attach");
    }

    @Test
    public void testStreamConsumerRetriesTopicWhoseFirstAttachFailed() throws Exception {
        String healthy = newScalableTopic(1);
        String failing = newScalableTopic(1);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        faulty.failNextAttach(failing);

        @Cleanup
        StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Awaitility.await().untilAsserted(() ->
                assertThat(faulty.attachedTopics())
                        .as("topics the consumer is attached to")
                        .containsExactlyInAnyOrder(healthy, failing));
        send(failing, "after-failed-attach");
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("message sent to the topic whose first attach failed").isNotNull();
        assertThat(msg.value()).isEqualTo("after-failed-attach");
    }

    @Test
    public void testQueueConsumerDropsTopicThatLeftTheFilterWhileAttaching() throws Exception {
        Map<String, String> filter = Map.of("team", "a");
        String kept = newScalableTopic(filter);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .namespace(getNamespace(), filter)
                .subscriptionName("sub")
                .subscribe();
        MultiTopicQueueConsumer<String> namespaceConsumer = (MultiTopicQueueConsumer<String>) consumer;

        String leaving = holdAttachOfNewTopic(faulty, filter);
        setProperties(leaving, Map.of("team", "b"));
        awaitRemovedFromMatchingSet(faulty, leaving);
        faulty.allowAttach();
        Awaitility.await().until(() -> faulty.settledAttaches(leaving) > 0);

        Awaitility.await().atMost(15, SECONDS).during(3, SECONDS).untilAsserted(() -> {
            assertThat(namespaceConsumer.attachedTopicsForTesting())
                    .as("topics the consumer is attached to")
                    .containsExactly(kept);
            assertThat(faulty.attachedTopics())
                    .as("topics with a segment subscription open")
                    .containsExactly(kept);
        });
    }

    @Test
    public void testStreamConsumerDropsTopicThatLeftTheFilterWhileAttaching() throws Exception {
        Map<String, String> filter = Map.of("team", "a");
        String kept = newScalableTopic(filter);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        @Cleanup
        StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace(), filter)
                .subscriptionName("sub")
                .subscribe();

        String leaving = holdAttachOfNewTopic(faulty, filter);
        setProperties(leaving, Map.of("team", "b"));
        awaitRemovedFromMatchingSet(faulty, leaving);
        faulty.allowAttach();
        Awaitility.await().until(() -> faulty.settledAttaches(leaving) > 0);

        Awaitility.await().atMost(15, SECONDS).during(3, SECONDS).untilAsserted(() ->
                assertThat(faulty.attachedTopics())
                        .as("topics with a segment subscription open")
                        .containsExactly(kept));
    }

    @Test
    public void testQueueConsumerStopsAttachingTopicDeletedWhileAttaching() throws Exception {
        String kept = newScalableTopic(1);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscribe();
        MultiTopicQueueConsumer<String> namespaceConsumer = (MultiTopicQueueConsumer<String>) consumer;

        String deleted = holdAttachOfNewTopic(faulty, Map.of());
        admin.scalableTopics().deleteScalableTopic(deleted, true);
        awaitRemovedFromMatchingSet(faulty, deleted);
        int lookupsAtRemoval = faulty.lookups(deleted);
        faulty.allowAttach();
        Awaitility.await().until(() -> faulty.settledAttaches(deleted) > 0);

        Awaitility.await().atMost(15, SECONDS).during(3, SECONDS).untilAsserted(() -> {
            assertThat(faulty.lookups(deleted))
                    .as("lookups of the deleted topic since it was removed")
                    .isEqualTo(lookupsAtRemoval);
            assertThat(namespaceConsumer.attachedTopicsForTesting())
                    .as("topics the consumer is attached to")
                    .containsExactly(kept);
            assertThat(faulty.attachedTopics())
                    .as("topics with a segment subscription open")
                    .containsExactly(kept);
        });
        assertThatThrownBy(() -> admin.scalableTopics().getMetadata(deleted))
                .as("the deleted topic stays deleted")
                .isInstanceOf(PulsarAdminException.NotFoundException.class);
    }

    @Test
    public void testStreamConsumerDoesNotRetryTopicThatLeftTheFilterWhileAttaching() throws Exception {
        Map<String, String> filter = Map.of("team", "a");
        String kept = newScalableTopic(filter);
        PulsarClientV5 client = newFaultyClient();
        FaultyClient faulty = (FaultyClient) client.v4Client();
        @Cleanup
        StreamConsumer<String> consumer = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace(), filter)
                .subscriptionName("sub")
                .subscribe();

        String leaving = holdAttachOfNewTopic(faulty, filter);
        setProperties(leaving, Map.of("team", "b"));
        awaitRemovedFromMatchingSet(faulty, leaving);
        int lookupsAtRemoval = faulty.lookups(leaving);
        faulty.failHeldAttaches();
        Awaitility.await().until(() -> faulty.settledAttaches(leaving) > 0);

        Awaitility.await().atMost(15, SECONDS).during(3, SECONDS).untilAsserted(() -> {
            assertThat(faulty.lookups(leaving))
                    .as("lookups of the topic since it left the matching set")
                    .isEqualTo(lookupsAtRemoval);
            assertThat(faulty.attachedTopics())
                    .as("topics with a segment subscription open")
                    .containsExactly(kept);
        });
    }

    // --- Helpers ---

    private PulsarClientV5 newFaultyClient() throws Exception {
        return track(new PulsarClientV5(FaultyClient.create(getBrokerServiceUrl()), "faulty-client", null));
    }

    private String newScalableTopicName() {
        return "topic://" + getNamespace() + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String newScalableTopic(Map<String, String> properties) throws Exception {
        String topic = newScalableTopicName();
        admin.scalableTopics().createScalableTopic(topic, 1, properties);
        return topic;
    }

    /**
     * Create a topic in the consumer's matching set while holding back its segment subscriptions, and wait
     * until the consumer is attaching it.
     */
    private String holdAttachOfNewTopic(FaultyClient faulty, Map<String, String> properties) throws Exception {
        String topic = newScalableTopicName();
        faulty.hold(topic);
        admin.scalableTopics().createScalableTopic(topic, 1, properties);
        Awaitility.await().until(() -> faulty.attachRequests(topic) > 0);
        return topic;
    }

    /** The admin API can't change the properties of a scalable topic: change its metadata directly. */
    private void setProperties(String topic, Map<String, String> properties) throws Exception {
        getPulsar().getPulsarResources().getScalableTopicResources()
                .updateScalableTopicAsync(TopicName.get(topic), metadata -> {
                    metadata.setProperties(properties);
                    return metadata;
                })
                .get(30, SECONDS);
    }

    /** Wait until the consumer's watcher has received the removal of the topic from the matching set. */
    private static void awaitRemovedFromMatchingSet(FaultyClient faulty, String topic) {
        ScalableTopicsWatcher watcher = faulty.watcher();
        Awaitility.await().until(() -> !watcher.currentSetForTesting().contains(topic));
    }

    private void send(String topic, String value) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage()
                .value(value)
                .send();
    }
}
