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
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Consumer;
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
 * A scalable topic created or deleted in a namespace while a namespace consumer is being created must be
 * attached or detached once the consumer exists. The broker reports each change once, and the consumer
 * follows the changes only after it has subscribed to every topic of the initial snapshot.
 *
 * <p>The tests hold back the segment subscriptions of a topic in the initial snapshot, change the
 * namespace, and wait until the consumer's watcher has received the change before letting the creation
 * finish.
 */
public class V5NamespaceConsumerTopicChangeDuringCreateTest extends V5ClientBaseTest {

    /**
     * A v4 client that holds back the segment subscriptions to the topics the test picks until it allows
     * them, records the scalable topics it subscribes to, and keeps the namespace watchers registered on
     * its connections.
     */
    @CustomLog
    private static final class HoldingClient extends PulsarClientImpl {

        private final List<ScalableTopicsWatcher> watchers;
        private final Set<String> heldTopics = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<Void> attachAllowed = new CompletableFuture<>();
        private final List<String> attachRequests = new CopyOnWriteArrayList<>();
        private final Set<String> attachedTopics = ConcurrentHashMap.newKeySet();

        static HoldingClient create(String serviceUrl) throws Exception {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setServiceUrl(serviceUrl);
            conf.setStatsIntervalSeconds(0);
            EventLoopGroup eventLoopGroup =
                    EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("holding-client-io"));
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
            return new HoldingClient(conf, eventLoopGroup, connectionPool, watchers);
        }

        private HoldingClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                              ConnectionPool connectionPool, List<ScalableTopicsWatcher> watchers)
                throws Exception {
            super(conf, eventLoopGroup, connectionPool);
            this.watchers = watchers;
        }

        void hold(String topic) {
            heldTopics.add(topic);
        }

        void allowAttach() {
            attachAllowed.complete(null);
        }

        /** The scalable topics of the segments subscribed to so far, whether held or not. */
        List<String> attachRequests() {
            return List.copyOf(attachRequests);
        }

        /** The scalable topics with a segment subscription completed. */
        Set<String> attachedTopics() {
            return Set.copyOf(attachedTopics);
        }

        /** The watcher of the namespace consumer: the only one this client opens. */
        ScalableTopicsWatcher watcher() {
            Awaitility.await().until(() -> !watchers.isEmpty());
            return watchers.get(0);
        }

        // The v4 Schema is qualified: the V5 one is imported.
        @Override
        public <T> CompletableFuture<Consumer<T>> subscribeSegmentAsync(
                ConsumerConfigurationData<T> conf, org.apache.pulsar.client.api.Schema<T> schema) {
            String topic = SegmentTopicName.getParentTopicName(TopicName.get(conf.getSingleTopic())).toString();
            attachRequests.add(topic);
            CompletableFuture<Void> allowed =
                    heldTopics.contains(topic) ? attachAllowed : CompletableFuture.completedFuture(null);
            return allowed.thenCompose(__ -> super.subscribeSegmentAsync(conf, schema))
                    .thenApply(consumer -> {
                        attachedTopics.add(topic);
                        return consumer;
                    });
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
    public void testQueueConsumerAttachesTopicCreatedDuringCreate() throws Exception {
        String existing = newScalableTopic(1);
        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        holding.hold(existing);
        CompletableFuture<QueueConsumer<String>> creating = client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribeAsync();
        Awaitility.await().until(() -> holding.attachRequests().contains(existing));
        ScalableTopicsWatcher watcher = holding.watcher();

        String created = newScalableTopic(1);
        Awaitility.await().until(() -> watcher.currentSetForTesting().contains(created));
        assertThat(creating).as("the consumer is still subscribing to the existing topic").isNotDone();
        holding.allowAttach();
        @Cleanup
        QueueConsumer<String> consumer = creating.get(30, SECONDS);

        Awaitility.await().untilAsserted(() ->
                assertThat(((MultiTopicQueueConsumer<String>) consumer).attachedTopicsForTesting())
                        .as("topics the consumer is attached to")
                        .containsExactlyInAnyOrder(existing, created));
        send(created, "created-during-create");
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("message sent to the topic created during the creation").isNotNull();
        assertThat(msg.value()).isEqualTo("created-during-create");
    }

    @Test
    public void testQueueConsumerDetachesTopicDeletedDuringCreate() throws Exception {
        String kept = newScalableTopic(1);
        String deleted = newScalableTopic(1);
        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        holding.hold(kept);
        CompletableFuture<QueueConsumer<String>> creating = client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribeAsync();
        Awaitility.await().until(() -> holding.attachRequests().contains(kept)
                && holding.attachedTopics().contains(deleted));
        ScalableTopicsWatcher watcher = holding.watcher();

        admin.scalableTopics().deleteScalableTopic(deleted, true);
        Awaitility.await().until(() -> !watcher.currentSetForTesting().contains(deleted));
        assertThat(creating).as("the consumer is still subscribing to the kept topic").isNotDone();
        holding.allowAttach();
        @Cleanup
        QueueConsumer<String> consumer = creating.get(30, SECONDS);

        Awaitility.await().untilAsserted(() ->
                assertThat(((MultiTopicQueueConsumer<String>) consumer).attachedTopicsForTesting())
                        .as("topics the consumer is attached to")
                        .containsExactly(kept));
    }

    @Test
    public void testStreamConsumerReceivesFromTopicCreatedDuringCreate() throws Exception {
        String existing = newScalableTopic(1);
        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        holding.hold(existing);
        CompletableFuture<StreamConsumer<String>> creating = client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribeAsync();
        Awaitility.await().until(() -> holding.attachRequests().contains(existing));
        ScalableTopicsWatcher watcher = holding.watcher();

        String created = newScalableTopic(1);
        Awaitility.await().until(() -> watcher.currentSetForTesting().contains(created));
        assertThat(creating).as("the consumer is still subscribing to the existing topic").isNotDone();
        holding.allowAttach();
        @Cleanup
        StreamConsumer<String> consumer = creating.get(30, SECONDS);

        Awaitility.await().untilAsserted(() ->
                assertThat(holding.attachedTopics())
                        .as("topics the consumer is attached to")
                        .containsExactlyInAnyOrder(existing, created));
        send(created, "created-during-create");
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("message sent to the topic created during the creation").isNotNull();
        assertThat(msg.value()).isEqualTo("created-during-create");
    }

    // --- Helpers ---

    private PulsarClientV5 newHoldingClient() throws Exception {
        return track(new PulsarClientV5(HoldingClient.create(getBrokerServiceUrl()), "holding-client", null));
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
