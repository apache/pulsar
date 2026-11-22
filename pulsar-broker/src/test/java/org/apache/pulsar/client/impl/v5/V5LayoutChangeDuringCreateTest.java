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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A layout change that reaches the {@link DagWatchClient} of a producer or consumer still being created
 * must be applied once it exists. The broker pushes a layout once, and the listener that applies the
 * pushed layouts is only set after the initial layout has been applied.
 *
 * <p>The tests call the factories the builders call, with a {@link DagWatchClient} of their own, so they
 * can wait until it has received the split before letting the creation finish. They check which segments
 * were attached by the time the creation completes, rather than waiting for messages: the broker also
 * pushes the unchanged layout again whenever a node is created under the topic's metadata (the first
 * load report of a segment, for one), which would eventually hide the missed layout.
 */
public class V5LayoutChangeDuringCreateTest extends V5ClientBaseTest {

    private static final int KEYS = 10;

    /**
     * A v4 client that records the segment topics attached to, in order, and holds back the readers and
     * consumers until the test allows them.
     */
    private static final class HoldingClient extends PulsarClientImpl {

        private final List<String> attachRequests = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Void> attachAllowed = new CompletableFuture<>();

        HoldingClient(ClientConfigurationData conf) throws Exception {
            super(conf);
        }

        List<String> attachRequests() {
            return List.copyOf(attachRequests);
        }

        void allowAttach() {
            attachAllowed.complete(null);
        }

        // The v4 Schema and Producer are qualified: the V5 ones are imported.
        @Override
        public <T> CompletableFuture<Reader<T>> createSegmentReaderAsync(
                ReaderConfigurationData<T> conf, org.apache.pulsar.client.api.Schema<T> schema) {
            attachRequests.add(conf.getTopicName());
            return attachAllowed.thenCompose(__ -> super.createSegmentReaderAsync(conf, schema));
        }

        @Override
        public <T> CompletableFuture<Consumer<T>> subscribeSegmentAsync(
                ConsumerConfigurationData<T> conf, org.apache.pulsar.client.api.Schema<T> schema) {
            attachRequests.add(conf.getSingleTopic());
            return attachAllowed.thenCompose(__ -> super.subscribeSegmentAsync(conf, schema));
        }

        @Override
        public <T> CompletableFuture<org.apache.pulsar.client.api.Producer<T>> createSegmentProducerAsync(
                ProducerConfigurationData conf, org.apache.pulsar.client.api.Schema<T> schema) {
            attachRequests.add(conf.getTopicName());
            return super.createSegmentProducerAsync(conf, schema);
        }
    }

    @Test
    public void testCheckpointConsumerReadsChildrenOfSplitDuringCreate() throws Exception {
        String topic = newTopic();
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        List<String> sent = publish(producer, "pre-split");

        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        DagWatchClient dagWatch = new DagWatchClient(holding, TopicName.get(topic));
        ClientSegmentLayout initialLayout = dagWatch.start().get(30, SECONDS);
        // What CheckpointConsumerBuilderV5 does for a consumer outside a consumer group.
        CompletableFuture<CheckpointConsumer<String>> creating = ScalableCheckpointConsumer.createUnmanagedAsync(
                client, Schema.string(), dagWatch, initialLayout, Checkpoint.earliest(), null);
        assertThat(holding.attachRequests()).containsExactlyElementsOf(segmentTopics(initialLayout));

        List<String> children = splitAndAwait(topic, dagWatch, initialLayout);
        assertThat(creating).as("the consumer is still creating its segment readers").isNotDone();
        holding.allowAttach();
        @Cleanup
        CheckpointConsumer<String> consumer = creating.get(30, SECONDS);

        assertThat(holding.attachRequests())
                .as("segments the consumer reads, once created")
                .containsAll(children);
        sent.addAll(publish(producer, "post-split"));
        assertThat(receive(consumer::receive, sent.size()))
                .as("pre-split messages are on the sealed parent, post-split ones on its children")
                .containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    public void testQueueConsumerReceivesFromChildrenOfSplitDuringCreate() throws Exception {
        String topic = newTopic();
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        List<String> sent = publish(producer, "pre-split");

        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        DagWatchClient dagWatch = new DagWatchClient(holding, TopicName.get(topic));
        ClientSegmentLayout initialLayout = dagWatch.start().get(30, SECONDS);
        ConsumerConfigurationData<String> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("sub");
        conf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        // What QueueConsumerBuilderV5 does for a single topic.
        CompletableFuture<QueueConsumer<String>> creating = ScalableQueueConsumer.createAsync(
                client, Schema.string(), conf, dagWatch, initialLayout, null);
        assertThat(holding.attachRequests()).containsExactlyElementsOf(segmentTopics(initialLayout));

        List<String> children = splitAndAwait(topic, dagWatch, initialLayout);
        assertThat(creating).as("the consumer is still subscribing to its segments").isNotDone();
        holding.allowAttach();
        @Cleanup
        QueueConsumer<String> consumer = creating.get(30, SECONDS);

        assertThat(holding.attachRequests())
                .as("segments the consumer subscribes to, once created")
                .containsAll(children);
        sent.addAll(publish(producer, "post-split"));
        assertThat(receive(consumer::receive, sent.size()))
                .as("pre-split messages are on the sealed parent, post-split ones on its children")
                .containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    public void testProducerRoutesToChildrenOfSplitBeforeCreate() throws Exception {
        String topic = newTopic();
        PulsarClientV5 client = newHoldingClient();
        HoldingClient holding = (HoldingClient) client.v4Client();
        DagWatchClient dagWatch = new DagWatchClient(holding, TopicName.get(topic));
        ClientSegmentLayout initialLayout = dagWatch.start().get(30, SECONDS);
        List<String> children = splitAndAwait(topic, dagWatch, initialLayout);

        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setTopicName(topic);
        // What ProducerBuilderV5 does with the layout start() completed with, here older than the
        // watch's current one: the split arrived before the producer was created.
        @Cleanup
        ScalableTopicProducer<String> producer =
                new ScalableTopicProducer<>(client, Schema.string(), conf, dagWatch, initialLayout);
        CompletableFuture<MessageId> sending = producer.async().newMessage()
                .key("key")
                .value("value")
                .send();

        Awaitility.await().until(() -> !holding.attachRequests().isEmpty());
        assertThat(holding.attachRequests().get(0))
                .as("segment the producer sends its first message to")
                .isIn(children);
        sending.get(30, SECONDS);
    }

    // --- Helpers ---

    private String newTopic() throws Exception {
        String topic = newScalableTopic(1);
        // Only the split below may change the layout.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private PulsarClientV5 newHoldingClient() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(getBrokerServiceUrl());
        conf.setStatsIntervalSeconds(0);
        return track(new PulsarClientV5(new HoldingClient(conf), "holding-client", null));
    }

    private static List<String> segmentTopics(ClientSegmentLayout layout) {
        return layout.activeSegments().stream()
                .map(ActiveSegment::attachTopicName)
                .toList();
    }

    /**
     * Split the topic's only segment and wait until {@code dagWatch} has the new layout; returns the
     * topics of the children.
     */
    private List<String> splitAndAwait(String topic, DagWatchClient dagWatch, ClientSegmentLayout before)
            throws Exception {
        admin.scalableTopics().splitSegment(topic, before.activeSegments().get(0).segmentId());
        Awaitility.await().until(() -> dagWatch.currentLayout().activeSegments().size() == 2);
        return segmentTopics(dagWatch.currentLayout());
    }

    /** Publish one message to each of {@link #KEYS} keys, so that every child of a split gets some. */
    private static List<String> publish(Producer<String> producer, String prefix) throws Exception {
        List<String> values = new ArrayList<>();
        for (int k = 0; k < KEYS; k++) {
            String value = prefix + "-" + k;
            producer.newMessage()
                    .key("key-" + k)
                    .value(value)
                    .send();
            values.add(value);
        }
        return values;
    }

    @FunctionalInterface
    private interface Receiver {
        Message<String> receive(Duration timeout) throws Exception;
    }

    /** Receive up to {@code count} values, stopping early when none arrives for a while. */
    private static List<String> receive(Receiver receiver, int count) throws Exception {
        List<String> values = new ArrayList<>();
        while (values.size() < count) {
            Message<String> msg = receiver.receive(Duration.ofSeconds(10));
            if (msg == null) {
                break;
            }
            values.add(msg.value());
        }
        return values;
    }
}
