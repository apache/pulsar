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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A QUEUE consumer subscribes to the topic of each segment directly, without registering its subscription on
 * the scalable topic: the controller must still account for that subscription wherever it acts on the segments'
 * topics.
 */
public class ScalableTopicQueueSubscriptionTest extends V5ClientBaseTest {

    private static final String SUBSCRIPTION = "queue-sub";
    private static final int MESSAGES = 10;

    @Test
    public void testGcKeepsSealedSegmentUntilQueueSubscriptionAcknowledges() throws Exception {
        String topic = newScalableTopic(1);
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        publish(topic);
        long parent = segmentIds(topic).get(0);
        admin.scalableTopics().splitSegment(topic, parent);

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("the queue subscription has not acknowledged the messages of the sealed parent")
                .contains(parent);

        for (int i = 0; i < MESSAGES; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertThat(msg).as("message %s", i).isNotNull();
            consumer.acknowledge(msg.id());
        }
        Awaitility.await().untilAsserted(() -> {
            controller(topic).runGcTickAsync().get();
            assertThat(segmentIds(topic))
                    .as("the queue subscription acknowledged every message of the sealed parent")
                    .doesNotContain(parent);
        });
    }

    /**
     * A split creates the queue subscription on the children before they can take a message, also while no
     * consumer is connected: a consumer that comes back at the latest position still receives what the children
     * stored in the meantime.
     */
    @Test
    public void testSplitCreatesQueueSubscriptionOnChildren() throws Exception {
        String topic = newScalableTopic(1);
        subscribe(topic).close();
        long parent = segmentIds(topic).get(0);

        admin.scalableTopics().splitSegment(topic, parent);

        assertQueueSubscriptionOnActiveSegments(topic, 2);
        assertComingBackReceivesWhatWasPublished(topic);
    }

    /**
     * As {@link #testSplitCreatesQueueSubscriptionOnChildren()}, with the consumer connected during the split: the
     * children have the subscription as soon as the split completes, whether or not the consumer attached to them.
     */
    @Test
    public void testSplitCreatesQueueSubscriptionOnChildrenWhileConsumerConnected() throws Exception {
        String topic = newScalableTopic(1);
        @Cleanup
        QueueConsumer<String> consumer = subscribe(topic);
        long parent = segmentIds(topic).get(0);

        admin.scalableTopics().splitSegment(topic, parent);

        for (long segmentId : activeSegmentIds(topic, 2)) {
            assertThat(admin.scalableTopics().getSegmentStats(topic, segmentId).getSubscriptions())
                    .as("subscriptions of the new segment %s", segmentId)
                    .containsKey(SUBSCRIPTION);
        }
        publish(topic);
        assertReceivesWhatWasPublished(consumer);
    }

    /**
     * The QUEUE subscription on the children of a split has the properties it had on the parent, also after its
     * consumer comes back without them: an existing cursor keeps its properties.
     */
    @Test
    public void testSplitCarriesQueueSubscriptionPropertiesToChildren() throws Exception {
        String topic = newScalableTopic(1);
        // A '/' in the name must reach the segment's subscription endpoint as part of one path segment
        String subscription = "orders/eu";
        Map<String, String> properties = Map.of("filter", "a");
        v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionProperties(properties)
                .subscribe()
                .close();
        long parent = segmentIds(topic).get(0);

        admin.scalableTopics().splitSegment(topic, parent);
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        List<SegmentInfo> children = List.copyOf(controller(topic).getLayout().get().getActiveSegments().values());
        assertThat(children).hasSize(2);
        for (SegmentInfo child : children) {
            String childTopic = SegmentTopicName.backingTopicName(TopicName.get(topic), child);
            PersistentTopic persistentTopic = (PersistentTopic) getPulsar().getBrokerService()
                    .getTopicIfExists(childTopic).get().orElseThrow();
            assertThat(persistentTopic.getSubscription(subscription).getSubscriptionProperties())
                    .as("the properties of the queue subscription on %s", childTopic)
                    .isEqualTo(properties);
        }
    }

    /** As {@link #testSplitCreatesQueueSubscriptionOnChildren()}, for the child of a merge. */
    @Test
    public void testMergeCreatesQueueSubscriptionOnChild() throws Exception {
        String topic = newScalableTopic(2);
        subscribe(topic).close();
        List<Long> parents = segmentIds(topic);

        admin.scalableTopics().mergeSegments(topic, parents.get(0), parents.get(1));

        assertQueueSubscriptionOnActiveSegments(topic, 1);
        assertComingBackReceivesWhatWasPublished(topic);
    }

    private QueueConsumer<String> subscribe(String topic) throws Exception {
        return v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionInitialPosition(SubscriptionInitialPosition.LATEST)
                .subscribe();
    }

    private List<Long> activeSegmentIds(String topic, int expectedSegments) throws Exception {
        List<Long> active = admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(segment -> segment.isActive())
                .map(segment -> segment.getSegmentId())
                .toList();
        assertThat(active).hasSize(expectedSegments);
        return active;
    }

    private void assertQueueSubscriptionOnActiveSegments(String topic, int expectedSegments) throws Exception {
        for (long segmentId : activeSegmentIds(topic, expectedSegments)) {
            SegmentTopicStats.SubscriptionStats sub = admin.scalableTopics().getSegmentStats(topic, segmentId)
                    .getSubscriptions().get(SUBSCRIPTION);
            assertThat(sub).as("the queue subscription on the new segment %s", segmentId).isNotNull();
            assertThat(sub.getConsumers()).as("no consumer is connected").isEmpty();
        }
    }

    private void assertComingBackReceivesWhatWasPublished(String topic) throws Exception {
        publish(topic);
        @Cleanup
        QueueConsumer<String> consumer = subscribe(topic);
        assertReceivesWhatWasPublished(consumer);
    }

    private void assertReceivesWhatWasPublished(QueueConsumer<String> consumer) throws Exception {
        List<String> received = new ArrayList<>();
        for (int i = 0; i < MESSAGES; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertThat(msg).as("message %s", i).isNotNull();
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertThat(received).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, MESSAGES).mapToObj(i -> "m" + i).toList());
    }

    private void publish(String topic) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < MESSAGES; i++) {
            producer.newMessage().key("k" + i).value("m" + i).send();
        }
    }

    private ScalableTopicController controller(String topic) throws Exception {
        return getPulsar().getBrokerService().getScalableTopicService()
                .getOrCreateController(TopicName.get(topic)).get();
    }

    private List<Long> segmentIds(String topic) throws Exception {
        return List.copyOf(admin.scalableTopics().getMetadata(topic).getSegments().keySet());
    }
}
