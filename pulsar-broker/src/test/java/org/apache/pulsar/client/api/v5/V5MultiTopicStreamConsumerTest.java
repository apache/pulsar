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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * End-to-end tests for {@link StreamConsumerBuilder#namespace}: a multi-topic
 * StreamConsumer follows the matching set live, multiplexes from every per-topic
 * consumer into one user-visible queue, and supports cumulative ack across
 * topics via per-message position vectors.
 */
public class V5MultiTopicStreamConsumerTest extends V5ClientBaseTest {

    private String topicName(String suffix) {
        return "topic://" + getNamespace() + "/" + suffix + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    public void receivesFromAllTopicsInNamespace() throws Exception {
        String topicA = topicName("a");
        String topicB = topicName("b");
        admin.scalableTopics().createScalableTopic(topicA, 1);
        admin.scalableTopics().createScalableTopic(topicB, 1);

        @Cleanup
        Producer<String> pa = v5Client.newProducer(Schema.string()).topic(topicA).create();
        @Cleanup
        Producer<String> pb = v5Client.newProducer(Schema.string()).topic(topicB).create();

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-stream")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String va = "a-" + i;
            String vb = "b-" + i;
            pa.newMessage().value(va).send();
            pb.newMessage().value(vb).send();
            expected.add(va);
            expected.add(vb);
        }

        Set<String> received = new HashSet<>();
        MessageId last = null;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected.size() && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                last = msg.id();
            }
        }
        assertEquals(received, expected, "should receive every message produced to either topic");
        // Cumulative ack must succeed across both per-topic consumers — exercises the
        // multi-topic position vector embedded in the message id.
        assertNotNull(last);
        consumer.acknowledgeCumulative(last);
    }

    @Test
    public void picksUpTopicCreatedAfterSubscribe() throws Exception {
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-stream-late")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        String lateTopic = topicName("late");
        admin.scalableTopics().createScalableTopic(lateTopic, 1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(lateTopic).create();
        producer.newMessage().value("late-message").send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(15));
        assertTrue(msg != null, "expected to receive message from late-added topic");
        assertEquals(msg.value(), "late-message");
        assertEquals(msg.topic(), lateTopic, "topic() should surface the parent scalable topic");
        consumer.acknowledgeCumulative(msg.id());
    }

    @Test
    public void cumulativeAckCoversEveryTopicSeenSoFar() throws Exception {
        // Two topics, interleaved producers. After we cumulatively ack the LAST message,
        // closing and re-subscribing must NOT redeliver any of the previous messages —
        // that would only happen if the per-topic ack didn't fire for the topic that's
        // not the message's own.
        String topicA = topicName("a");
        String topicB = topicName("b");
        admin.scalableTopics().createScalableTopic(topicA, 1);
        admin.scalableTopics().createScalableTopic(topicB, 1);

        @Cleanup
        Producer<String> pa = v5Client.newProducer(Schema.string()).topic(topicA).create();
        @Cleanup
        Producer<String> pb = v5Client.newProducer(Schema.string()).topic(topicB).create();

        StreamConsumer<String> first = v5Client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-stream-cumulative")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 5;
        for (int i = 0; i < n; i++) {
            pa.newMessage().value("a-" + i).send();
            pb.newMessage().value("b-" + i).send();
        }

        // Drain everything, remember the last message id.
        Set<String> drained = new HashSet<>();
        MessageId last = null;
        long deadline = System.currentTimeMillis() + 20_000L;
        while (drained.size() < 2 * n && System.currentTimeMillis() < deadline) {
            Message<String> msg = first.receive(Duration.ofSeconds(1));
            if (msg != null) {
                drained.add(msg.value());
                last = msg.id();
            }
        }
        assertEquals(drained.size(), 2 * n, "first consumer should drain every message");
        assertNotNull(last);
        // Cumulative ack — should fan out to BOTH topics' per-segment acks.
        first.acknowledgeCumulative(last);
        // Block briefly so the async ack flushes through to the broker before we close.
        Thread.sleep(500);
        first.close();

        // Re-subscribe with the same name. If the cumulative ack covered both topics,
        // there's nothing to re-deliver. If it only acked the message's OWN topic, the
        // other topic would re-deliver from the start.
        @Cleanup
        StreamConsumer<String> second = v5Client.newStreamConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-stream-cumulative")
                .subscribe();

        Message<String> stale = second.receive(Duration.ofSeconds(2));
        assertTrue(stale == null,
                "cumulative ack should have covered every topic; got redelivery: "
                        + (stale != null ? stale.value() : ""));
    }

    @Test
    public void filtersByPropertySoOnlyMatchingTopicsAttach() throws Exception {
        String aliceTopic = topicName("alice");
        String bobTopic = topicName("bob");
        admin.scalableTopics().createScalableTopic(aliceTopic, 1, Map.of("owner", "alice"));
        admin.scalableTopics().createScalableTopic(bobTopic, 1, Map.of("owner", "bob"));

        @Cleanup
        Producer<String> pa = v5Client.newProducer(Schema.string()).topic(aliceTopic).create();
        @Cleanup
        Producer<String> pb = v5Client.newProducer(Schema.string()).topic(bobTopic).create();

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .namespace(getNamespace(), Map.of("owner", "alice"))
                .subscriptionName("multi-stream-filter")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        pa.newMessage().value("alice-msg").send();
        pb.newMessage().value("bob-msg").send();

        Message<String> got = consumer.receive(Duration.ofSeconds(10));
        assertTrue(got != null, "expected one message");
        assertEquals(got.value(), "alice-msg");

        Message<String> empty = consumer.receive(Duration.ofSeconds(2));
        assertTrue(empty == null, "bob's message must be filtered out, got " + empty);
    }
}
