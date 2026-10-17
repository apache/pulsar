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
 * End-to-end tests for {@link QueueConsumerBuilder#namespace}: a multi-topic
 * QueueConsumer follows the matching set live, multiplexes from every per-topic
 * consumer into one user-visible queue, and routes individual acks back to the
 * right topic for redelivery purposes.
 */
public class V5MultiTopicQueueConsumerTest extends V5ClientBaseTest {

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
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-q")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Send to both topics; the multi-topic consumer must receive both sets.
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
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected.size() && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                consumer.acknowledge(msg.id());
            }
        }
        assertEquals(received, expected, "should receive every message produced to either topic");
    }

    @Test
    public void picksUpTopicCreatedAfterSubscribe() throws Exception {
        // Fresh namespace, no topics yet — initial snapshot is empty. Earliest so the
        // race between "topic created" and "per-topic consumer attached via Diff" can't
        // drop messages produced in that window.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .namespace(getNamespace())
                .subscriptionName("multi-q-late")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Create a topic AFTER subscribe; the watcher's Diff event must trigger the
        // consumer to attach.
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
        consumer.acknowledge(msg.id());
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
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .namespace(getNamespace(), Map.of("owner", "alice"))
                .subscriptionName("multi-q-filter")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        pa.newMessage().value("alice-msg").send();
        pb.newMessage().value("bob-msg").send();

        // Only alice's message reaches this consumer.
        Message<String> got = consumer.receive(Duration.ofSeconds(10));
        assertTrue(got != null, "expected one message");
        assertEquals(got.value(), "alice-msg");
        consumer.acknowledge(got.id());

        // Confirm bob's message never arrives within a generous window.
        Message<String> empty = consumer.receive(Duration.ofSeconds(2));
        assertTrue(empty == null, "bob's message must be filtered out, got " + empty);
    }
}
