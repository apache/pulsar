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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Tests for V5 namespace-level (multi-topic) consumers and how they react when scalable topics in the
 * namespace are deleted. Expected behavior:
 * <ul>
 *   <li>property filters select the matching topics — only those get a per-topic consumer;</li>
 *   <li>when a matched topic is deleted, the watcher notifies the consumer, which stops the per-topic
 *       consumer for it;</li>
 *   <li>the deleted topic is <b>not</b> auto-recreated by the namespace consumer, even though the
 *       broker has auto-topic-creation enabled (a reconnecting per-topic consumer must not resurrect a
 *       topic the operator just deleted).</li>
 * </ul>
 *
 * <p>Lives in {@code org.apache.pulsar.client.impl.v5} to reach the package-private
 * {@link MultiTopicQueueConsumer#attachedTopicsForTesting()} accessor.
 */
public class V5NamespaceConsumerTopicDeletionTest extends V5ClientBaseTest {

    private String topicName(String suffix) {
        return "topic://" + getNamespace() + "/" + suffix + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    private MultiTopicQueueConsumer<String> subscribeNamespace(String subscription,
                                                               Map<String, String> propertyFilters)
            throws Exception {
        QueueConsumer<String> consumer;
        if (propertyFilters == null) {
            consumer = v5Client.newQueueConsumer(Schema.string())
                    .subscriptionName(subscription)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                    .namespace(getNamespace())
                    .subscribe();
        } else {
            consumer = v5Client.newQueueConsumer(Schema.string())
                    .subscriptionName(subscription)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                    .namespace(getNamespace(), propertyFilters)
                    .subscribe();
        }
        return (MultiTopicQueueConsumer<String>) consumer;
    }

    /**
     * Property filters act as the selector: only topics whose properties match get a per-topic
     * consumer; non-matching topics in the same namespace never attach.
     */
    @Test
    public void filtersByPropertySoOnlyMatchingTopicsAttach() throws Exception {
        String aliceTopic = topicName("alice");
        String bobTopic = topicName("bob");
        admin.scalableTopics().createScalableTopic(aliceTopic, 1, Map.of("owner", "alice"));
        admin.scalableTopics().createScalableTopic(bobTopic, 1, Map.of("owner", "bob"));

        @Cleanup
        MultiTopicQueueConsumer<String> consumer = subscribeNamespace("ns-filter", Map.of("owner", "alice"));

        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.attachedTopicsForTesting(), Set.of(aliceTopic)));
        assertFalse(consumer.attachedTopicsForTesting().contains(bobTopic));
    }

    /**
     * Deleting a matched topic notifies the namespace consumer, which stops the per-topic consumer
     * for it (the attached set shrinks).
     */
    @Test
    public void internalConsumerStopsWhenTopicDeleted() throws Exception {
        String keep = topicName("keep");
        String drop = topicName("drop");
        admin.scalableTopics().createScalableTopic(keep, 1);
        admin.scalableTopics().createScalableTopic(drop, 1);

        @Cleanup
        MultiTopicQueueConsumer<String> consumer = subscribeNamespace("ns-stop", null);
        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.attachedTopicsForTesting(), Set.of(keep, drop)));

        admin.scalableTopics().deleteScalableTopic(drop, true);

        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.attachedTopicsForTesting(), Set.of(keep)));
    }

    /**
     * A deleted topic must not be auto-recreated by the namespace consumer, even though the broker has
     * auto-topic-creation enabled. Asserted continuously across reconnect/recheck cycles, so a
     * reconnecting per-topic consumer cannot quietly resurrect the topic.
     */
    @Test
    public void deletedTopicIsNotRecreatedByNamespaceConsumer() throws Exception {
        String keep = topicName("keep");
        String drop = topicName("drop");
        admin.scalableTopics().createScalableTopic(keep, 1);
        admin.scalableTopics().createScalableTopic(drop, 1);

        @Cleanup
        MultiTopicQueueConsumer<String> consumer = subscribeNamespace("ns-norecreate", null);
        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.attachedTopicsForTesting(), Set.of(keep, drop)));

        admin.scalableTopics().deleteScalableTopic(drop, true);
        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.attachedTopicsForTesting(), Set.of(keep)));

        // The deleted topic stays gone — absent from the broker and never re-attached — held
        // continuously for several seconds so a per-topic reconnect or recheck cannot resurrect it.
        Awaitility.await()
                .during(5, TimeUnit.SECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThrows(PulsarAdminException.NotFoundException.class,
                            () -> admin.scalableTopics().getMetadata(drop));
                    assertFalse(consumer.attachedTopicsForTesting().contains(drop),
                            "namespace consumer must not re-attach the deleted topic");
                });
    }
}
