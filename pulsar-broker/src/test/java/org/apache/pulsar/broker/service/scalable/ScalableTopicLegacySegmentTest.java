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
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A regular topic migrated to a scalable topic (PIP-475) is kept as the sealed legacy parent segments of the
 * scalable topic: the old {@code persistent://} topic, or each of its partitions. They have no {@code segment://}
 * topic of their own, so every operation of the scalable topic that reaches the topic backing a segment must reach
 * the {@code persistent://} topic for them.
 */
public class ScalableTopicLegacySegmentTest extends V5ClientBaseTest {

    @Test
    public void testGcDeletesPrunedLegacyTopic() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("no subscription holds the legacy parent, so the GC prunes it")
                .doesNotContain(0L);
        assertThat(persistentTopicExists("persistent://" + base))
                .as("the GC deletes the topic backing the pruned legacy parent")
                .isFalse();
        assertThat(admin.schemas().getAllSchemas(topic))
                .as("the scalable topic keeps the schema it shares with the deleted topic")
                .extracting(SchemaInfo::getType)
                .containsExactly(SchemaType.STRING);
    }

    @Test
    public void testGcDeletesPrunedLegacyPartitions() throws Exception {
        String base = migrateAfterPublishing(2);
        String topic = "topic://" + base;

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("no subscription holds the legacy parents, so the GC prunes them")
                .doesNotContain(0L, 1L);
        assertThat(persistentTopicExists("persistent://" + base + "-partition-0"))
                .as("the GC deletes the partition backing the pruned legacy parent 0")
                .isFalse();
        assertThat(persistentTopicExists("persistent://" + base + "-partition-1"))
                .as("the GC deletes the partition backing the pruned legacy parent 1")
                .isFalse();
    }

    @Test
    public void testGcPrunesDrainedLegacyParent() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        // The subscription has read everything on the legacy topic: its cursor there is at the end.
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.latest);

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("the subscription has drained the legacy parent, so the GC prunes it")
                .doesNotContain(0L);
        assertThat(persistentTopicExists("persistent://" + base)).isFalse();
    }

    @Test
    public void testGcKeepsLegacyParentWithBacklog() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.earliest);

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("the subscription has not read the legacy parent yet")
                .contains(0L);
        assertThat(persistentTopicExists("persistent://" + base)).isTrue();
    }

    @Test
    public void testGcKeepsLegacyParentWithBacklogOfSubscriptionFromBeforeTheMigration() throws Exception {
        String base = newBase();
        admin.topics().createNonPartitionedTopic("persistent://" + base);
        // A subscription of the regular topic that no V5 consumer has used yet: the scalable topic does not
        // know it, but its backlog is still on the legacy topic.
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.earliest);
        publishThenMigrate(base);
        String topic = "topic://" + base;

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("a subscription of the legacy topic has not read it yet")
                .contains(0L);
        assertThat(persistentTopicExists("persistent://" + base)).isTrue();
    }

    @Test
    public void testGcPrunesLegacyParentWhoseTopicWasDeleted() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        admin.topics().delete("persistent://" + base, true);

        controller(topic).runGcTickAsync().get();

        assertThat(segmentIds(topic))
                .as("the data of the legacy parent is gone, so nothing holds it")
                .doesNotContain(0L);
    }

    @Test
    public void testDeleteScalableTopicDeletesLegacyTopic() throws Exception {
        String base = migrateAfterPublishing(0);

        admin.scalableTopics().deleteScalableTopic("topic://" + base, true);

        assertThat(persistentTopicExists("persistent://" + base)).isFalse();
    }

    @Test
    public void testDeleteScalableTopicDeletesLegacyPartitions() throws Exception {
        String base = migrateAfterPublishing(2);

        admin.scalableTopics().deleteScalableTopic("topic://" + base, true);

        assertThat(persistentTopicExists("persistent://" + base + "-partition-0")).isFalse();
        assertThat(persistentTopicExists("persistent://" + base + "-partition-1")).isFalse();
    }

    @Test
    public void testStreamConsumerReadsLegacyParentFirst() throws Exception {
        String base = migrateAfterPublishing(0);

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic("topic://" + base)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // The child is read only once the controller's drain poll sees the legacy parent drained.
        List<String> received = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Message<String> message = consumer.receive(Duration.ofSeconds(30));
            assertThat(message).as("message %s, after %s", i, received).isNotNull();
            received.add(message.value());
            consumer.acknowledgeCumulative(message.id());
        }
        assertThat(received).containsExactly("pre", "post");
    }

    @Test
    public void testCheckpointConsumerGroupReadsLegacyParent() throws Exception {
        String base = migrateAfterPublishing(0);

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic("topic://" + base)
                .consumerGroup("group")
                .startPosition(Checkpoint.earliest())
                .create();

        List<String> received = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Message<String> message = consumer.receive(Duration.ofSeconds(10));
            assertThat(message).as("message %s, after %s", i, received).isNotNull();
            received.add(message.value());
        }
        assertThat(received).containsExactlyInAnyOrder("pre", "post");
    }

    @Test
    public void testClearBacklogSkipsLegacyBacklog() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.earliest);

        admin.scalableTopics().clearBacklog(topic, "sub");

        assertThat(legacyBacklog(base, "sub")).isZero();
    }

    @Test
    public void testSeekResetsLegacyCursorToTimestamp() throws Exception {
        String base = newBase();
        admin.topics().createNonPartitionedTopic("persistent://" + base);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + base)
                .create();
        producer.newMessage().key("k").value("pre-1").send();
        long timestamp = System.currentTimeMillis();
        Awaitility.await().until(() -> System.currentTimeMillis() > timestamp);
        producer.newMessage().key("k").value("pre-2").send();
        admin.scalableTopics().migrateToScalable(base, false);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.latest);

        // Both messages predate the legacy parent's createdAtMs, the migration time: only "pre-2" is newer than the
        // timestamp.
        admin.scalableTopics().seekSubscription(topic, "sub", timestamp + 1);

        assertThat(legacyBacklog(base, "sub")).isEqualTo(1);
    }

    @Test
    public void testDeleteSubscriptionDeletesLegacyCursor() throws Exception {
        String base = migrateAfterPublishing(0);
        String topic = "topic://" + base;
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        admin.topics().createSubscription("persistent://" + base, "sub", MessageId.earliest);

        admin.scalableTopics().deleteSubscription(topic, "sub");

        assertThat(admin.topics().getSubscriptions("persistent://" + base)).doesNotContain("sub");
    }

    private String newBase() {
        return getNamespace() + "/legacy-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Creates a regular topic ({@code partitions <= 0} for a non-partitioned one), publishes {@code "pre"} with a
     * V5 producer, migrates it to a scalable topic, then publishes {@code "post"} on the scalable topic. Returns the
     * {@code tenant/namespace/name} the regular and the scalable topic share.
     */
    private String migrateAfterPublishing(int partitions) throws Exception {
        String base = newBase();
        if (partitions > 0) {
            admin.topics().createPartitionedTopic("persistent://" + base, partitions);
        } else {
            admin.topics().createNonPartitionedTopic("persistent://" + base);
        }
        publishThenMigrate(base);
        return base;
    }

    private void publishThenMigrate(String base) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + base)
                .create();
        producer.newMessage().key("k").value("pre").send();
        admin.scalableTopics().migrateToScalable(base, false);
        producer.newMessage().key("k").value("post").send();
    }

    private ScalableTopicController controller(String topic) throws Exception {
        return getPulsar().getBrokerService().getScalableTopicService()
                .getOrCreateController(TopicName.get(topic)).get();
    }

    private List<Long> segmentIds(String topic) throws Exception {
        return List.copyOf(admin.scalableTopics().getMetadata(topic).getSegments().keySet());
    }

    private boolean persistentTopicExists(String topic) throws Exception {
        return getPulsar().getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get(topic)).get();
    }

    private long legacyBacklog(String base, String subscription) throws Exception {
        return admin.topics().getStats("persistent://" + base).getSubscriptions().get(subscription)
                .getMsgBacklog();
    }
}
