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
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting the backing topic of one segment must leave the schema of the scalable topic in place.
 * Every segment maps to the same schema id ({@link TopicName#getSchemaName()} drops the segment
 * descriptor), so the schema belongs to the scalable topic and is shared by all of its segments:
 * only deleting the scalable topic itself deletes it.
 */
public class ScalableTopicSegmentDeleteSchemaTest extends V5ClientBaseTest {

    @Test
    public void testDeleteScalableTopicDeletesSchema() throws Exception {
        String topic = newScalableTopic(2);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value("v").send();
        producer.close();
        String schemaId = TopicName.get(topic).getSchemaName();
        assertThat(getPulsar().getSchemaRegistryService().getSchema(schemaId).get())
                .as("schema registered by the producer on %s", topic)
                .isNotNull();

        admin.scalableTopics().deleteScalableTopic(topic, true);

        assertThat(getPulsar().getSchemaRegistryService().getSchema(schemaId).get())
                .as("schema of %s after deleting the scalable topic", topic)
                .isNull();
    }

    @Test
    public void testDeleteSegmentKeepsTopicSchema() throws Exception {
        String topic = newScalableTopic(1);
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        long parent = splitAfterPublishing(topic, producer);

        admin.scalableTopics().deleteSegment(segmentTopicName(topic, parent), true);

        assertTopicSchemaKept(topic);
    }

    @Test
    public void testGcPruneKeepsTopicSchema() throws Exception {
        String topic = newScalableTopic(1);
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        long parent = splitAfterPublishing(topic, producer);
        TopicName parentSegmentTopic = TopicName.get(segmentTopicName(topic, parent));

        // The broker default retention (defaultRetentionTimeInMinutes = 0) makes the sealed parent
        // eligible right away, and with no subscriptions there is nothing left to drain.
        ScalableTopicController controller = getPulsar().getBrokerService().getScalableTopicService()
                .getOrCreateController(TopicName.get(topic)).get();
        controller.runGcTickAsync().get();

        assertThat(admin.scalableTopics().getMetadata(topic).getSegments())
                .as("the GC tick must prune the sealed parent from the layout")
                .doesNotContainKey(parent);
        Awaitility.await().untilAsserted(() -> assertThat(getPulsar().getPulsarResources().getTopicResources()
                .persistentTopicExists(parentSegmentTopic).get())
                .as("the GC tick must delete the backing topic of the sealed parent")
                .isFalse());
        assertTopicSchemaKept(topic);
    }

    /**
     * Publishes {@code "pre"} on the single initial segment, splits it, then publishes
     * {@code "post"} on the children. Returns the id of the sealed parent.
     */
    private long splitAfterPublishing(String topic, Producer<String> producer) throws Exception {
        producer.newMessage().key("k").value("pre").send();
        long parent = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parent);
        Awaitility.await().untilAsserted(() -> assertThat(activeSegmentCount(topic))
                .as("active segments after splitting segment %s", parent)
                .isEqualTo(2));
        producer.newMessage().key("k").value("post").send();
        return parent;
    }

    /**
     * The schema registered by the producer is still there, so a new consumer with the same
     * schema can attach to the surviving children and read what was published after the split.
     */
    private void assertTopicSchemaKept(String topic) throws Exception {
        assertThat(admin.schemas().getAllSchemas(topic))
                .as("schemas of %s after the backing topic of its sealed segment was deleted", topic)
                .extracting(SchemaInfo::getType)
                .containsExactly(SchemaType.STRING);

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("message published on the children after the split").isNotNull();
        assertThat(msg.value()).isEqualTo("post");
    }

    private long singleActiveSegmentId(String topic) throws Exception {
        return admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(seg -> seg.isActive())
                .mapToLong(seg -> seg.getSegmentId())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no active segment for " + topic));
    }

    private long activeSegmentCount(String topic) throws Exception {
        return admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(seg -> seg.isActive())
                .count();
    }

    private String segmentTopicName(String topic, long segmentId) throws Exception {
        var range = admin.scalableTopics().getMetadata(topic).getSegments().get(segmentId).getHashRange();
        return SegmentTopicName.fromParent(TopicName.get(topic),
                HashRange.of(range.getStart(), range.getEnd()), segmentId).toString();
    }
}
