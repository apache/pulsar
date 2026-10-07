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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.Test;

/**
 * Scalable-topic stats across brokers: the topic-level call is redirected to the controller
 * leader, whose fan-out has to reach segments served by other brokers; the per-segment call
 * is redirected to the broker serving the segment.
 */
public class V5MultiBrokerScalableTopicStatsTest extends V5MultiBrokerClientBaseTest {

    @Test
    public void testStatsRedirectToLeaderAndFanOutAcrossBrokers() throws Exception {
        String topic = newScalableTopic(3);
        TopicName tn = TopicName.get(topic);

        // Segment 0's backing topic was created on its bundle owner at topic creation.
        ScalableTopicMetadata.SegmentInfo seg0 = admin.scalableTopics().getMetadata(topic).getSegments().get(0L);
        TopicName segment0 = SegmentTopicName.fromParent(tn,
                HashRange.of(seg0.getHashRange().getStart(), seg0.getHashRange().getEnd()), 0L);
        int ownerIndex = findOwnerIndex(segment0);

        // Make a broker that does not serve segment 0 the controller leader: the first broker
        // to materialize the controller wins the election. Its stats fan-out must then cross
        // brokers to reach segment 0.
        int leaderIndex = (ownerIndex + 1) % brokers.size();
        brokers.get(leaderIndex).getBrokerService().getScalableTopicService()
                .getOrCreateController(tn).get(10, TimeUnit.SECONDS);
        assertEquals(findControllerLeaderIndex(topic), leaderIndex);

        String subscription = "mb-sub";
        admin.scalableTopics().createSubscription(topic, subscription, ScalableSubscriptionType.QUEUE);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("mb-producer")
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        int n = 60;
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        // Topic-level stats requested from a non-leader broker (segment 0's owner is one):
        // redirected to the leader, which collects segment 0 from its owner over HTTP.
        ScalableTopicStats stats = admins.get(ownerIndex).scalableTopics().getStats(topic);

        String ownerId = brokers.get(ownerIndex).getBrokerId();
        assertNotEquals(ownerId, brokers.get(leaderIndex).getBrokerId());
        assertEquals(stats.getLayout().getSegments().size(), 3);
        assertEquals(stats.getLayout().getSegments().get(0L).getOwnerBroker(), ownerId,
                "segment 0's stats must come from the broker serving it");
        for (ScalableTopicStats.LayoutSegment segment : stats.getLayout().getSegments().values()) {
            assertNotNull(segment.getOwnerBroker(), "every segment's stats must be collected: " + segment);
        }
        assertEquals(stats.getProducers().size(), 1, "got " + stats.getProducers());
        assertEquals(stats.getProducers().get(0).getProducerName(), "mb-producer");
        assertEquals(stats.getSubscriptions().get(subscription).getMsgBacklog(), n,
                "backlog is summed across segments on every broker");

        // Per-segment stats requested from a broker that does not serve the segment (the
        // leader, by construction): redirected to the owner.
        SegmentTopicStats segmentStats = admins.get(leaderIndex).scalableTopics().getSegmentStats(segment0.toString());
        assertEquals(segmentStats.getOwnerBroker(), ownerId);
        assertEquals(segmentStats.getProducers().get(0).getProducerName(), "mb-producer-seg-0");
        assertEquals(segmentStats.getSubscriptions().get(subscription).getMsgBacklog(),
                stats.getSubscriptions().get(subscription).getSegments().get(0L).getMsgBacklog(),
                "the per-segment view and the topic-level breakdown agree");
    }
}
