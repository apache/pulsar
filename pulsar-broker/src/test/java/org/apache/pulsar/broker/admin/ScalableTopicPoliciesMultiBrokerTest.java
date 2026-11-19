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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.SharedMultiBrokerPulsarBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A topic-level policy of a scalable topic written through any broker is served by the owner of the
 * {@code topic://} name and reaches the segments loaded on every broker.
 */
@Test(groups = "broker-admin")
public class ScalableTopicPoliciesMultiBrokerTest extends SharedMultiBrokerPulsarBaseTest {

    @Test(timeOut = 120_000)
    public void testWriteThroughAnyBrokerReachesAllSegments() throws Exception {
        String topic = "topic://" + getNamespace() + "/policies-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 4);
        admin.scalableTopics().setAutoScalePolicy(topic, AutoScalePolicyOverride.builder().enabled(false).build());
        // Collecting the stats loads every segment on the broker owning it
        List<String> segments = admin.scalableTopics().getStats(topic).getLayout().getSegments().values().stream()
                .map(ScalableTopicStats.LayoutSegment::getName)
                .toList();
        assertEquals(segments.size(), 4);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (String segment : segments) {
                assertTrue(brokers.stream().anyMatch(b -> b.getBrokerService().getTopicReference(segment).isPresent()),
                        segment + " is not loaded on any broker");
            }
        });

        for (int i = 0; i < brokers.size(); i++) {
            int ttl = 100 + i;
            admins.get(i).topicPolicies().setMessageTTL(topic, ttl);
            for (int j = 0; j < brokers.size(); j++) {
                assertEquals(admins.get(j).topicPolicies().getMessageTTL(topic), Integer.valueOf(ttl));
            }
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                for (PulsarService broker : brokers) {
                    for (String segment : segments) {
                        Optional<Topic> loaded = broker.getBrokerService().getTopicReference(segment);
                        if (loaded.isPresent()) {
                            assertEquals(((PersistentTopic) loaded.get()).getHierarchyTopicPolicies()
                                    .getMessageTTLInSeconds().get(), Integer.valueOf(ttl),
                                    segment + " on " + broker.getBrokerId());
                        }
                    }
                }
            });
        }
    }
}
