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

import static org.testng.Assert.assertEquals;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A segment of a scalable topic loads, and keeps receiving, the topic policies of its parent {@code topic://}.
 * Policies are written straight to the {@link TopicPoliciesService} here; the REST write path is tested elsewhere.
 */
@Test(groups = "broker")
public class SegmentTopicPoliciesInheritanceTest extends SharedPulsarBaseTest {

    private String newScalableTopic() throws Exception {
        String topic = "topic://" + getNamespace() + "/policies-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 1);
        admin.scalableTopics().setAutoScalePolicy(topic, AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private String segmentName(String topic, long id) throws Exception {
        return admin.scalableTopics().getStats(topic).getLayout().getSegments().get(id).getName();
    }

    private PersistentTopic loadSegment(String name) throws Exception {
        return (PersistentTopic) getPulsar().getBrokerService().getTopic(name, false).get().orElseThrow();
    }

    private void setLocal(String topic, Consumer<TopicPolicies> mutator) throws Exception {
        getPulsar().getTopicPoliciesService()
                .updateTopicPoliciesAsync(TopicName.get(topic), false, false, mutator).get();
    }

    @Test(timeOut = 60_000)
    public void testLoadedSegmentReadsParentPolicies() throws Exception {
        String topic = newScalableTopic();
        setLocal(topic, p -> {
            p.setMessageTTLInSeconds(120);
            p.setMaxMessageSize(1024);
        });
        PersistentTopic segment = loadSegment(segmentName(topic, 0));
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(segment.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), Integer.valueOf(120));
            assertEquals(segment.getHierarchyTopicPolicies().getTopicMaxMessageSize().get(), Integer.valueOf(1024));
        });
    }

    @Test(timeOut = 60_000)
    public void testParentUpdateReachesLoadedSegment() throws Exception {
        String topic = newScalableTopic();
        PersistentTopic segment = loadSegment(segmentName(topic, 0));
        setLocal(topic, p -> p.setMessageTTLInSeconds(77));
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(segment.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(),
                        Integer.valueOf(77)));
    }

    @Test(timeOut = 60_000)
    public void testSplitChildInheritsParentPolicies() throws Exception {
        String topic = newScalableTopic();
        setLocal(topic, p -> p.setMessageTTLInSeconds(55));
        admin.scalableTopics().splitSegment(topic, 0);
        List<ScalableTopicStats.LayoutSegment> children = admin.scalableTopics().getStats(topic).getLayout()
                .getSegments().entrySet().stream()
                .filter(e -> e.getKey() != 0L && e.getValue().isActive())
                .map(e -> e.getValue())
                .toList();
        assertEquals(children.size(), 2);
        for (ScalableTopicStats.LayoutSegment child : children) {
            PersistentTopic segment = loadSegment(child.getName());
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertEquals(segment.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(),
                            Integer.valueOf(55)));
        }
    }

    @Test(timeOut = 60_000)
    public void testSegmentManagedLedgerConfigUsesParentPolicies() throws Exception {
        String topic = newScalableTopic();
        setLocal(topic, p -> p.setRetentionPolicies(new RetentionPolicies(60, 100)));
        PersistentTopic segment = loadSegment(segmentName(topic, 0));
        assertEquals(segment.getManagedLedger().getConfig().getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(60));
        assertEquals(segment.getManagedLedger().getConfig().getRetentionSizeInMB(), 100);
    }

    @Test(timeOut = 60_000)
    public void testSegmentDeletionKeepsParentPolicies() throws Exception {
        String topic = newScalableTopic();
        setLocal(topic, p -> p.setMessageTTLInSeconds(120));
        PersistentTopic segment = loadSegment(segmentName(topic, 0));
        getPulsar().getBrokerService().deleteTopic(segment.getName(), true).get();
        assertEquals(getPulsar().getTopicPoliciesService()
                .getTopicPoliciesAsync(TopicName.get(topic), TopicPoliciesService.GetType.LOCAL_ONLY).get()
                .orElseThrow().getMessageTTLInSeconds(), Integer.valueOf(120));
    }

    @Test(timeOut = 60_000)
    public void testSegmentUsesParentPoliciesName() throws Exception {
        String topic = newScalableTopic();
        assertEquals(loadSegment(segmentName(topic, 0)).getTopicPoliciesName(), TopicName.get(topic));
    }
}
