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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Topic-level policies of a scalable topic, written through {@code /admin/v2/topic} with
 * {@code admin.topicPolicies()} and applied by its segments.
 */
@Test(groups = "broker-admin")
public class ScalableTopicPoliciesTest extends SharedPulsarBaseTest {

    private String newScalableTopic() throws Exception {
        String topic = "topic://" + getNamespace() + "/policies-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 1);
        admin.scalableTopics().setAutoScalePolicy(topic, AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private PersistentTopic loadSegment(String topic) throws Exception {
        String name = admin.scalableTopics().getStats(topic).getLayout().getSegments().get(0L).getName();
        return (PersistentTopic) getPulsar().getBrokerService().getTopic(name, false).get().orElseThrow();
    }

    private static void awaitSegmentTTL(PersistentTopic segment, int expected) {
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(segment.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(),
                        Integer.valueOf(expected)));
    }

    @Test(timeOut = 60_000)
    public void testRetentionRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        RetentionPolicies retention = new RetentionPolicies(60, 100);
        tp.setRetention(topic, retention);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getRetention(topic), retention));
        tp.removeRetention(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getRetention(topic)));
    }

    @Test(timeOut = 60_000)
    public void testMessageTTLRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        tp.setMessageTTL(topic, 120);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getMessageTTL(topic), Integer.valueOf(120)));
        tp.removeMessageTTL(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getMessageTTL(topic)));
    }

    @Test(timeOut = 60_000)
    public void testPersistenceRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        PersistencePolicies persistence = new PersistencePolicies(1, 1, 1, 0.0);
        tp.setPersistence(topic, persistence);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getPersistence(topic), persistence));
        tp.removePersistence(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getPersistence(topic)));
    }

    @Test(timeOut = 60_000)
    public void testMaxMessageSizeRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        tp.setMaxMessageSize(topic, 1024);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getMaxMessageSize(topic), Integer.valueOf(1024)));
        tp.removeMaxMessageSize(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getMaxMessageSize(topic)));
    }

    @Test(timeOut = 60_000)
    public void testDelayedDeliveryRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        tp.setDelayedDeliveryPolicy(topic, DelayedDeliveryPolicies.builder().active(true).tickTime(2000).build());
        Awaitility.await().untilAsserted(() -> {
            DelayedDeliveryPolicies policies = tp.getDelayedDeliveryPolicy(topic);
            assertEquals(policies.getTickTime(), 2000);
            assertTrue(policies.isActive());
        });
        tp.removeDelayedDeliveryPolicy(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getDelayedDeliveryPolicy(topic)));
    }

    @Test(timeOut = 60_000)
    public void testOffloadPoliciesRoundTrip() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(100L);
        offloadPolicies.setManagedLedgerOffloadThresholdInSeconds(100L);
        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(200L);
        offloadPolicies.setManagedLedgerOffloadDriver("s3");
        offloadPolicies.setManagedLedgerOffloadBucket("buck");
        tp.setOffloadPolicies(topic, offloadPolicies);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getOffloadPolicies(topic), offloadPolicies));
        tp.removeOffloadPolicies(topic);
        Awaitility.await().untilAsserted(() -> assertNull(tp.getOffloadPolicies(topic)));
    }

    @Test(timeOut = 60_000)
    public void testAppliedFallsBackToNamespace() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        RetentionPolicies namespaceRetention = new RetentionPolicies(30, 10);
        admin.namespaces().setRetention(getNamespace(), namespaceRetention);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getRetention(topic, true), namespaceRetention));
        assertNull(tp.getRetention(topic));
    }

    @Test(timeOut = 60_000)
    public void testLocalTakesPrecedenceOverGlobal() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        PersistentTopic segment = loadSegment(topic);
        admin.topicPolicies(true).setMessageTTL(topic, 300);
        tp.setMessageTTL(topic, 60);
        awaitSegmentTTL(segment, 60);
        tp.removeMessageTTL(topic);
        awaitSegmentTTL(segment, 300);
    }

    @Test(timeOut = 60_000)
    public void testAdminSetPolicyReachesLoadedSegment() throws Exception {
        String topic = newScalableTopic();
        PersistentTopic segment = loadSegment(topic);
        admin.topicPolicies().setMessageTTL(topic, 90);
        awaitSegmentTTL(segment, 90);
    }

    @Test(timeOut = 60_000)
    public void testMissingTopicReturns404() {
        TopicPolicies tp = admin.topicPolicies();
        PulsarAdminException.NotFoundException e = expectThrows(PulsarAdminException.NotFoundException.class,
                () -> tp.getRetention("topic://" + getNamespace() + "/missing"));
        assertTrue(e.getMessage().contains("Scalable topic not found"), e.getMessage());
    }

    @Test(timeOut = 60_000)
    public void testUnsupportedPolicyReturns405() throws Exception {
        String topic = newScalableTopic();
        PulsarAdminException.NotAllowedException e = expectThrows(PulsarAdminException.NotAllowedException.class,
                () -> admin.topicPolicies().setMaxProducers(topic, 1));
        assertTrue(e.getMessage().contains("Supported topic policies: retention, messageTTL"), e.getMessage());
        // A policy further down the path, at {subName}/dispatchRate
        PulsarAdminException.NotAllowedException deeper = expectThrows(
                PulsarAdminException.NotAllowedException.class,
                () -> admin.topicPolicies().setSubscriptionDispatchRate(topic, "sub",
                        DispatchRate.builder().dispatchThrottlingRateInMsg(100).build()));
        assertTrue(deeper.getMessage().contains("Supported topic policies: retention, messageTTL"),
                deeper.getMessage());
    }

    @Test(timeOut = 60_000)
    public void testPartitionSuffixedNameRejected() throws Exception {
        String topic = "topic://" + getNamespace() + "/x-partition-0";
        // The policies service keys policies by the partitioned topic name, so these would be the ones of "x"
        expectThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.topicPolicies().getRetention(topic));
    }

    @Test(timeOut = 60_000)
    public void testDeletingPartitionSuffixedRecordKeepsSiblingPolicies() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        tp.setMessageTTL(topic, 120);
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getMessageTTL(topic), Integer.valueOf(120)));
        // A scalable topic record under a "-partition-N" name of the topic, seeded directly in the metadata store
        String sibling = topic + "-partition-0";
        getPulsar().getPulsarResources().getScalableTopicResources().createScalableTopicAsync(TopicName.get(sibling),
                ScalableTopicController.createInitialMetadata(1, 4, Map.of())).get();
        admin.scalableTopics().deleteScalableTopic(sibling, true);
        // The delete returns once the policies service applied it, so policies it removed would be gone already
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getMessageTTL(topic), Integer.valueOf(120)));
    }

    @Test(timeOut = 60_000)
    public void testOtherTopicOperationReturns405() throws Exception {
        String topic = newScalableTopic();
        PulsarAdminException.NotAllowedException e = expectThrows(PulsarAdminException.NotAllowedException.class,
                () -> admin.topics().getStats(topic));
        assertTrue(e.getMessage().contains("pulsar-admin scalable-topics"), e.getMessage());
        // A PUT, here a policy that is not supported
        PulsarAdminException.NotAllowedException put = expectThrows(PulsarAdminException.NotAllowedException.class,
                () -> admin.topicPolicies().setSchemaCompatibilityStrategy(topic,
                        SchemaCompatibilityStrategy.FULL));
        assertTrue(put.getMessage().contains("pulsar-admin scalable-topics"), put.getMessage());
        // An operation on the topic path itself
        PulsarAdminException.NotAllowedException delete = expectThrows(
                PulsarAdminException.NotAllowedException.class, () -> admin.topics().delete(topic));
        assertTrue(delete.getMessage().contains("pulsar-admin scalable-topics"), delete.getMessage());
    }

    @Test(timeOut = 60_000)
    public void testDeleteScalableTopicRemovesItsPolicies() throws Exception {
        String topic = newScalableTopic();
        TopicPolicies tp = admin.topicPolicies();
        tp.setMessageTTL(topic, 120);
        tp.setRetention(topic, new RetentionPolicies(60, 100));
        Awaitility.await().untilAsserted(() -> assertEquals(tp.getMessageTTL(topic), Integer.valueOf(120)));
        admin.scalableTopics().deleteScalableTopic(topic, true);
        admin.scalableTopics().createScalableTopic(topic, 1);
        // A topic created again with the same name doesn't inherit the policies of the deleted one
        Awaitility.await().untilAsserted(() -> {
            assertNull(tp.getMessageTTL(topic));
            assertNull(tp.getRetention(topic));
        });
    }
}
