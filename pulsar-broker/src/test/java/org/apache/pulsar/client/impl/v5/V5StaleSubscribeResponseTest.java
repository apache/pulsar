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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.scalable.ConsumerAssignment;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A member's subscribe response must not override a newer assignment the controller pushed
 * meanwhile. The coordinator computes a registering member's assignment under its monitor, but
 * the response used to be written only once the registration completed; a second member that
 * registered in between rebalanced the group and pushed the first member a newer assignment with
 * the same layout epoch, which reached the client ahead of the older response.
 *
 * <p>Lives in {@code org.apache.pulsar.client.impl.v5} to drive {@link ScalableConsumerClient}
 * directly and read its assignment. The broker's {@link ServerCnx} holds a member's registration,
 * as the connection sees it, until the test releases it: that widens the window between the
 * coordinator computing the assignment and the connection seeing the registration complete.
 */
public class V5StaleSubscribeResponseTest extends MockedPulsarServiceBaseTest {

    private record HeldRegistration(CompletableFuture<Void> registered, CompletableFuture<Void> release) {
    }

    /** Registrations to hold, by consumer name. */
    private final Map<String, HeldRegistration> heldRegistrations = new ConcurrentHashMap<>();

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        // The scalable consumer session needs a binary-protocol client.
        isTcpLookup = true;
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void releaseHeldRegistrations() {
        heldRegistrations.values().forEach(held -> held.release().complete(null));
        heldRegistrations.clear();
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        brokerService.setPulsarChannelInitializerFactory(
                (pulsar, opts) -> new PulsarChannelInitializer(pulsar, opts) {
                    @Override
                    protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName)
                            throws Exception {
                        return new HoldingServerCnx(pulsar, listenerName);
                    }
                });
        return brokerService;
    }

    private class HoldingServerCnx extends ServerCnx {

        HoldingServerCnx(PulsarService pulsar, String listenerName) {
            super(pulsar, listenerName);
        }

        @Override
        protected CompletableFuture<ConsumerAssignment> registerScalableConsumer(
                ScalableTopicService scalableTopicService, TopicName topicName, String subscription,
                String consumerName, long consumerId, ScalableConsumerType consumerType) {
            CompletableFuture<ConsumerAssignment> registration = super.registerScalableConsumer(
                    scalableTopicService, topicName, subscription, consumerName, consumerId, consumerType);
            HeldRegistration held = heldRegistrations.get(consumerName);
            if (held == null) {
                return registration;
            }
            // Completes once the coordinator has registered the consumer and computed its
            // assignment; the connection sees the registration only when the test releases it.
            registration.thenRun(() -> held.registered().complete(null));
            return registration.thenCompose(assignment -> held.release().thenApply(__ -> assignment));
        }
    }

    @Test(timeOut = 60_000)
    public void testLateSubscribeResponseDoesNotOverrideRebalance() throws Exception {
        String topic = "topic://public/default/stale-subscribe-response-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 2);
        // Pin the layout: only the two registrations below may change the assignment.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        String group = "group";
        HeldRegistration memberAHeld = new HeldRegistration(new CompletableFuture<>(), new CompletableFuture<>());
        heldRegistrations.put("member-a", memberAHeld);

        PulsarClientImpl v4Client = (PulsarClientImpl) pulsarClient;
        @Cleanup
        ScalableConsumerClient memberA = new ScalableConsumerClient(v4Client, TopicName.get(topic), group,
                "member-a", ScalableConsumerType.CHECKPOINT);
        CompletableFuture<List<ActiveSegment>> memberAStart = memberA.start();
        memberAHeld.registered().get(30, TimeUnit.SECONDS);

        // member-b joins while member-a's registration is held: the coordinator rebalances and
        // pushes member-a its share of the two segments.
        @Cleanup
        ScalableConsumerClient memberB = new ScalableConsumerClient(v4Client, TopicName.get(topic), group,
                "member-b", ScalableConsumerType.CHECKPOINT);
        memberB.start().get(30, TimeUnit.SECONDS);

        memberAHeld.release().complete(null);
        memberAStart.get(30, TimeUnit.SECONDS);

        Map<String, Set<Long>> controllerView = assignedSegmentIds(topic, group);
        assertThat(controllerView.get("member-a")).as("controller assignment of member-a").hasSize(1);
        assertThat(controllerView.get("member-b")).as("controller assignment of member-b").hasSize(1);
        Awaitility.await().untilAsserted(() -> {
            assertThat(segmentIds(memberA.currentAssignment())).as("member-a assignment")
                    .isEqualTo(controllerView.get("member-a"));
            assertThat(segmentIds(memberB.currentAssignment())).as("member-b assignment")
                    .isEqualTo(controllerView.get("member-b"));
        });
    }

    private Map<String, Set<Long>> assignedSegmentIds(String topic, String group) throws Exception {
        ScalableTopicStats.SubscriptionStats sub = admin.scalableTopics().getStats(topic)
                .getSubscriptions().get(group);
        Map<String, Set<Long>> assigned = new HashMap<>();
        for (ScalableTopicStats.ConsumerStats consumer : sub.getConsumers()) {
            assigned.put(consumer.getConsumerName(), new HashSet<>(consumer.getSegmentIds()));
        }
        return assigned;
    }

    private static Set<Long> segmentIds(List<ActiveSegment> segments) {
        return segments.stream().map(ActiveSegment::segmentId).collect(Collectors.toSet());
    }
}
