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
package org.apache.pulsar.functions.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "functions-worker")
public class PulsarWorkerServiceCoordinationSubscriptionTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30000)
    public void testWorkersStartingTogether() throws Exception {
        String coordinationTopic = newTopicName();
        admin.topics().createNonPartitionedTopic(coordinationTopic);
        // Both workers find the subscription missing before either creates it. Unloading the topic then makes
        // both creations wait for the same topic load, so they overlap on the broker
        CyclicBarrier checked = new CyclicBarrier(2, () -> {
            try {
                admin.topics().unload(coordinationTopic);
            } catch (PulsarAdminException e) {
                throw new IllegalStateException(e);
            }
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> workers = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Topics topics = waitAfterFirstCheck(admin.topics(), coordinationTopic, checked);
                workers.add(executor.submit(() -> {
                    PulsarWorkerService.createCoordinationSubscriptionIfMissing(topics, coordinationTopic);
                    return null;
                }));
            }
            for (Future<?> worker : workers) {
                worker.get();
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(admin.topics().getSubscriptions(coordinationTopic))
                .containsExactly(MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION);
    }

    private static Topics waitAfterFirstCheck(Topics topics, String topic, CyclicBarrier barrier)
            throws PulsarAdminException {
        Topics spyTopics = spy(topics);
        doAnswer(invocation -> {
            Object subscriptions = invocation.callRealMethod();
            barrier.await(10, TimeUnit.SECONDS);
            return subscriptions;
        }).doCallRealMethod().when(spyTopics).getSubscriptions(topic);
        return spyTopics;
    }

    @Test
    public void testCreationFailureWithoutTheSubscription() throws Exception {
        String coordinationTopic = newTopicName();
        admin.topics().createSubscription(coordinationTopic, "other", MessageId.earliest);
        // A topic that reached its subscription limit also fails the creation with 409 Conflict
        admin.namespaces().setMaxSubscriptionsPerTopic(getNamespace(), 1);
        PersistentTopic topic = (PersistentTopic) getTopicReference(coordinationTopic).orElseThrow();
        Awaitility.await().until(() -> topic.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().get() == 1);

        assertThatThrownBy(() ->
                PulsarWorkerService.createCoordinationSubscriptionIfMissing(admin.topics(), coordinationTopic))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("maximum number of subscriptions");
        assertThat(admin.topics().getSubscriptions(coordinationTopic)).containsExactly("other");
    }
}
