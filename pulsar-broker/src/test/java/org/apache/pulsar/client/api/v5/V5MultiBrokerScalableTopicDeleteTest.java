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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting a scalable topic must stop its controllers before the metadata delete removes the
 * controller lock: the leader's, also when the delete goes through another broker, and the ones
 * following it on the other brokers, which must not take the lock over.
 */
public class V5MultiBrokerScalableTopicDeleteTest extends V5MultiBrokerClientBaseTest {

    @Test
    public void deleteThroughNonLeaderBrokerStopsController() throws Exception {
        String topic = newScalableTopic(1);
        TopicName tn = TopicName.get(topic);
        // Elect the controller on broker 0 only: no other broker holds one for the topic.
        ScalableTopicService leaderService = brokers.get(0).getBrokerService().getScalableTopicService();
        leaderService.getOrCreateController(tn).get(10, TimeUnit.SECONDS);
        ScalableTopicResources resources = brokers.get(0).getPulsarResources().getScalableTopicResources();
        String lockPath = resources.controllerLockPath(tn);
        assertThat(resources.getStore().exists(lockPath).get())
                .as("controller lock after the election").isTrue();

        admins.get(1).scalableTopics().deleteScalableTopic(topic, true);

        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(resources.getStore().exists(lockPath).get())
                            .as("controller lock of the deleted topic").isFalse();
                    assertThat(resources.getStore().exists(resources.topicPath(tn)).get())
                            .as("record of the deleted topic").isFalse();
                });
        assertThat(admins.get(1).scalableTopics().listScalableTopics(getNamespace()))
                .doesNotContain(topic);
        assertThatThrownBy(() -> leaderService.getOrCreateController(tn).get(10, TimeUnit.SECONDS))
                .hasMessageContaining("Failed to initialize controller");
    }

    /**
     * With a controller on every broker, the delete removes the lock of the leader while the
     * other brokers follow it. A follower's leader election takes the lock over right away, under
     * the topic being deleted, so the record delete fails (NotEmpty on Oxia and ZooKeeper) or the
     * lock outlives the record.
     */
    @Test
    public void deleteThroughLeaderStopsFollowingControllers() throws Exception {
        String topic = newScalableTopic(1);
        // A subscription gives the metadata delete more to remove before it reaches the record.
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        // Materializes the controller on every broker: the leader and two followers.
        int leaderIndex = findControllerLeaderIndex(topic);

        admins.get(leaderIndex).scalableTopics().deleteScalableTopic(topic, true);

        assertTopicStaysDeleted(topic);
    }

    @Test
    public void deleteThroughNonLeaderStopsFollowingControllers() throws Exception {
        String topic = newScalableTopic(1);
        admin.scalableTopics().createSubscription(topic, "sub", ScalableSubscriptionType.STREAM);
        int leaderIndex = findControllerLeaderIndex(topic);

        admins.get((leaderIndex + 1) % brokers.size()).scalableTopics().deleteScalableTopic(topic, true);

        assertTopicStaysDeleted(topic);
    }

    /**
     * Followers must still take over when the leader releases its controller while the topic is
     * not being deleted (the leader broker unloads it or shuts down), and the new leader must
     * serve as one.
     */
    @Test
    public void followerTakesOverReleasedController() throws Exception {
        String topic = newScalableTopic(1);
        TopicName tn = TopicName.get(topic);
        int leaderIndex = findControllerLeaderIndex(topic);

        brokers.get(leaderIndex).getBrokerService().getScalableTopicService().releaseController(tn)
                .get(10, TimeUnit.SECONDS);

        ScalableTopicResources resources = brokers.get(0).getPulsarResources().getScalableTopicResources();
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<String> leaders = new ArrayList<>();
            for (int i = 0; i < brokers.size(); i++) {
                if (i != leaderIndex && brokers.get(i).getBrokerService().getScalableTopicService()
                        .getOrCreateController(tn).get(10, TimeUnit.SECONDS).isLeader()) {
                    leaders.add(brokers.get(i).getBrokerId());
                }
            }
            assertThat(leaders).as("followers that took the controller over").hasSize(1);
            Optional<GetResult> lock = resources.getStore().get(resources.controllerLockPath(tn)).get();
            assertThat(lock).as("controller lock").isPresent();
            assertThat(ObjectMapperFactory.getMapper().reader().readValue(lock.get().getValue(), String.class))
                    .as("broker holding the controller lock").isEqualTo(leaders.get(0));
        });
    }

    private void assertTopicStaysDeleted(String topic) throws Exception {
        TopicName tn = TopicName.get(topic);
        ScalableTopicResources resources = brokers.get(0).getPulsarResources().getScalableTopicResources();
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(resources.getStore().get(resources.controllerLockPath(tn)).get())
                            .as("controller lock of the deleted topic").isEmpty();
                    assertThat(resources.getStore().get(resources.topicPath(tn)).get())
                            .as("record of the deleted topic").isEmpty();
                });
        assertThat(admin.scalableTopics().listScalableTopics(getNamespace())).doesNotContain(topic);
        // No broker keeps a controller for the deleted topic: getting one creates a new controller,
        // which fails to initialize.
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            for (PulsarService broker : brokers) {
                assertThatThrownBy(() -> broker.getBrokerService().getScalableTopicService()
                        .getOrCreateController(tn).get(10, TimeUnit.SECONDS))
                        .as("controller of the deleted topic on " + broker.getBrokerId())
                        .hasMessageContaining("Failed to initialize controller");
            }
        });
    }
}
