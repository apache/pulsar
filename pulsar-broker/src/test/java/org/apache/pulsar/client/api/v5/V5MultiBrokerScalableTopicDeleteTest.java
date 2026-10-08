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
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting a scalable topic through a broker other than its controller leader must stop the
 * controller on the leader, before the metadata delete removes the controller lock.
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
}
