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
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Controller-leader failover across brokers: every broker that handled a request for a scalable
 * topic holds a controller for it, following the leader. When the leader goes away, the leader
 * election of one of them takes the controller lock over, and that broker must serve the topic.
 */
public class V5MultiBrokerControllerFailoverTest extends V5MultiBrokerClientBaseTest {

    @Test
    public void testStreamConsumerSubscribesAfterControllerFailover() throws Exception {
        String topic = newScalableTopic(2);
        TopicName tn = TopicName.get(topic);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        int formerLeader = findControllerLeaderIndex(topic);
        String formerLeaderId = brokers.get(formerLeader).getBrokerId();
        ScalableTopicController follower = scalableTopicService((formerLeader + 1) % brokers.size())
                .getOrCreateController(tn).get();

        // The leader releases the controller lock, as its broker does when it shuts down: the
        // election of one of the followers takes it over.
        scalableTopicService(formerLeader).releaseController(tn).get();
        Awaitility.await().untilAsserted(() -> assertThat(follower.getLeaderBrokerId().get())
                .as("controller leader after the former leader released the lock")
                .isPresent()
                .isNotEqualTo(Optional.of(formerLeaderId)));

        // The new leader serves the subscribe once it has taken over.
        @Cleanup
        StreamConsumer<String> consumer = Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .until(() -> v5Client.newStreamConsumer(Schema.string())
                        .topic(topic)
                        .subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                        .subscribe(), Objects::nonNull);
        producer.newMessage().key("k").value("v").send();
        Message<String> message = consumer.receive(Duration.ofSeconds(10));
        assertThat(message).as("message received under the new controller leader").isNotNull();
        assertThat(message.value()).isEqualTo("v");

        // The former leader, holding a controller for the topic again, follows the new leader.
        ScalableTopicController recreated = scalableTopicService(formerLeader).getOrCreateController(tn).get();
        assertThat(recreated.isLeader()).as("the former leader, back with a controller").isFalse();
    }

    private ScalableTopicService scalableTopicService(int brokerIndex) {
        return brokers.get(brokerIndex).getBrokerService().getScalableTopicService();
    }
}
