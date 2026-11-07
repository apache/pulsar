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
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting a scalable topic through the admin API must stop the topic's controller: once
 * the delete returns, the controller must not re-elect itself under the deleted path, accept
 * consumers against the deleted layout, or serve a topic re-created with the same name.
 */
public class V5ScalableTopicDeleteTest extends V5ClientBaseTest {

    @Test
    public void deleteReleasesElectedController() throws Exception {
        String topic = newScalableTopic(1);
        TopicName tn = TopicName.get(topic);
        ScalableTopicResources resources = scalableTopicResources();
        String lockPath = resources.controllerLockPath(tn);
        electController(topic);
        assertThat(resources.getStore().exists(lockPath).get())
                .as("controller lock after the consumer registered").isTrue();

        admin.scalableTopics().deleteScalableTopic(topic, true);

        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(resources.getStore().exists(lockPath).get())
                            .as("controller lock of the deleted topic").isFalse();
                    assertThat(resources.getStore().exists(resources.topicPath(tn)).get())
                            .as("record of the deleted topic").isFalse();
                });
    }

    @Test
    public void subscribeToDeletedTopicFails() throws Exception {
        String topic = newScalableTopic(1);
        TopicName tn = TopicName.get(topic);
        electController(topic);

        admin.scalableTopics().deleteScalableTopic(topic, true);

        assertThatThrownBy(() -> subscribe(topic, "sub", "late-consumer"))
                .hasMessageContaining("Failed to initialize controller");
        ScalableTopicResources resources = scalableTopicResources();
        assertThat(resources.getStore().exists(resources.consumerPath(tn, "sub", "late-consumer")).get())
                .as("consumer registration under the deleted topic").isFalse();
    }

    @Test
    public void recreatedTopicGetsFreshController() throws Exception {
        String topic = newScalableTopic(1);
        electController(topic);

        admin.scalableTopics().deleteScalableTopic(topic, true);
        admin.scalableTopics().createScalableTopic(topic, 2);

        ScalableConsumerAssignment assignment = subscribe(topic, "sub", "consumer");
        assertThat(assignment.getSegmentsCount())
                .as("segments assigned from the re-created topic's layout").isEqualTo(2);
    }

    private ScalableTopicResources scalableTopicResources() {
        return getPulsar().getPulsarResources().getScalableTopicResources();
    }

    /** A stream consumer registers with the topic's controller, which elects it on this broker. */
    private void electController(String topic) throws Exception {
        v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("elect")
                .subscribe()
                .close();
    }

    /** Send a raw scalable-topic subscribe to the broker and return the assignment it replies with. */
    private ScalableConsumerAssignment subscribe(String topic, String subscription, String consumerName)
            throws Exception {
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) newPulsarClient();
        ClientCnx cnx = client.getConnectionToServiceUrl().get(10, TimeUnit.SECONDS);
        long requestId = client.newRequestId();
        return cnx.<ScalableConsumerAssignment>sendScalableSessionRequest(
                Commands.newScalableTopicSubscribe(requestId, topic, subscription, consumerName, 1L,
                        ScalableConsumerType.STREAM),
                requestId).get(10, TimeUnit.SECONDS);
    }
}
