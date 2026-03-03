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
package org.apache.pulsar.client.impl;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class AutoCloseUselessClientConMultiTopicTest extends AutoCloseUselessClientConSupports {

    private static final String TOPIC_NAME_1 = UUID.randomUUID().toString().replaceAll("-", "");
    private static final String TOPIC_FULL_NAME_1 = "persistent://public/default/" + TOPIC_NAME_1;
    private static final String TOPIC_NAME_2 = UUID.randomUUID().toString().replaceAll("-", "");
    private static final String TOPIC_FULL_NAME_2 = "persistent://public/default/" + TOPIC_NAME_2;

    @BeforeMethod
    public void before() throws PulsarAdminException {
        // Create Topics
        PulsarAdmin pulsarAdmin0 = super.getAllAdmins().get(0);
        List<String> topicList = pulsarAdmin0.topics().getList("public/default");
        if (!topicList.contains(TOPIC_NAME_1) && !topicList.contains(TOPIC_FULL_NAME_1 + "-partition-0")
                && !topicList.contains(TOPIC_FULL_NAME_1)){
            pulsarAdmin0.topics().createNonPartitionedTopic(TOPIC_FULL_NAME_1);
        }
        if (!topicList.contains(TOPIC_NAME_2) && !topicList.contains(TOPIC_FULL_NAME_2 + "-partition-0")
                && !topicList.contains(TOPIC_FULL_NAME_2)){
            pulsarAdmin0.topics().createNonPartitionedTopic(TOPIC_FULL_NAME_2);
        }
    }

    @Test
    public void testConnectionAutoReleaseMultiTopic() throws Exception {
        // Init clients
        PulsarClientImpl pulsarClient = (PulsarClientImpl) super.getAllClients().get(0);
        Consumer consumer = pulsarClient.newConsumer()
                .topic(TOPIC_NAME_1, TOPIC_NAME_2)
                .subscriptionName("my-subscription-x")
                .isAckReceiptEnabled(true)
                .subscribe();
        Producer producer1 = pulsarClient.newProducer()
                .topic(TOPIC_NAME_1)
                .create();
        Producer producer2 = pulsarClient.newProducer()
                .topic(TOPIC_NAME_2)
                .create();
        // Ensure producer and consumer works
        ensureProducerAndConsumerWorks(producer1, producer2, consumer);
        // Connection to every Broker
        connectionToEveryBrokerWithUnloadBundle(pulsarClient);
        // Ensure that the client has reconnected finish after unload-bundle
        try {
            // Ensure that the consumer has reconnected finish after unload-bundle
            Awaitility.waitAtMost(Duration.ofSeconds(5)).until(consumer::isConnected);
        } catch (Exception e){
            // When consumer reconnect failure, create a new one.
            consumer.close();
            consumer = pulsarClient.newConsumer()
                    .topic(TOPIC_NAME_1, TOPIC_NAME_2)
                    .subscriptionName("my-subscription-x")
                    .isAckReceiptEnabled(true)
                    .subscribe();
        }
        try {
            // Ensure that the producer has reconnected finish after unload-bundle
            Awaitility.waitAtMost(Duration.ofSeconds(5)).until(producer1::isConnected);
        } catch (Exception e){
            // When producer reconnect failure, create a new one.
            producer1.close();
            producer1 = pulsarClient.newProducer()
                    .topic(TOPIC_NAME_1)
                    .create();
        }
        try {
            // Ensure that the producer has reconnected finish after unload-bundle
            Awaitility.waitAtMost(Duration.ofSeconds(5)).until(producer2::isConnected);
        } catch (Exception e){
            // When producer reconnect failure, create a new one.
            producer2.close();
            producer2 = pulsarClient.newProducer()
                    .topic(TOPIC_NAME_2)
                    .create();
        }
        // Assert "auto release works"
        trigReleaseConnection(pulsarClient);
        Awaitility.waitAtMost(Duration.ofSeconds(30)).until(()-> {
            // Wait for async task done, then assert auto release success
            return pulsarClient.getCnxPool().getPoolSize() == 1;
        });
        // Ensure all things still works
        ensureProducerAndConsumerWorks(producer1, producer2, consumer);
        // Release sources
        consumer.close();
        producer1.close();
        producer2.close();
    }
}
