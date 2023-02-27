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
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class AutoCloseUselessClientConMultiPartTest extends AutoCloseUselessClientConSupports {

    private static String topicName = UUID.randomUUID().toString().replaceAll("-","");
    private static String topicFullName = "persistent://public/default/" + topicName;

    @BeforeMethod
    public void before() throws PulsarAdminException {
        // Create Topics
        PulsarAdmin pulsarAdmin_0 = super.getAllAdmins().get(0);
        List<String> topicList = pulsarAdmin_0.topics().getList("public/default");
        if (!topicList.contains(topicName) && !topicList.contains(topicFullName + "-partition-0")
                && !topicList.contains(topicFullName)){
            pulsarAdmin_0.topics().createPartitionedTopic(topicFullName, 2);
        }
    }

    @Test
    public void testConnectionAutoReleasePartitionedTopic() throws Exception {
        // Init clients
        PulsarClientImpl pulsarClient = (PulsarClientImpl) super.getAllClients().get(0);
        Consumer consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-subscription-x")
                .subscribe();
        Producer producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        // Ensure producer and consumer works
        ensureProducerAndConsumerWorks(producer, consumer);
        // Connection to every Broker
        connectionToEveryBrokerWithUnloadBundle(pulsarClient);
        try {
            // Ensure that the consumer has reconnected finish after unload-bundle
            Awaitility.waitAtMost(Duration.ofSeconds(5)).until(consumer::isConnected);
        } catch (Exception e){
            // When consumer reconnect failure, create a new one.
            consumer.close();
            consumer = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(topicName)
                    .isAckReceiptEnabled(true)
                    .subscriptionName("my-subscription-x")
                    .subscribe();
        }
        try {
            // Ensure that the producer has reconnected finish after unload-bundle
            Awaitility.waitAtMost(Duration.ofSeconds(5)).until(producer::isConnected);
        } catch (Exception e){
            // When producer reconnect failure, create a new one.
            producer.close();
            producer = pulsarClient.newProducer(Schema.BYTES)
                    .topic(topicName)
                    .create();
        }
        // Assert "auto release works"
        trigReleaseConnection(pulsarClient);
        Awaitility.waitAtMost(Duration.ofSeconds(5)).until(()-> {
            // wait for async task done, then assert auto release success
            return pulsarClient.getCnxPool().getPoolSize() <= 2;
        });
        // Ensure all things still works
        ensureProducerAndConsumerWorks(producer, consumer);
        // Verify that the number of connections did not increase after the work was completed
        Assert.assertTrue(pulsarClient.getCnxPool().getPoolSize() <= 2);
        // Release sources
        consumer.close();
        producer.close();
    }
}
