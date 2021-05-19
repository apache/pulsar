/**
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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PartitionKeyTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10000)
    public void testPartitionKey() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testPartitionKey";

        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").subscribe();

        // 1. producer with batch enabled
        Producer<byte[]> producerWithBatches = pulsarClient.newProducer().topic(topicName).enableBatching(true)
                .create();


        // 2. Producer without batches
        Producer<byte[]> producerWithoutBatches = pulsarClient.newProducer().topic(topicName).create();

        producerWithBatches.newMessage().key("key-1").value("msg-1".getBytes()).sendAsync();
        producerWithBatches.newMessage().key("key-2").value("msg-2".getBytes()).send();

        producerWithoutBatches.newMessage().key("key-3").value("msg-3".getBytes()).sendAsync();

        for (int i = 1; i <= 3; i++) {
            Message<byte[]> msg = consumer.receive();

            assertTrue(msg.hasKey());
            assertEquals(msg.getKey(), "key-" + i);
            assertEquals(msg.getData(), ("msg-" + i).getBytes());

            consumer.acknowledge(msg);
        }

    }

}
