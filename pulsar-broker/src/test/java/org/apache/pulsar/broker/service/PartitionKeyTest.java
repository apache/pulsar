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
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 */
@Test
public class PartitionKeyTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10000)
    public void testPartitionKey() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testPartitionKey";

        org.apache.pulsar.client.api.Consumer consumer = pulsarClient.subscribe(topicName, "my-subscription");

        // 1. producer with batch enabled
        ProducerConfiguration conf = new ProducerConfiguration();
        conf.setBatchingEnabled(true);
        Producer producerWithBatches = pulsarClient.createProducer(topicName, conf);

        // 2. Producer without batches
        Producer producerWithoutBatches = pulsarClient.createProducer(topicName);

        producerWithBatches.sendAsync(MessageBuilder.create().setKey("key-1").setContent("msg-1".getBytes()).build());
        producerWithBatches.sendAsync(MessageBuilder.create().setKey("key-2").setContent("msg-2".getBytes()).build())
                .get();

        producerWithoutBatches
                .sendAsync(MessageBuilder.create().setKey("key-3").setContent("msg-3".getBytes()).build());

        for (int i = 1; i <= 3; i++) {
            Message msg = consumer.receive();

            assertTrue(msg.hasKey());
            assertEquals(msg.getKey(), "key-" + i);
            assertEquals(msg.getData(), ("msg-" + i).getBytes());

            consumer.acknowledge(msg);
        }

    }

}
