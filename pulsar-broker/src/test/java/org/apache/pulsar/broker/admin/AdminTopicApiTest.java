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
package org.apache.pulsar.broker.admin;

import lombok.Cleanup;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

@Test(groups = "broker-admin")
public class AdminTopicApiTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(AdminTopicApiTest.class);

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPeekMessages() throws Exception {
        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
            .serviceUrl(lookupUrl.toString())
            .build();

        final String topic = "persistent://my-property/my-ns/test-publish-timestamp";

        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("my-sub")
            .subscribe();

        final int numMessages = 5;

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(3, TimeUnit.SECONDS)
                .batchingMaxMessages(5)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.newMessage()
                .value(("value-" + i).getBytes(UTF_8))
                .sendAsync();
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info("Received message '{}'.", new String(msg.getValue(), UTF_8));
        }
        List<Message<byte[]>> messages = admin.topics().peekMessages(topic, "my-sub", 5);
        Assert.assertEquals(new String(messages.get(0).getValue(), UTF_8), "value-0");
        Assert.assertEquals(new String(messages.get(1).getValue(), UTF_8), "value-1");
        Assert.assertEquals(new String(messages.get(2).getValue(), UTF_8), "value-2");
        Assert.assertEquals(new String(messages.get(3).getValue(), UTF_8), "value-3");
        Assert.assertEquals(new String(messages.get(4).getValue(), UTF_8), "value-4");
    }
}
