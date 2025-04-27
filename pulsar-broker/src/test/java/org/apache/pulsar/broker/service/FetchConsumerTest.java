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
package org.apache.pulsar.broker.service;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.FetchConsumer;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class FetchConsumerTest extends BrokerTestBase {


    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testFetchConsumerE2EWithMultiPartitionTopic() throws PulsarClientException {
        String topic = "persistent://prop/ns-abc/testFetchConsumerE2EWithMultiPartitionTopic";
        String subName = "testFetchConsumerE2EWithMultiPartitionTopic-sub";
        String startMsg = "startMsg";
        FetchConsumer<byte[]> fetchConsumer = pulsarClient.newFetchConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        MessageIdAdv startMessageId = (MessageIdAdv) producer.newMessage().value(startMsg.getBytes()).send();
        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(("msg_" + i).getBytes()).send();
        }

        Messages<byte[]> messages = fetchConsumer.fetchMessages(102, 10240, startMessageId, 10, TimeUnit.SECONDS);

        assert messages.size() == 102;

        messages.forEach(message -> {
            System.out.println("Fetched message: " + new String(message.getData()));
        });
    }
}
