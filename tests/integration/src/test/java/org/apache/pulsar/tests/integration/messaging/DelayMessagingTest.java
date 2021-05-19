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
package org.apache.pulsar.tests.integration.messaging;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Delay messaging test.
 */
@Slf4j
public class DelayMessagingTest extends PulsarTestSuite {

    @Test(dataProvider = "ServiceUrls")
    public void delayMsgBlockTest(Supplier<String> serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topic = generateTopicName(nsName, "testDelayMsgBlock", true);
        pulsarCluster.createPartitionedTopic(topic, 3);

        String retryTopic = topic + "-RETRY";
        String deadLetterTopic = topic + "-DLT";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl.get()).build();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int redeliverCnt = 10;
        final int delayTimeSeconds = 5;
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(redeliverCnt)
                        .retryLetterTopic(retryTopic)
                        .deadLetterTopic(deadLetterTopic)
                        .build())
                .receiverQueueSize(100)
                .ackTimeout(60, TimeUnit.SECONDS)
                .subscribe();

        producer.newMessage().value("hello".getBytes()).send();

        // receive message at first time
        Message<byte[]> message = consumer.receive(delayTimeSeconds * 2, TimeUnit.SECONDS);
        Assert.assertNotNull(message, "Can't receive message at the first time.");
        consumer.reconsumeLater(message, delayTimeSeconds, TimeUnit.SECONDS);

        // receive retry messages
        for (int i = 0; i < redeliverCnt; i++) {
            message = consumer.receive(delayTimeSeconds * 2, TimeUnit.SECONDS);
            Assert.assertNotNull(message, "Consumer can't receive message in double delayTimeSeconds time "
                    + delayTimeSeconds * 2 + "s");
            log.info("receive msg. reConsumeTimes: {}", message.getProperty("RECONSUMETIMES"));
            consumer.reconsumeLater(message, delayTimeSeconds, TimeUnit.SECONDS);
        }

        @Cleanup
        Consumer<byte[]> dltConsumer = pulsarClient.newConsumer()
                .topic(deadLetterTopic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        message = dltConsumer.receive(10, TimeUnit.SECONDS);
        Assert.assertNotNull(message, "Dead letter topic consumer can't receive message.");
    }

}
