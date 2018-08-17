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
package org.apache.pulsar.client.tutorial;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class SubscriptionWithDeadLetter {

    private static final String topic = "persistent://public/default/my-topic";

    private static final int maxRedeliveryCount = 2;

    private static final int sendMessages = 500;

    public static void main(String[] args) throws PulsarClientException {

        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        for (int i = 0; i < sendMessages; i++) {
            producer.send("Hello Pulsar!".getBytes());
        }

        Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(10, TimeUnit.SECONDS)
                .maxRedeliveryCount(maxRedeliveryCount)
                .receiverQueueSize(100)
                .maxUnackedMessagesPerConsumer(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Consumer<byte[]> deadLetterConsumer = client.newConsumer(Schema.BYTES)
                .topic("persistent://public/default/my-topic-my-subscription-DLQ")
                .subscriptionName("my-subscription")
                .subscribe();

        int totalReceived = 0;
        do {
            Message<byte[]> msg = consumer.receive();
            totalReceived++;
            System.out.println(new String(msg.getData()));
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter = 0;
        do {
            Message<byte[]> msg = deadLetterConsumer.receive();
            totalInDeadLetter++;
            System.out.println(new String(msg.getData()));
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

        // test subscribe changed
        for (int i = 0; i < sendMessages; i++) {
            producer.send("Hello Pulsar!".getBytes());
        }

        Consumer<byte[]> consumer2 = client.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(10, TimeUnit.SECONDS)
                .maxRedeliveryCount(maxRedeliveryCount)
                .receiverQueueSize(100)
                .maxUnackedMessagesPerConsumer(100)
                .deadLetterTopic("persistent://public/default/my-topic-my-subscription-custom-DLQ")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Consumer<byte[]> deadLetterConsumer2 = client.newConsumer(Schema.BYTES)
                .topic("persistent://public/default/my-topic-my-subscription-custom-DLQ")
                .subscriptionName("my-subscription")
                .subscribe();

        int totalReceived2 = 0;
        do {
            Message<byte[]> msg = consumer2.receive();
            totalReceived2++;
            System.out.println(new String(msg.getData()));
        } while (totalReceived2 < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter2 = 0;
        do {
            Message<byte[]> msg = deadLetterConsumer2.receive();
            totalInDeadLetter2++;
            System.out.println(new String(msg.getData()));
        } while (totalInDeadLetter2 < sendMessages);

        deadLetterConsumer2.close();
        consumer2.close();
        producer.close();
        client.close();
    }
}
