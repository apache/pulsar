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
package org.apache.pulsar.client.examples;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;

/**
 * Example to use Pulsar transactions.
 *
 * <p>TODO: add an example about how to use the Pulsar transaction API.
 */
public class TransactionExample {

    public static void main(String[] args) throws Exception {
        String serviceUrl = "pulsar://localhost:6650";

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build();

        String inputTopic = "input-topic";
        String outputTopic1 = "output-topic-1";
        String outputTopic2 = "output-topic-2";

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(inputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("transactional-sub")
            .subscribe();

        Producer<String> producer1 = client.newProducer(Schema.STRING)
            .topic(outputTopic1)
            .sendTimeout(0, TimeUnit.MILLISECONDS)
            .create();

        Producer<String> producer2 = client.newProducer(Schema.STRING)
            .topic(outputTopic2)
            .sendTimeout(0, TimeUnit.MILLISECONDS)
            .create();


        while (true) {
            Message<String> message = consumer.receive();

            // process the messages to generate other messages
            String outputMessage1 = message.getValue() + "-output-1";
            String outputMessage2= message.getValue() + "-output-2";

            Transaction txn = client.newTransaction()
                .withTransactionTimeout(1, TimeUnit.MINUTES)
                .build().get();

            CompletableFuture<MessageId> sendFuture1 = producer1.newMessage(txn)
                .value(outputMessage1)
                .sendAsync();

            CompletableFuture<MessageId> sendFuture2 = producer2.newMessage(txn)
                .value(outputMessage2)
                .sendAsync();

            CompletableFuture<Void> ackFuture = consumer.acknowledgeAsync(message.getMessageId(), txn);

            txn.commit().get();

            // the message ids can be returned from the sendFuture1 and sendFuture2

            MessageId msgId1 = sendFuture1.get();
            MessageId msgId2 = sendFuture2.get();
        }
    }

}
