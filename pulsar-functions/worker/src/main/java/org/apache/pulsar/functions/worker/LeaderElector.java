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
package org.apache.pulsar.functions.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * A simple implementation of leader election using a pulsar topic.
 */
@Slf4j
public class LeaderElector implements AutoCloseable {

    private final Producer producer;
    private final Consumer consumer;
    private String workerId;

    LeaderElector(String workerId, String topic, PulsarClient client) throws PulsarClientException {
        producer = client.createProducer(topic);
        consumer = client.subscribe(topic, "participants",
                new ConsumerConfiguration()
                        .setSubscriptionType(SubscriptionType.Failover));
        this.workerId = workerId;
    }

    public CompletableFuture<Boolean> becomeLeader() {
        long time = System.currentTimeMillis();
        String str = String.format("%s-%d", this.workerId, time);
        return producer.sendAsync(str.getBytes())
                .thenCompose(messageId -> LeaderElector.this.receiveTillMessage((MessageIdImpl) messageId))
                .exceptionally(cause -> false);
    }

    private CompletableFuture<Boolean> receiveTillMessage(MessageIdImpl endMsgId) {
        CompletableFuture<Boolean> finalFuture = new CompletableFuture<>();
        receiveOne(endMsgId, finalFuture);
        return finalFuture;
    }

    private void receiveOne(MessageIdImpl endMsgId, CompletableFuture<Boolean> finalFuture) {
        consumer.receiveAsync()
                .thenAccept(message -> {
                    MessageIdImpl idReceived = (MessageIdImpl) message.getMessageId();
                    int compareResult = idReceived.compareTo(endMsgId);
                    if (compareResult < 0) {
                        // drop the message
                        consumer.acknowledgeCumulativeAsync(message);
                        // receive next message
                        receiveOne(endMsgId, finalFuture);
                        return;
                    } else if (compareResult > 0) {
                        // the end message is consumed by other participants, which it means some other
                        // consumers take over the leadership at some time. so `becomeLeader` fails
                        finalFuture.complete(false);
                        return;
                    } else {
                        // i got what I published, i become the leader
                        consumer.acknowledgeCumulativeAsync(message);
                        finalFuture.complete(true);
                        return;
                    }
                })
                .exceptionally(cause -> {
                    finalFuture.completeExceptionally(cause);
                    return null;
                });
    }

    @Override
    public void close() throws PulsarClientException {
        producer.close();
        consumer.close();
    }

}