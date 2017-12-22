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
package org.apache.pulsar.client.impl;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerImpl.SubscriptionMode;

public class ReaderImpl implements Reader<byte[]> {

    private final ConsumerImpl consumer;

    public ReaderImpl(PulsarClientImpl client, String topic, MessageId startMessageId,
        ReaderConfig<byte[]> readerConfiguration, ExecutorService listenerExecutor,
        CompletableFuture<Consumer<byte[]>> consumerFuture) {

        String subscription = "reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

        ConsumerConfiguration<byte[]> consumerConfiguration = new ConsumerConfiguration<>();
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setReceiverQueueSize(readerConfiguration.getReceiverQueueSize());
        if (readerConfiguration.getReaderName() != null) {
            consumerConfiguration.setConsumerName(readerConfiguration.getReaderName());
        }

        if (readerConfiguration.getReaderListener() != null) {
            ReaderListener<byte[]> readerListener = readerConfiguration.getReaderListener();
            consumerConfiguration.setMessageListener(new MessageListener<byte[]>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                    readerListener.received(ReaderImpl.this, msg);
                    consumer.acknowledgeCumulativeAsync(msg);
                }

                @Override
                public void reachedEndOfTopic(Consumer consumer) {
                    readerListener.reachedEndOfTopic(ReaderImpl.this);
                }
            });
        }

        consumerConfiguration.setCryptoFailureAction(readerConfiguration.getCryptoFailureAction());
        if (readerConfiguration.getCryptoKeyReader() != null) {
            consumerConfiguration.setCryptoKeyReader(readerConfiguration.getCryptoKeyReader());
        }

        consumer = new ConsumerImpl(client, topic, subscription, consumerConfiguration, listenerExecutor, -1,
                consumerFuture, SubscriptionMode.NonDurable, startMessageId);
    }

    @Override
    public String getTopic() {
        return consumer.getTopic();
    }

    public ConsumerImpl getConsumer() {
        return consumer;
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumer.hasReachedEndOfTopic();
    }

    @Override
    public Message readNext() throws PulsarClientException {
        Message msg = consumer.receive();

        // Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
        // it will specify the subscription position anyway
        consumer.acknowledgeCumulativeAsync(msg);
        return msg;
    }

    @Override
    public Message readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        Message msg = consumer.receive(timeout, unit);

        if (msg != null) {
            consumer.acknowledgeCumulativeAsync(msg);
        }
        return msg;
    }

    @Override
    public CompletableFuture<Message<byte[]>> readNextAsync() {
        return consumer.receiveAsync().thenApply(msg -> {
            consumer.acknowledgeCumulativeAsync(msg);
            return msg;
        });
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return consumer.closeAsync();
    }

}
