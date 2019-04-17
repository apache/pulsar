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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerImpl.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderImpl<T> implements Reader<T> {

    private static final Logger log = LoggerFactory.getLogger(ReaderImpl.class);

    private final ConsumerImpl<T> consumer;
    private final Long startPublishTime;
    private boolean seekDone;

    public ReaderImpl(PulsarClientImpl client, ReaderConfigurationData<T> readerConfiguration,
                      ExecutorService listenerExecutor, CompletableFuture<Consumer<T>> consumerFuture, Schema<T> schema) {

        String subscription = "reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);
        if (StringUtils.isNotBlank(readerConfiguration.getSubscriptionRolePrefix())) {
            subscription = readerConfiguration.getSubscriptionRolePrefix() + "-" + subscription;
        }

        ConsumerConfigurationData<T> consumerConfiguration = new ConsumerConfigurationData<>();
        consumerConfiguration.getTopicNames().add(readerConfiguration.getTopicName());
        consumerConfiguration.setSubscriptionName(subscription);
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setReceiverQueueSize(readerConfiguration.getReceiverQueueSize());
        consumerConfiguration.setReadCompacted(readerConfiguration.isReadCompacted());
        if (readerConfiguration.getReaderName() != null) {
            consumerConfiguration.setConsumerName(readerConfiguration.getReaderName());
        }

        if (readerConfiguration.getReaderListener() != null) {
            ReaderListener<T> readerListener = readerConfiguration.getReaderListener();
            consumerConfiguration.setMessageListener(new MessageListener<T>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void received(Consumer<T> consumer, Message<T> msg) {
                    readerListener.received(ReaderImpl.this, msg);
                    consumer.acknowledgeCumulativeAsync(msg);
                }

                @Override
                public void reachedEndOfTopic(Consumer<T> consumer) {
                    readerListener.reachedEndOfTopic(ReaderImpl.this);
                }
            });
        }

        consumerConfiguration.setCryptoFailureAction(readerConfiguration.getCryptoFailureAction());
        if (readerConfiguration.getCryptoKeyReader() != null) {
            consumerConfiguration.setCryptoKeyReader(readerConfiguration.getCryptoKeyReader());
        }

        final int partitionIdx = TopicName.getPartitionIndex(readerConfiguration.getTopicName());
        consumer = new ConsumerImpl<>(client, readerConfiguration.getTopicName(), consumerConfiguration, listenerExecutor,
                partitionIdx, consumerFuture, SubscriptionMode.NonDurable, readerConfiguration.getStartMessageId(), schema, null,
                client.getConfiguration().getDefaultBackoffIntervalNanos(), client.getConfiguration().getMaxBackoffIntervalNanos());
        startPublishTime = readerConfiguration.getStartPublishTime();
        if (startPublishTime == null) {
            seekDone = true;
        } else {
            seekDone = false;
        }
    }

    @Override
    public String getTopic() {
        return consumer.getTopic();
    }

    public ConsumerImpl<T> getConsumer() {
        return consumer;
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumer.hasReachedEndOfTopic();
    }

    private void seekIfNeeded() {
        try {
            seekIfNeededAsync().get();
        } catch (InterruptedException | ExecutionException exc) {
            log.warn(exc.getMessage());
        }
    }

    private CompletableFuture<Void> seekIfNeededAsync() {
        final CompletableFuture<Void> future;
        if (!seekDone) {
            seekDone = true;
            future = seekAsync(startPublishTime);
        } else {
            future = new CompletableFuture<>();
            future.complete(null);
        }
        return future;
    }

    @Override
    public Message<T> readNext() throws PulsarClientException {
        seekIfNeeded();
        Message<T> msg = consumer.receive();

        // Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
        // it will specify the subscription position anyway
        consumer.acknowledgeCumulativeAsync(msg);
        return msg;
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        seekIfNeeded();
        Message<T> msg = consumer.receive(timeout, unit);

        if (msg != null) {
            consumer.acknowledgeCumulativeAsync(msg);
        }
        return msg;
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return seekIfNeededAsync().thenApply(argument -> {
            try {
                return consumer.receiveAsync().thenApply(msg -> {
                    consumer.acknowledgeCumulativeAsync(msg);
                    return msg;
                }).get();
            } catch (InterruptedException | ExecutionException e) {
                log.warn(e.getMessage());
            }
            return null;
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

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return consumer.hasMessageAvailable();
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return consumer.hasMessageAvailableAsync();
    }

    @Override
    public boolean isConnected() {
        return consumer.isConnected();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        consumer.seek(messageId);
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        consumer.seek(timestamp);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return consumer.seekAsync(messageId);
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return consumer.seekAsync(timestamp);
    }
}
