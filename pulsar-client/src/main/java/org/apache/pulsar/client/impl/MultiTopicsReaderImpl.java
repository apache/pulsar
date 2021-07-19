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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;

public class MultiTopicsReaderImpl<T> implements Reader<T> {

    private final MultiTopicsConsumerImpl<T> multiTopicsConsumer;

    public MultiTopicsReaderImpl(PulsarClientImpl client, ReaderConfigurationData<T> readerConfiguration,
                                 ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> consumerFuture, Schema<T> schema) {
        String subscription = "multiTopicsReader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);
        if (StringUtils.isNotBlank(readerConfiguration.getSubscriptionRolePrefix())) {
            subscription = readerConfiguration.getSubscriptionRolePrefix() + "-" + subscription;
        }
        if (StringUtils.isNotBlank(readerConfiguration.getSubscriptionName())) {
            subscription = readerConfiguration.getSubscriptionName();
        }
        ConsumerConfigurationData<T> consumerConfiguration = new ConsumerConfigurationData<>();
        consumerConfiguration.getTopicNames().addAll(readerConfiguration.getTopicNames());
        consumerConfiguration.setSubscriptionName(subscription);
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfiguration.setReceiverQueueSize(readerConfiguration.getReceiverQueueSize());
        consumerConfiguration.setReadCompacted(readerConfiguration.isReadCompacted());

        if (readerConfiguration.getReaderListener() != null) {
            ReaderListener<T> readerListener = readerConfiguration.getReaderListener();
            consumerConfiguration.setMessageListener(new MessageListener<T>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void received(Consumer<T> consumer, Message<T> msg) {
                    readerListener.received(MultiTopicsReaderImpl.this, msg);
                    consumer.acknowledgeCumulativeAsync(msg);
                }

                @Override
                public void reachedEndOfTopic(Consumer<T> consumer) {
                    readerListener.reachedEndOfTopic(MultiTopicsReaderImpl.this);
                }
            });
        }

        if (readerConfiguration.getReaderName() != null) {
            consumerConfiguration.setConsumerName(readerConfiguration.getReaderName());
        }
        if (readerConfiguration.isResetIncludeHead()) {
            consumerConfiguration.setResetIncludeHead(true);
        }
        consumerConfiguration.setCryptoFailureAction(readerConfiguration.getCryptoFailureAction());
        if (readerConfiguration.getCryptoKeyReader() != null) {
            consumerConfiguration.setCryptoKeyReader(readerConfiguration.getCryptoKeyReader());
        }
        if (readerConfiguration.getKeyHashRanges() != null) {
            consumerConfiguration.setKeySharedPolicy(
                    KeySharedPolicy
                            .stickyHashRange()
                            .ranges(readerConfiguration.getKeyHashRanges())
            );
        }
        multiTopicsConsumer = new MultiTopicsConsumerImpl<>(client, consumerConfiguration, executorProvider, consumerFuture, schema,
                null, true, readerConfiguration.getStartMessageId(), readerConfiguration.getStartMessageFromRollbackDurationInSec());
    }

    @Override
    public String getTopic() {
        return multiTopicsConsumer.getTopic();
    }

    @Override
    public Message<T> readNext() throws PulsarClientException {
        Message<T> msg = multiTopicsConsumer.receive();
        multiTopicsConsumer.tryAcknowledgeMessage(msg);
        return msg;
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> msg = multiTopicsConsumer.receive(timeout, unit);
        multiTopicsConsumer.tryAcknowledgeMessage(msg);
        return msg;
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return multiTopicsConsumer.receiveAsync().thenApply(msg -> {
            multiTopicsConsumer.acknowledgeCumulativeAsync(msg);
            return msg;
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return multiTopicsConsumer.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return multiTopicsConsumer.hasReachedEndOfTopic();
    }

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return multiTopicsConsumer.hasMessageAvailable() || multiTopicsConsumer.numMessagesInQueue() > 0;
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return multiTopicsConsumer.hasMessageAvailableAsync();
    }

    @Override
    public boolean isConnected() {
        return multiTopicsConsumer.isConnected();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        multiTopicsConsumer.seek(messageId);
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        multiTopicsConsumer.seek(timestamp);
    }

    @Override
    public void seek(Function<String, Object> function) throws PulsarClientException {
        multiTopicsConsumer.seek(function);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return multiTopicsConsumer.seekAsync(messageId);
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return multiTopicsConsumer.seekAsync(timestamp);
    }

    @Override
    public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
        return multiTopicsConsumer.seekAsync(function);
    }

    @Override
    public void close() throws IOException {
        multiTopicsConsumer.close();
    }

    public MultiTopicsConsumerImpl<T> getMultiTopicsConsumer() {
        return multiTopicsConsumer;
    }
}
