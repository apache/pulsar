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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck;

/**
 *  An extended ReaderImpl used for StrategicTwoPhaseCompactor.
 *  The compaction consumer subscription is durable and consumes compacted messages from the earliest position.
 *  It does not acknowledge the message after each read. (needs to call acknowledgeCumulativeAsync to ack messages.)
 */
@Slf4j
public class CompactionReaderImpl<T> extends ReaderImpl<T> {

    ConsumerBase<T> consumer;

    ReaderConfigurationData<T> readerConfiguration;
    private CompactionReaderImpl(PulsarClientImpl client, ReaderConfigurationData<T> readerConfiguration,
                                 ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> consumerFuture,
                                 Schema<T> schema) {
        super(client, readerConfiguration, executorProvider, consumerFuture, schema);
        this.readerConfiguration = readerConfiguration;
        this.consumer = getConsumer();
    }

    public static <T> CompactionReaderImpl<T> create(PulsarClientImpl client, Schema<T> schema, String topic,
                                                     CompletableFuture<Consumer<T>> consumerFuture,
                                                     CryptoKeyReader cryptoKeyReader) {
        ReaderConfigurationData<T> conf = new ReaderConfigurationData<>();
        conf.setTopicName(topic);
        conf.setSubscriptionName(COMPACTION_SUBSCRIPTION);
        conf.setStartMessageId(MessageId.earliest);
        conf.setStartMessageFromRollbackDurationInSec(0);
        conf.setReadCompacted(true);
        conf.setSubscriptionMode(SubscriptionMode.Durable);
        conf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        conf.setCryptoKeyReader(cryptoKeyReader);
        return new CompactionReaderImpl<>(client, conf, client.externalExecutorProvider(), consumerFuture, schema);
    }


    @Override
    public Message<T> readNext() throws PulsarClientException {
        return consumer.receive();
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        return consumer.receive(timeout, unit);
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return consumer.receiveAsync();
    }

    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return consumer.getLastMessageIdAsync();
    }

    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Map<String, Long> properties) {
        return consumer.doAcknowledge(messageId, CommandAck.AckType.Cumulative, properties, null);
    }
}
