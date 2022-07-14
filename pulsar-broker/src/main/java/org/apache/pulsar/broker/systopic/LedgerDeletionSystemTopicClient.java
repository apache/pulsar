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
package org.apache.pulsar.broker.systopic;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.deletion.PendingDeleteLedgerInfo;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System topic for ledger deletion.
 */
public class LedgerDeletionSystemTopicClient extends SystemTopicClientBase<PendingDeleteLedgerInfo> {

    private final int sendDelaySeconds;

    private final int reconsumeLaterSeconds;

    public LedgerDeletionSystemTopicClient(PulsarClient client, TopicName topicName, int sendDelaySeconds,
                                           int reconsumeLaterSeconds) {
        super(client, topicName);
        this.sendDelaySeconds = sendDelaySeconds;
        this.reconsumeLaterSeconds = reconsumeLaterSeconds;
    }

    @Override
    protected CompletableFuture<Writer<PendingDeleteLedgerInfo>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(PendingDeleteLedgerInfo.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new PendingDeleteLedgerWriter(producer, LedgerDeletionSystemTopicClient.this,
                                    sendDelaySeconds));
                });
    }

    @Override
    protected CompletableFuture<Reader<PendingDeleteLedgerInfo>> newReaderAsyncInternal() {
        return client.newConsumer(Schema.AVRO(PendingDeleteLedgerInfo.class))
                .topic(topicName.toString())
                .subscriptionName("ledger-deletion-worker")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .deadLetterTopic(SystemTopicNames.LEDGER_DELETION_ARCHIVE_TOPIC.getPartitionedTopicName())
                        .maxRedeliverCount(10).build())
                .subscribeAsync()
                .thenCompose(consumer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new PendingDeleteLedgerReader(consumer, LedgerDeletionSystemTopicClient.this,
                                    reconsumeLaterSeconds));
                });
    }

    public static class PendingDeleteLedgerWriter implements Writer<PendingDeleteLedgerInfo> {

        private final Producer<PendingDeleteLedgerInfo> producer;
        private final SystemTopicClient<PendingDeleteLedgerInfo> systemTopicClient;
        private final int sendDelaySeconds;

        private PendingDeleteLedgerWriter(Producer<PendingDeleteLedgerInfo> producer,
                                          SystemTopicClient<PendingDeleteLedgerInfo> systemTopicClient,
                                          int sendDelaySeconds) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
            this.sendDelaySeconds = sendDelaySeconds;
        }

        @Override
        public MessageId write(PendingDeleteLedgerInfo pendingDeleteLedgerInfo) throws PulsarClientException {
            TypedMessageBuilder<PendingDeleteLedgerInfo> builder =
                    producer.newMessage().value(pendingDeleteLedgerInfo)
                            .deliverAfter(sendDelaySeconds, TimeUnit.SECONDS);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(PendingDeleteLedgerInfo pendingDeleteLedgerInfo) {
            TypedMessageBuilder<PendingDeleteLedgerInfo> builder =
                    producer.newMessage().value(pendingDeleteLedgerInfo).deliverAfter(1, TimeUnit.MINUTES);
            return builder.sendAsync();
        }


        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(PendingDeleteLedgerWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(PendingDeleteLedgerWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<PendingDeleteLedgerInfo> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    public static class PendingDeleteLedgerReader implements Reader<PendingDeleteLedgerInfo> {

        private final Consumer<PendingDeleteLedgerInfo> consumer;
        private final LedgerDeletionSystemTopicClient systemTopic;
        private final int reconsumeLaterSeconds;

        private PendingDeleteLedgerReader(Consumer<PendingDeleteLedgerInfo> consumer,
                                          LedgerDeletionSystemTopicClient systemTopic,
                                          int reconsumeLaterSeconds) {
            this.consumer = consumer;
            this.systemTopic = systemTopic;
            this.reconsumeLaterSeconds = reconsumeLaterSeconds;
        }

        @Override
        public Message<PendingDeleteLedgerInfo> readNext() throws PulsarClientException {
            return consumer.receive();
        }

        @Override
        public CompletableFuture<Message<PendingDeleteLedgerInfo>> readNextAsync() {
            return consumer.receiveAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            if (consumer instanceof ConsumerImpl<PendingDeleteLedgerInfo>) {
                return ((ConsumerImpl<PendingDeleteLedgerInfo>) consumer).hasMessageAvailable();
            } else if (consumer instanceof MultiTopicsConsumerImpl<PendingDeleteLedgerInfo>) {
                return ((MultiTopicsConsumerImpl<PendingDeleteLedgerInfo>) consumer).hasMessageAvailable();
            }
            throw new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents.");
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            if (consumer instanceof ConsumerImpl<PendingDeleteLedgerInfo>) {
                return ((ConsumerImpl<PendingDeleteLedgerInfo>) consumer).hasMessageAvailableAsync();
            } else if (consumer instanceof MultiTopicsConsumerImpl<PendingDeleteLedgerInfo>) {
                return ((MultiTopicsConsumerImpl<PendingDeleteLedgerInfo>) consumer).hasMessageAvailableAsync();
            }
            return FutureUtil.failedFuture(
                    new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents."));
        }


        @Override
        public void close() throws IOException {
            this.consumer.close();
            systemTopic.getReaders().remove(PendingDeleteLedgerReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return consumer.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(PendingDeleteLedgerReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletableFuture<Void> ackMessageAsync(Message<PendingDeleteLedgerInfo> message) {
            return this.consumer.acknowledgeAsync(message);
        }

        public CompletableFuture<Void> reconsumeLaterAsync(Message<PendingDeleteLedgerInfo> message) {
            return this.consumer.reconsumeLaterAsync(message, reconsumeLaterSeconds, TimeUnit.SECONDS);
        }

        @Override
        public SystemTopicClient<PendingDeleteLedgerInfo> getSystemTopic() {
            return systemTopic;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerDeletionSystemTopicClient.class);
}
