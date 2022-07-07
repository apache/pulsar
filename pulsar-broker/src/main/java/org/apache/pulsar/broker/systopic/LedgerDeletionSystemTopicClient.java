/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.bookkeeper.mledger.deletion.RubbishLedger;
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
public class LedgerDeletionSystemTopicClient extends SystemTopicClientBase<RubbishLedger> {

    public LedgerDeletionSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected CompletableFuture<Writer<RubbishLedger>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(RubbishLedger.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new RubbishLedgerWriter(producer,
                            LedgerDeletionSystemTopicClient.this));
                });
    }

    @Override
    protected CompletableFuture<Reader<RubbishLedger>> newReaderAsyncInternal() {
        return client.newConsumer(Schema.AVRO(RubbishLedger.class))
                .topic(topicName.toString())
                .subscriptionName("ledger-deletion-worker")
                .subscriptionType(SubscriptionType.Shared)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .deadLetterTopic(SystemTopicNames.LEDGER_DELETION_ARCHIVE_TOPIC.getPartitionedTopicName())
                        .maxRedeliverCount(10).build())
                .subscribeAsync()
                .thenCompose(consumer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new RubbishLedgerReader(consumer,
                            LedgerDeletionSystemTopicClient.this));
                });
    }

    public static class RubbishLedgerWriter implements Writer<RubbishLedger> {

        private final Producer<RubbishLedger> producer;
        private final SystemTopicClient<RubbishLedger> systemTopicClient;
        private final int sendDelayMinutes = 1;

        private RubbishLedgerWriter(Producer<RubbishLedger> producer, SystemTopicClient<RubbishLedger> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(RubbishLedger rubbishLedger) throws PulsarClientException {
            TypedMessageBuilder<RubbishLedger> builder =
                    producer.newMessage().value(rubbishLedger).deliverAfter(sendDelayMinutes, TimeUnit.MINUTES);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(RubbishLedger rubbishLedger) {
            TypedMessageBuilder<RubbishLedger> builder =
                    producer.newMessage().value(rubbishLedger).deliverAfter(1, TimeUnit.MINUTES);
            return builder.sendAsync();
        }


        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(RubbishLedgerWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(RubbishLedgerWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<RubbishLedger> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    public static class RubbishLedgerReader implements Reader<RubbishLedger> {

        private final Consumer<RubbishLedger> consumer;
        private final LedgerDeletionSystemTopicClient systemTopic;
        private final int reconsumeLaterMin = 10;

        private RubbishLedgerReader(Consumer<RubbishLedger> consumer,
                                    LedgerDeletionSystemTopicClient systemTopic) {
            this.consumer = consumer;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<RubbishLedger> readNext() throws PulsarClientException {
            return consumer.receive();
        }

        @Override
        public CompletableFuture<Message<RubbishLedger>> readNextAsync() {
            return consumer.receiveAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            if (consumer instanceof ConsumerImpl<RubbishLedger>) {
                return ((ConsumerImpl<RubbishLedger>) consumer).hasMessageAvailable();
            } else if (consumer instanceof MultiTopicsConsumerImpl<RubbishLedger>) {
                return ((MultiTopicsConsumerImpl<RubbishLedger>) consumer).hasMessageAvailable();
            }
            throw new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents.");
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            if (consumer instanceof ConsumerImpl<RubbishLedger>) {
                return ((ConsumerImpl<RubbishLedger>) consumer).hasMessageAvailableAsync();
            } else if (consumer instanceof MultiTopicsConsumerImpl<RubbishLedger>) {
                return ((MultiTopicsConsumerImpl<RubbishLedger>) consumer).hasMessageAvailableAsync();
            }
            return FutureUtil.failedFuture(
                    new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents."));
        }


        @Override
        public void close() throws IOException {
            this.consumer.close();
            systemTopic.getReaders().remove(RubbishLedgerReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return consumer.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(RubbishLedgerReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletableFuture<Void> ackMessageAsync(Message<RubbishLedger> message) {
            return this.consumer.acknowledgeAsync(message);
        }

        public CompletableFuture<Void> reconsumeLaterAsync(Message<RubbishLedger> message) {
            return this.consumer.reconsumeLaterAsync(message, reconsumeLaterMin, TimeUnit.MINUTES);
        }

        @Override
        public SystemTopicClient<RubbishLedger> getSystemTopic() {
            return systemTopic;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerDeletionSystemTopicClient.class);
}
