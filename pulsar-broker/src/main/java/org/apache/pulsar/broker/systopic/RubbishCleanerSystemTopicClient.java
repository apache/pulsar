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
import org.apache.bookkeeper.mledger.rubbish.RubbishInfo;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System topic for rubbish cleaner.
 */
public class RubbishCleanerSystemTopicClient extends SystemTopicClientBase<RubbishInfo> {

    public RubbishCleanerSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected CompletableFuture<Writer<RubbishInfo>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(RubbishInfo.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new RubbishInfoWriter(producer,
                            RubbishCleanerSystemTopicClient.this));
                });
    }

    @Override
    protected CompletableFuture<Reader<RubbishInfo>> newReaderAsyncInternal() {
        return client.newConsumer(Schema.AVRO(RubbishInfo.class))
                .topic(topicName.toString())
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .deadLetterTopic(SystemTopicNames.RUBBISH_CLEANER_ARCHIVE_TOPIC.getPartitionedTopicName())
                        .maxRedeliverCount(10).build())
                .subscribeAsync()
                .thenCompose(consumer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new RubbishInfoReader(consumer,
                            RubbishCleanerSystemTopicClient.this));
                });
    }

    public static class RubbishInfoWriter implements Writer<RubbishInfo> {

        private final Producer<RubbishInfo> producer;
        private final SystemTopicClient<RubbishInfo> systemTopicClient;

        private RubbishInfoWriter(Producer<RubbishInfo> producer, SystemTopicClient<RubbishInfo> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(RubbishInfo rubbishInfo) throws PulsarClientException {
            TypedMessageBuilder<RubbishInfo> builder =
                    producer.newMessage().value(rubbishInfo).deliverAfter(5, TimeUnit.MINUTES);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(RubbishInfo rubbishInfo) {
            TypedMessageBuilder<RubbishInfo> builder =
                    producer.newMessage().value(rubbishInfo).deliverAfter(5, TimeUnit.MINUTES);
            return builder.sendAsync();
        }


        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(RubbishInfoWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(RubbishInfoWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<RubbishInfo> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    public static class RubbishInfoReader implements Reader<RubbishInfo> {

        private final Consumer<RubbishInfo> consumer;
        private final RubbishCleanerSystemTopicClient systemTopic;
        private Message<RubbishInfo> lastMessage;

        private RubbishInfoReader(Consumer<RubbishInfo> consumer,
                                  RubbishCleanerSystemTopicClient systemTopic) {
            this.consumer = consumer;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<RubbishInfo> readNext() throws PulsarClientException {
            return consumer.receive();
        }

        private void updateLastMessage(Message<RubbishInfo> message) {
            if (lastMessage == null) {
                lastMessage = message;
                return;
            }
            if (message.getMessageId().compareTo(lastMessage.getMessageId()) > 0) {
                lastMessage = message;
            }
        }

        @Override
        public CompletableFuture<Message<RubbishInfo>> readNextAsync() {
            return consumer.receiveAsync().whenComplete((res, e) -> {
                if (e == null) {
                    updateLastMessage(res);
                }
            });
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            if (consumer instanceof ConsumerImpl<RubbishInfo>) {
                return ((ConsumerImpl<RubbishInfo>) consumer).hasMessageAvailable();
            } else if (consumer instanceof MultiTopicsConsumerImpl<RubbishInfo>) {
                return ((MultiTopicsConsumerImpl<RubbishInfo>) consumer).hasMessageAvailable();
            }
            throw new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents.");
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            if (consumer instanceof ConsumerImpl<RubbishInfo>) {
                return ((ConsumerImpl<RubbishInfo>) consumer).hasMessageAvailableAsync();
            } else if (consumer instanceof MultiTopicsConsumerImpl<RubbishInfo>) {
                return ((MultiTopicsConsumerImpl<RubbishInfo>) consumer).hasMessageAvailableAsync();
            }
            return FutureUtil.failedFuture(
                    new PulsarClientException.NotSupportedException("The consumer not support hasMoreEvents."));
        }


        @Override
        public void close() throws IOException {
            this.consumer.close();
            systemTopic.getReaders().remove(RubbishInfoReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return consumer.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(RubbishInfoReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletableFuture<Void> ackMessageAsync(Message<RubbishInfo> message) {
            return this.consumer.acknowledgeAsync(message);
        }

        public CompletableFuture<Void> reconsumeLaterAsync(Message<RubbishInfo> message) {
            return this.consumer.reconsumeLaterAsync(message, 10, TimeUnit.MINUTES);
        }

        @Override
        public SystemTopicClient<RubbishInfo> getSystemTopic() {
            return systemTopic;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RubbishCleanerSystemTopicClient.class);
}
