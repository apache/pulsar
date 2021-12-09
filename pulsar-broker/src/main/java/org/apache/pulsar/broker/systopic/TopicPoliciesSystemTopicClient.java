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
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System topic for topic policy.
 */
public class TopicPoliciesSystemTopicClient extends SystemTopicClientBase<PulsarEvent> {

    public TopicPoliciesSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected  CompletableFuture<Writer<PulsarEvent>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(PulsarEvent.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new TopicPolicyWriter(producer,
                            TopicPoliciesSystemTopicClient.this));
                });
    }

    @Override
    protected CompletableFuture<Reader<PulsarEvent>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(PulsarEvent.class))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true).createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new TopicPolicyReader(reader,
                            TopicPoliciesSystemTopicClient.this));
                });
    }

    private static class TopicPolicyWriter implements Writer<PulsarEvent> {

        private final Producer<PulsarEvent> producer;
        private final SystemTopicClient<PulsarEvent> systemTopicClient;

        private TopicPolicyWriter(Producer<PulsarEvent> producer, SystemTopicClient<PulsarEvent> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(PulsarEvent event) throws PulsarClientException {
            TypedMessageBuilder<PulsarEvent> builder = producer.newMessage().key(getEventKey(event)).value(event);
            setReplicateCluster(event, builder);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(PulsarEvent event) {
            TypedMessageBuilder<PulsarEvent> builder = producer.newMessage().key(getEventKey(event)).value(event);
            setReplicateCluster(event, builder);
            return builder.sendAsync();
        }

        @Override
        public MessageId delete(PulsarEvent event) throws PulsarClientException {
            validateActionType(event);
            TypedMessageBuilder<PulsarEvent> builder = producer.newMessage().key(getEventKey(event)).value(null);
            setReplicateCluster(event, builder);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(PulsarEvent event) {
            validateActionType(event);
            TypedMessageBuilder<PulsarEvent> builder = producer.newMessage().key(getEventKey(event)).value(null);
            setReplicateCluster(event, builder);
            return builder.sendAsync();
        }

        private String getEventKey(PulsarEvent event) {
            return TopicName.get(event.getTopicPoliciesEvent().getDomain(),
                event.getTopicPoliciesEvent().getTenant(),
                event.getTopicPoliciesEvent().getNamespace(),
                event.getTopicPoliciesEvent().getTopic()).toString();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(TopicPolicyWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(TopicPolicyWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<PulsarEvent> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    private static void setReplicateCluster(PulsarEvent event, TypedMessageBuilder<PulsarEvent> builder) {
        if (event.getReplicateTo() != null) {
            builder.replicationClusters(new ArrayList<>(event.getReplicateTo()));
        }
    }

    private static void validateActionType(PulsarEvent event) {
        if (event == null || !ActionType.DELETE.equals(event.getActionType())) {
            throw new UnsupportedOperationException("The only supported ActionType is DELETE");
        }
    }

    private static class TopicPolicyReader implements Reader<PulsarEvent> {

        private final org.apache.pulsar.client.api.Reader<PulsarEvent> reader;
        private final TopicPoliciesSystemTopicClient systemTopic;

        private TopicPolicyReader(org.apache.pulsar.client.api.Reader<PulsarEvent> reader,
                                  TopicPoliciesSystemTopicClient systemTopic) {
            this.reader = reader;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<PulsarEvent> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<PulsarEvent>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
            systemTopic.getReaders().remove(TopicPolicyReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(TopicPolicyReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<PulsarEvent> getSystemTopic() {
            return systemTopic;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TopicPoliciesSystemTopicClient.class);
}
