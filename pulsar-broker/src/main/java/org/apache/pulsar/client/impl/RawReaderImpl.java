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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawReaderImpl implements RawReader {

    static final int DEFAULT_RECEIVER_QUEUE_SIZE = 1000;
    private final ConsumerConfigurationData<byte[]> consumerConfiguration;
    private RawConsumerImpl consumer;

    public RawReaderImpl(PulsarClientImpl client, String topic, String subscription,
                         CompletableFuture<Consumer<byte[]>> consumerFuture) {
        consumerConfiguration = new ConsumerConfigurationData<>();
        consumerConfiguration.getTopicNames().add(topic);
        consumerConfiguration.setSubscriptionName(subscription);
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setReceiverQueueSize(DEFAULT_RECEIVER_QUEUE_SIZE);
        consumerConfiguration.setReadCompacted(true);

        consumer = new RawConsumerImpl(client, consumerConfiguration,
                                       consumerFuture);
    }

    @Override
    public String getTopic() {
        return consumerConfiguration.getTopicNames().stream()
            .findFirst().orElse(null);
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return consumer.hasMessageAvailableAsync();
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return consumer.seekAsync(messageId);
    }

    @Override
    public CompletableFuture<RawMessage> readNextAsync() {
        return consumer.receiveRawAsync();
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Map<String, Long> properties) {
        return consumer.doAcknowledgeWithTxn(messageId, AckType.Cumulative, properties, null);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return consumer.closeAsync();
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return consumer.getLastMessageIdAsync();
    }

    @Override
    public String toString() {
        return "RawReader(topic=" + getTopic() + ")";
    }

    static class RawConsumerImpl extends ConsumerImpl<byte[]> {
        final BlockingQueue<RawMessageAndCnx> incomingRawMessages;
        final Queue<CompletableFuture<RawMessage>> pendingRawReceives;

        RawConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<byte[]> conf,
                CompletableFuture<Consumer<byte[]>> consumerFuture) {
            super(client,
                    conf.getSingleTopic(),
                    conf,
                    client.externalExecutorProvider(),
                    TopicName.getPartitionIndex(conf.getSingleTopic()),
                    false,
                    consumerFuture,
                    MessageId.earliest,
                    0 /* startMessageRollbackDurationInSec */,
                    Schema.BYTES, null,
                    true
            );
            incomingRawMessages = new GrowableArrayBlockingQueue<>();
            pendingRawReceives = new ConcurrentLinkedQueue<>();
        }

        void tryCompletePending() {
            CompletableFuture<RawMessage> future = null;
            RawMessageAndCnx messageAndCnx = null;

            synchronized (this) {
                if (!pendingRawReceives.isEmpty()
                    && !incomingRawMessages.isEmpty()) {
                    future = pendingRawReceives.remove();
                    messageAndCnx = incomingRawMessages.remove();
                }
            }
            if (future == null) {
                assert(messageAndCnx == null);
            } else {
                int numMsg;
                try {
                    MessageMetadata msgMetadata =
                            Commands.parseMessageMetadata(messageAndCnx.msg.getHeadersAndPayload());
                    numMsg = msgMetadata.getNumMessagesInBatch();
                } catch (Throwable t) {
                    // TODO message validation
                    numMsg = 1;
                }
                if (!future.complete(messageAndCnx.msg)) {
                    messageAndCnx.msg.close();
                    closeAsync();
                }

                ClientCnx currentCnx = cnx();
                if (currentCnx == messageAndCnx.cnx) {
                    increaseAvailablePermits(currentCnx, numMsg);
                }
            }
        }

        CompletableFuture<RawMessage> receiveRawAsync() {
            CompletableFuture<RawMessage> result = new CompletableFuture<>();
            pendingRawReceives.add(result);
            tryCompletePending();
            return result;
        }

        private void reset() {
            List<CompletableFuture<RawMessage>> toError = new ArrayList<>();
            synchronized (this) {
                while (!pendingRawReceives.isEmpty()) {
                    toError.add(pendingRawReceives.remove());
                }
                RawMessageAndCnx m = incomingRawMessages.poll();
                while (m != null) {
                    m.msg.close();
                    m = incomingRawMessages.poll();
                }
                incomingRawMessages.clear();
            }
            toError.forEach((f) -> f.cancel(false));
        }

        @Override
        public CompletableFuture<Void> seekAsync(long timestamp) {
            reset();
            return super.seekAsync(timestamp);
        }

        @Override
        public CompletableFuture<Void> seekAsync(MessageId messageId) {
            reset();
            return super.seekAsync(messageId);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            reset();
            return super.closeAsync();
        }

        @Override
        void messageReceived(MessageIdData messageId, int redeliveryCount,
                             List<Long> ackSet, ByteBuf headersAndPayload, ClientCnx cnx) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Received raw message: {}/{}/{}", topic, subscription,
                        messageId.getEntryId(), messageId.getLedgerId(), messageId.getPartition());
            }
            incomingRawMessages.add(
                    new RawMessageAndCnx(new RawMessageImpl(messageId, headersAndPayload), cnx));
            tryCompletePending();
        }
    }

    private static class RawMessageAndCnx {
        final RawMessage msg;
        final ClientCnx cnx;

        RawMessageAndCnx(RawMessage msg, ClientCnx cnx) {
            this.msg = msg;
            this.cnx = cnx;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RawReaderImpl.class);
}
