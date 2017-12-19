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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ConsumerImpl.SubscriptionMode;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawReaderImpl implements RawReader {

    final static int DEFAULT_RECEIVER_QUEUE_SIZE = 1000;
    private final PulsarClientImpl client;
    private final String topic;
    private final String subscription;
    private final ConsumerConfiguration consumerConfiguration;
    private RawConsumerImpl consumer;

    public RawReaderImpl(PulsarClientImpl client, String topic, CompletableFuture<Consumer> consumerFuture) {
        this.client = client;
        this.topic = topic;

        subscription = "raw-reader";

        consumerConfiguration = new ConsumerConfiguration();
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setReceiverQueueSize(DEFAULT_RECEIVER_QUEUE_SIZE);

        consumer = new RawConsumerImpl(client, topic, subscription, consumerConfiguration,
                                       consumerFuture);
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
    public CompletableFuture<Void> closeAsync() {
        return consumer.closeAsync();
    }

    static class RawConsumerImpl extends ConsumerImpl {
        final BlockingQueue<RawMessageAndCnx> incomingRawMessages;
        final Queue<CompletableFuture<RawMessage>> pendingRawReceives;

        RawConsumerImpl(PulsarClientImpl client, String topic, String subscription, ConsumerConfiguration conf,
                        CompletableFuture<Consumer> consumerFuture) {
            super(client, topic, subscription, conf,
                  client.externalExecutorProvider().getExecutor(), -1, consumerFuture,
                  SubscriptionMode.Durable, MessageId.earliest);
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
                future.complete(messageAndCnx.msg);

                ClientCnx currentCnx = cnx();
                if (currentCnx == messageAndCnx.cnx) {
                    increaseAvailablePermits(currentCnx);
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
                incomingRawMessages.clear();
            }
            toError.forEach((f) -> f.completeExceptionally(
                                    new PulsarClientException.ConsumerBusyException("Sought while reading")));
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
        void messageReceived(MessageIdData messageId, ByteBuf headersAndPayload, ClientCnx cnx) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Received raw message: {}/{}", topic, subscription,
                          messageId.getLedgerId(), messageId.getEntryId());
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
