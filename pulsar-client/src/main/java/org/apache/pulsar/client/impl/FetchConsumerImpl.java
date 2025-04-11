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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.FetchConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ProtocolVersion;

@Slf4j
public class FetchConsumerImpl<T> extends ConsumerImpl<T> implements FetchConsumer<T> {

    private final AtomicReference<CompletableFuture<Messages<T>>> lastFutureRef =
            new AtomicReference<>(CompletableFuture.completedFuture(null));

    protected FetchConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                                ExecutorProvider executorProvider, int partitionIndex,
                                boolean hasParentConsumer,
                                CompletableFuture<Consumer<T>> subscribeFuture,
                                MessageId startMessageId, long startMessageRollbackDurationInSec,
                                Schema<T> schema, ConsumerInterceptors<T> interceptors,
                                boolean createTopicIfDoesNotExist) {
        super(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer, false,
                subscribeFuture, startMessageId, startMessageRollbackDurationInSec, schema, interceptors,
                createTopicIfDoesNotExist);
        verifyConfig();
    }

    static <T> FetchConsumerImpl<T> create(PulsarClientImpl client, String topic,
                                      ConsumerConfigurationData<T> conf,
                                      ExecutorProvider executorProvider, int partitionIndex,
                                      boolean hasParentConsumer,
                                      CompletableFuture<Consumer<T>> subscribeFuture,
                                      MessageId startMessageId, long startMessageRollbackDurationInSec,
                                      Schema<T> schema, ConsumerInterceptors<T> interceptors,
                                      boolean createTopicIfDoesNotExist) {
        return new FetchConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex,
                hasParentConsumer, subscribeFuture, startMessageId,
                startMessageRollbackDurationInSec, schema, interceptors, createTopicIfDoesNotExist);
    }

    @Override
    public Messages<T> fetchMessages(int maxMessages, int maxBytes, MessageId messageId,
                                     int timeout, TimeUnit unit) throws PulsarClientException {
        try {
            return fetchMessagesAsync(maxMessages, maxBytes, messageId, timeout, unit).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new PulsarClientException("Failed to fetch messages", e);
        }
    }

    @Override
    public CompletableFuture<Messages<T>> fetchMessagesAsync(int maxMessages, int maxBytes, MessageId messageId,
                                                             int timeout, TimeUnit unit) {
        // Link the new task to the end and execute it in series
        return lastFutureRef.getAndUpdate(currentFuture ->
                currentFuture.thenCompose(ignored ->
                        internalFetchMessagesAsync(maxMessages, maxBytes, messageId, timeout, unit))
        );
    }

    private CompletableFuture<Messages<T>> internalFetchMessagesAsync(int maxMessages, int maxBytes,
                                                                      MessageId messageId,
                                                                      int timeout, TimeUnit unit) {
        CompletableFuture<Void> seekFuture = new CompletableFuture<>();
        MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        if (cnx().getRemoteEndpointProtocolVersion() < ProtocolVersion.V22.getValue()
                && (messageIdAdv.getLedgerId() == -1L || messageIdAdv.getEntryId() == -1L)) {
            seekFuture.completeExceptionally(
                    new PulsarClientException("Fetch messages by offset is not supported on this broker version, "
                            + "please update broker version or use message id with ledgerId and entryId"));
        }
        // TODO：this logic can be improve to check where to seek.
        if (((MessageIdAdv) super.lastDequeuedMessageId).getOffset() != ((MessageIdAdv) messageId).getOffset() - 1) {
            super.seekAsync(messageId).whenComplete((v, ex) -> {
                if (ex != null) {
                    seekFuture.completeExceptionally(ex);
                } else {
                    seekFuture.complete(null);
                }
            });
        } else {
            seekFuture.complete(null);
        }

        return seekFuture.thenCompose(v -> {
            super.conf.setBatchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(maxMessages)
                    .maxNumBytes(maxBytes).timeout(timeout, unit).build());
            //todo: 不立刻返回，如果最后的offset不满一个batch则剔除
            return super.batchReceiveAsync();
        });
    }
    private void verifyConfig() {
        if (conf.getSubscriptionType() != SubscriptionType.Failover
                && conf.getSubscriptionType() != SubscriptionType.Exclusive) {
            throw new IllegalArgumentException("FetchConsumer can only be used with SubscriptionType.Fetch");
        }
        if (conf.getMessageListener() != null) {
            throw new IllegalArgumentException("FetchConsumer cannot have a message listener");
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        return super.equals(object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lastFutureRef);
    }
}
