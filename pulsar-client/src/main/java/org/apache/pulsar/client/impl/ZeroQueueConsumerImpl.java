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

import static java.lang.String.format;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Slf4j
public class ZeroQueueConsumerImpl<T> extends ConsumerImpl<T> {

    private final Lock zeroQueueLock = new ReentrantLock();

    private volatile boolean waitingOnReceiveForZeroQueueSize = false;
    private volatile boolean waitingOnListenerForZeroQueueSize = false;

    public ZeroQueueConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
             ExecutorProvider executorProvider, int partitionIndex, boolean hasParentConsumer,
             CompletableFuture<Consumer<T>> subscribeFuture, MessageId startMessageId, Schema<T> schema,
             ConsumerInterceptors<T> interceptors,
             boolean createTopicIfDoesNotExist) {
        super(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer, false, subscribeFuture,
                startMessageId, 0 /* startMessageRollbackDurationInSec */, schema, interceptors,
                createTopicIfDoesNotExist);
    }

    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        zeroQueueLock.lock();
        try {
            Message<T> msg = fetchSingleMessageFromBroker();
            trackMessage(msg);
            return beforeConsume(msg);
        } finally {
            zeroQueueLock.unlock();
        }
    }

    @Override
    protected CompletableFuture<Message<T>> internalReceiveAsync() {
        CompletableFuture<Message<T>> future = super.internalReceiveAsync();
        if (!future.isDone()) {
            // We expect the message to be not in the queue yet
            increaseAvailablePermits(cnx());
        }

        return future;
    }

    private Message<T> fetchSingleMessageFromBroker() throws PulsarClientException {
        // Just being cautious
        if (incomingMessages.size() > 0) {
            log.error("The incoming message queue should never be greater than 0 when Queue size is 0");
            incomingMessages.forEach(Message::release);
            incomingMessages.clear();
        }

        Message<T> message;
        try {
            // if cnx is null or if the connection breaks the connectionOpened function will send the flow again
            waitingOnReceiveForZeroQueueSize = true;
            synchronized (this) {
                if (isConnected()) {
                    increaseAvailablePermits(cnx());
                }
            }
            do {
                message = incomingMessages.take();
                lastDequeuedMessageId = message.getMessageId();
                ClientCnx msgCnx = ((MessageImpl<?>) message).getCnx();
                // synchronized need to prevent race between connectionOpened and the check "msgCnx == cnx()"
                synchronized (this) {
                    // if message received due to an old flow - discard it and wait for the message from the
                    // latest flow command
                    if (msgCnx == cnx()) {
                        waitingOnReceiveForZeroQueueSize = false;
                        break;
                    }
                }
            } while (true);

            stats.updateNumMsgsReceived(message);
            return message;
        } catch (InterruptedException e) {
            stats.incrementNumReceiveFailed();
            throw PulsarClientException.unwrap(e);
        } finally {
            // Finally blocked is invoked in case the block on incomingMessages is interrupted
            waitingOnReceiveForZeroQueueSize = false;
            // Clearing the queue in case there was a race with messageReceived
            incomingMessages.clear();
        }
    }

    @Override
    protected void consumerIsReconnectedToBroker(ClientCnx cnx, int currentQueueSize) {
        super.consumerIsReconnectedToBroker(cnx, currentQueueSize);

        // For zerosize queue : If the connection is reset and someone is waiting for the messages
        // or queue was not empty: send a flow command
        if (waitingOnReceiveForZeroQueueSize
                || currentQueueSize > 0
                || (listener != null && !waitingOnListenerForZeroQueueSize)) {
            increaseAvailablePermits(cnx);
        }
    }

    @Override
    protected boolean canEnqueueMessage(Message<T> message) {
        if (listener != null) {
            triggerZeroQueueSizeListener(message);
            return false;
        } else {
            return true;
        }
    }

    private void triggerZeroQueueSizeListener(final Message<T> message) {
        Objects.requireNonNull(listener, "listener can't be null");
        Objects.requireNonNull(message, "unqueued message can't be null");

        externalPinnedExecutor.execute(() -> {
            stats.updateNumMsgsReceived(message);
            try {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Calling message listener for unqueued message {}", topic, subscription,
                            message.getMessageId());
                }
                waitingOnListenerForZeroQueueSize = true;
                trackMessage(message);
                listener.received(ZeroQueueConsumerImpl.this, beforeConsume(message));
            } catch (Throwable t) {
                log.error("[{}][{}] Message listener error in processing unqueued message: {}", topic, subscription,
                        message.getMessageId(), t);
            }
            increaseAvailablePermits(cnx());
            waitingOnListenerForZeroQueueSize = false;
        });
    }

    @Override
    protected void tryTriggerListener() {
        // Ignore since it was already triggered in the triggerZeroQueueSizeListener() call
    }

    @Override
    void receiveIndividualMessagesFromBatch(BrokerEntryMetadata brokerEntryMetadata, MessageMetadata msgMetadata,
                                            int redeliveryCount, List<Long> ackSet, ByteBuf uncompressedPayload,
                                            MessageIdData messageId, ClientCnx cnx, long consumerEpoch) {
        log.warn(
                "Closing consumer [{}]-[{}] due to unsupported received batch-message with zero receiver queue size",
                subscription, consumerName);
        // close connection
        closeAsync().handle((ok, e) -> {
            // notify callback with failure result
            notifyPendingReceivedCallback(null,
                    new PulsarClientException.InvalidMessageException(
                            format("Unsupported Batch message with 0 size receiver queue for [%s]-[%s] ",
                                    subscription, consumerName)));
            return null;
        });
    }
}
