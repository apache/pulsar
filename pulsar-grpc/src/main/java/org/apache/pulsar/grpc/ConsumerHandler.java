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
package org.apache.pulsar.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.DeadLetterPolicy.DeadLetterPolicyBuilder;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.grpc.proto.ConsumerAck;
import org.apache.pulsar.grpc.proto.ConsumerMessage;
import org.apache.pulsar.grpc.proto.ConsumerParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.naming.NoPermissionException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;

public class ConsumerHandler extends AbstractGrpcHandler {

    private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);

    private final StreamObserver<ConsumerMessage> messageStreamObserver;
    private String subscription;
    private Consumer<byte[]> consumer;

    private int maxPendingMessages;
    private final AtomicInteger pendingMessages = new AtomicInteger();

    private volatile long msgDeliveredCounter = 0;
    private static final AtomicLongFieldUpdater<ConsumerHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ConsumerHandler.class, "msgDeliveredCounter");

    ConsumerHandler(GrpcProxyService service, StreamObserver<ConsumerMessage> messageStreamObserver) {
        super(service);
        this.messageStreamObserver = messageStreamObserver;
        ConsumerParameters parameters = clientParameters.getConsumerParameters();

        try {
            ConsumerBuilderImpl<byte[]> builder = (ConsumerBuilderImpl<byte[]>) getConsumerConfiguration(
                    parameters,
                    service.getPulsarClient()
            );
            this.maxPendingMessages = (builder.getConf().getReceiverQueueSize() == 0) ? 1
                    : builder.getConf().getReceiverQueueSize();

            // checkAuth() should be called after assigning a value to this.subscription
            this.subscription = parameters.getSubscription();
            checkArgument(!subscription.isEmpty(), "Empty subscription name");

            checkAuth();

            consumer = builder
                    .topic(topic.toString())
                    .subscriptionName(subscription)
                    .subscribe();

        } catch (Exception e) {
            throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
        }
    }

    public StreamObserver<ConsumerAck> consume() {
        receiveMessage();
        return new StreamObserver<ConsumerAck>() {
            @Override
            public void onNext(ConsumerAck ack) {
                MessageId msgId;
                try {
                    msgId = MessageId.fromByteArrayWithTopic(
                            ack.getMessageId().toByteArray(),
                            TopicName.get(consumer.getTopic()));
                } catch (IOException e) {
                    log.warn("Failed to deserialize message id: {}", ack.getMessageId(), e);
                    messageStreamObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
                    throw Status.INVALID_ARGUMENT.asRuntimeException();
                }
                consumer.acknowledgeAsync(msgId).thenAccept(consumer -> {
                    log.error("acknowledged " + msgId);
                    //numMsgsAcked.increment();
                });

                int pending = pendingMessages.getAndDecrement();
                if (pending >= maxPendingMessages) {
                    // Resume delivery
                    receiveMessage();
                }
            }

            @Override
            public void onError(Throwable t) {
                messageStreamObserver.onError(t);
                close();
            }

            @Override
            public void onCompleted() {
                messageStreamObserver.onCompleted();
                close();
            }
        };
    }

    private static Status getStatus(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT;
        } else if (e instanceof AuthenticationException) {
            return Status.UNAUTHENTICATED;
        } else if (e instanceof NoPermissionException) {
            return Status.PERMISSION_DENIED;
        } else if (e instanceof PulsarClientException.ConsumerBusyException) {
            return  Status.ALREADY_EXISTS;
        } else {
            return Status.INTERNAL;
        }
    }

    private static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return "Invalid header params: " + e.getMessage();
        } else {
            return "Failed to subscribe: " + e.getMessage();
        }
    }

    private void receiveMessage() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Receive next message", remoteAddress, topic, subscription);
        }

        consumer.receiveAsync().thenAccept(msg -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Got message {}", remoteAddress, topic, subscription,
                        msg.getMessageId());
            }

            ConsumerMessage.Builder dm = ConsumerMessage.newBuilder();
            dm.setMessageId(ByteString.copyFrom(msg.getMessageId().toByteArray()));
            dm.setPayload(ByteString.copyFrom(msg.getData()));
            dm.putAllProperties(msg.getProperties());
            dm.setPublishTime(msg.getPublishTime());
            dm.setEventTime(msg.getEventTime());
            //TODO: needs proto hasKey or empty string is OK ?
            if (msg.hasKey()) {
                dm.setKey(msg.getKey());
            }
            //final long msgSize = msg.getData().length;

            //TODO: thread safety ?
            messageStreamObserver.onNext(dm.build());

            int pending = pendingMessages.incrementAndGet();
            if (pending < maxPendingMessages) {
                // Start next read in a separate thread to avoid recursion
                // TODO: needed ?
                service.getExecutor().execute(this::receiveMessage);
            }
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                log.info("[{}/{}] Consumer was closed while receiving msg from broker", consumer.getTopic(),
                        subscription);
            } else {
                log.warn("[{}/{}] Error occurred while consumer handler was delivering msg to {}: {}",
                        consumer.getTopic(), subscription, remoteAddress,
                        exception.getMessage());
            }
            return null;
        });
    }

    private static SubscriptionType toSubscriptionType(ConsumerParameters.SubscriptionType type) {
        switch(type) {
            case SUBSCRIPTION_TYPE_EXCLUSIVE:
                return SubscriptionType.Exclusive;
            case SUBSCRIPTION_TYPE_FAILOVER:
                return SubscriptionType.Failover;
            case SUBSCRIPTION_TYPE_SHARED:
                return SubscriptionType.Shared;
            case SUBSCRIPTION_TYPE_DEFAULT:
                return null;
        }
        throw new IllegalArgumentException("Invalid subscription type");
    }

    private ConsumerBuilder<byte[]> getConsumerConfiguration(ConsumerParameters params, PulsarClient client) {
        ConsumerBuilder<byte[]> builder = client.newConsumer();

        if (params.hasAckTimeoutMillis()) {
            builder.ackTimeout(params.getAckTimeoutMillis().getValue(), TimeUnit.MILLISECONDS);
        }

        Optional.ofNullable(toSubscriptionType(params.getSubscriptionType())).ifPresent(builder::subscriptionType);

        if (params.hasReceiverQueueSize()) {
            builder.receiverQueueSize(params.getReceiverQueueSize().getValue());
        }

        if (!params.getConsumerName().isEmpty()) {
            builder.consumerName(params.getConsumerName());
        }

        if (params.hasPriorityLevel()) {
            builder.priorityLevel(params.getPriorityLevel().getValue());
        }

        if (params.hasDeadLetterPolicy()) {
            org.apache.pulsar.grpc.proto.DeadLetterPolicy deadLetterPolicy = params.getDeadLetterPolicy();
            DeadLetterPolicyBuilder dlpBuilder = DeadLetterPolicy.builder();
            if (deadLetterPolicy.hasMaxRedeliverCount()) {
                dlpBuilder.maxRedeliverCount(deadLetterPolicy.getMaxRedeliverCount().getValue())
                        .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription));
            }

            if (!deadLetterPolicy.getDeadLetterTopic().isEmpty()) {
                dlpBuilder.deadLetterTopic(deadLetterPolicy.getDeadLetterTopic());
            }
            builder.deadLetterPolicy(dlpBuilder.build());
        }
        return builder;
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        return service.getAuthorizationService().canConsume(topic, authRole, authenticationData,
                this.subscription);
    }

    @Override
    public void close() {
        if (consumer != null) {
            /*if (!this.service.removeConsumer(this)) {
                log.warn("[{}] Failed to remove consumer handler", consumer.getTopic());
            }*/
            consumer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed consumer asynchronously", consumer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close consumer", consumer.getTopic(), exception);
                return null;
            });
        }
    }

}
