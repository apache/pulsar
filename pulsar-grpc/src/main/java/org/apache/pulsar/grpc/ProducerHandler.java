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

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededError;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.grpc.proto.ProducerAck;
import org.apache.pulsar.grpc.proto.ProducerMessage;
import org.apache.pulsar.grpc.proto.ProducerParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.naming.NoPermissionException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


public class ProducerHandler extends AbstractGrpcHandler {

    private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

    private final StreamObserver<ProducerAck> ackStreamObserver;
    private Producer<byte[]> producer;
    private volatile long msgPublishedCounter = 0;
    private static final AtomicLongFieldUpdater<ProducerHandler> MSG_PUBLISHED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerHandler.class, "msgPublishedCounter");

    public ProducerHandler(GrpcProxyService service, StreamObserver<ProducerAck> ackStreamObserver) {
        super(service);
        this.ackStreamObserver = ackStreamObserver;
        ProducerParameters parameters = clientParameters.getProducerParameters();

        try {
            checkAuth();
            this.producer = getProducerBuilder(parameters, service.getPulsarClient())
                    .topic(topic.toString()).create();

        } catch (Exception e) {
            log.warn("[{}] Failed in creating producer on topic {}: {}", remoteAddress, topic, e.getMessage());
            throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
        }
    }

    private static Status getStatus(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT;
        } else if (e instanceof AuthenticationException) {
            return Status.UNAUTHENTICATED;
        } else if (e instanceof NoPermissionException) {
            return Status.PERMISSION_DENIED;
        } else if (e instanceof ProducerBusyException) {
            return Status.ALREADY_EXISTS;
        } else if (e instanceof ProducerBlockedQuotaExceededError || e instanceof ProducerBlockedQuotaExceededException) {
            return Status.RESOURCE_EXHAUSTED;
        } else {
            return Status.INTERNAL;
        }
    }

    private static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return "Invalid header params: " + e.getMessage();
        } else {
            return "Failed to create producer: " + e.getMessage();
        }
    }

    public StreamObserver<ProducerMessage> produce() {
        return new StreamObserver<ProducerMessage>() {
            @Override
            public void onNext(ProducerMessage message) {
                TypedMessageBuilder<byte[]> builder = producer.newMessage();
                String requestContext = message.getContext();
                try {
                    builder.value(message.getPayload().toByteArray());
                } catch (SchemaSerializationException e) {
                    ackStreamObserver.onNext(
                            ProducerAck.newBuilder()
                                    .setStatusCode(Code.INVALID_ARGUMENT.value())
                                    .setErrorMsg(e.getMessage())
                                    .setContext(requestContext)
                                    .build()
                    );
                    return;
                }

                if (message.getPropertiesCount() != 0) {
                    builder.properties(message.getPropertiesMap());
                }
                if (!Strings.isNullOrEmpty(message.getKey())) {
                    builder.key(message.getKey());
                }
                if (message.getReplicationClustersCount() != 0) {
                    builder.replicationClusters(message.getReplicationClustersList());
                }

                final long now = System.nanoTime();
                builder.sendAsync().thenAccept(msgId -> {
                    //updateSentMsgStats(msgSize, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - now));
                    ackStreamObserver.onNext(ProducerAck.newBuilder()
                            .setStatusCode(Code.OK.value())
                            .setMessageId(ByteString.copyFrom(msgId.toByteArray()))
                            .setContext(requestContext)
                            .build());
                }).exceptionally(exception -> {
                    log.warn("[{}] Error occurred while producer handler was sending msg from {}: {}", producer.getTopic(),
                            remoteAddress, exception.getMessage());
                    //numMsgsFailed.increment();
                    ackStreamObserver.onNext(ProducerAck.newBuilder()
                            .setStatusCode(Code.INTERNAL.value())
                            .setErrorMsg(exception.getMessage())
                            .setContext(requestContext)
                            .build());
                    return null;
                });
            }

            @Override
            public void onError(Throwable t) {
                ackStreamObserver.onError(t);
                close();
            }

            @Override
            public void onCompleted() {
                ackStreamObserver.onCompleted();
                close();
            }
        };
    }



    @Override
    public void close() {
        if (producer != null) {
            /*if (!this.service.removeProducer(this)) {
                log.warn("[{}] Failed to remove producer handler", producer.getTopic());
            }*/
            producer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed producer asynchronously", producer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close producer", producer.getTopic(), exception);
                return null;
            });
        }
    }

    private static HashingScheme toHashingScheme(ProducerParameters.HashingScheme  scheme) {
        switch(scheme) {
            case HASHING_SCHEME_JAVA_STRING_HASH:
                return HashingScheme.JavaStringHash;
            case HASHING_SCHEME_MURMUR3_32HASH:
                return HashingScheme.Murmur3_32Hash;
            case HASHING_SCHEME_DEFAULT:
                return null;
        }
        throw new IllegalArgumentException("Invalid hashing scheme");
    }

    private static MessageRoutingMode toMessageRoutingMode(ProducerParameters.MessageRoutingMode mode) {
        switch(mode) {
            case MESSAGE_ROUTING_MODE_SINGLE_PARTITION:
                return MessageRoutingMode.SinglePartition;
            case MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION:
                return MessageRoutingMode.RoundRobinPartition;
            case MESSAGE_ROUTING_MODE_DEFAULT:
                return null;
        }
        throw new IllegalArgumentException("Invalid message routing mode");
    }

    private static CompressionType toCompressionType(ProducerParameters.CompressionType type) {
        switch(type) {
            case COMPRESSION_TYPE_NONE:
                return CompressionType.NONE;
            case COMPRESSION_TYPE_LZ4:
                return CompressionType.LZ4;
            case COMPRESSION_TYPE_ZLIB:
                return CompressionType.ZLIB;
            case COMPRESSION_TYPE_DEFAULT:
                return null;
        }
        throw new IllegalArgumentException("Invalid compression type");
    }

    private ProducerBuilder<byte[]> getProducerBuilder(ProducerParameters params, PulsarClient client) {
        ProducerBuilder<byte[]> builder = client.newProducer()
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition);

        // Set to false to prevent the server thread from being blocked if a lot of messages are pending.
        builder.blockIfQueueFull(false);

        if (!params.getProducerName().isEmpty()) {
            builder.producerName(params.getProducerName());
        }

        if (params.hasInitialSequenceId()) {
            builder.initialSequenceId(params.getInitialSequenceId().getValue());
        }

        Optional.ofNullable(toHashingScheme(params.getHashingScheme())).ifPresent(builder::hashingScheme);

        if (params.hasSendTimeoutMillis()) {
            builder.sendTimeout(params.getSendTimeoutMillis().getValue(), TimeUnit.MILLISECONDS);
        }

        if (params.hasBatchingEnabled()) {
            builder.enableBatching(params.getBatchingEnabled().getValue());
        }

        if (params.hasBatchingMaxMessages()) {
            builder.batchingMaxMessages(params.getBatchingMaxMessages().getValue());
        }

        if (params.hasMaxPendingMessages()) {
            builder.maxPendingMessages(params.getMaxPendingMessages().getValue());
        }

        if (params.hasBatchingMaxPublishDelay()) {
            builder.batchingMaxPublishDelay(params.getBatchingMaxPublishDelay().getValue(), TimeUnit.MILLISECONDS);
        }

        Optional.ofNullable(toMessageRoutingMode(params.getMessageRoutingMode())).ifPresent(builder::messageRoutingMode);

        Optional.ofNullable(toCompressionType(params.getCompressionType())).ifPresent(builder::compressionType);

        return builder;
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        return service.getAuthorizationService().canProduce(topic, authRole, authenticationData);
    }

}
