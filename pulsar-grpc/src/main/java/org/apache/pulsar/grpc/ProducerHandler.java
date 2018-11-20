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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Enums;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededError;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.grpc.proto.ProducerAck;
import org.apache.pulsar.grpc.proto.ProducerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.pulsar.grpc.Constant.GRPC_PROXY_CTX_KEY;


public class ProducerHandler implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

    private final GrpcService service;
    private final StreamObserver<ProducerAck> ackStreamObserver;
    private String topic;
    private Producer<byte[]> producer;
    private volatile long msgPublishedCounter = 0;
    private static final AtomicLongFieldUpdater<ProducerHandler> MSG_PUBLISHED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerHandler.class, "msgPublishedCounter");

    public ProducerHandler(GrpcService service, StreamObserver<ProducerAck> ackStreamObserver) {
        this.service = service;
        this.ackStreamObserver = ackStreamObserver;
        Map<String, String> params = GRPC_PROXY_CTX_KEY.get();


        // TODO: AuthN/AuthZ
        /*if (!checkAuth(response)) {
            return;
        }*/

        try {
            this.topic = params.get("pulsar-topic");
            checkArgument(!Strings.isNullOrEmpty(topic), "Empty topic name");
            this.producer = getProducerBuilder(params, service.getPulsarClient()).topic(topic).create();

        } catch (Exception e) {
            throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
        }
    }

    private static Status getStatus(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT;
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
                    builder.properties(message.getProperties());
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
                    log.warn("[{}] Error occurred while producer handler was sending msg: {}", producer.getTopic(),
                            /*getRemote().getInetSocketAddress().toString(),*/ exception.getMessage());
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

    private ProducerBuilder<byte[]> getProducerBuilder(Map<String, String> queryParams, PulsarClient client) {
        ProducerBuilder<byte[]> builder = client.newProducer()
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

        // Set to false to prevent the server thread from being blocked if a lot of messages are pending.
        builder.blockIfQueueFull(false);

        if (queryParams.containsKey("pulsar-producer-name")) {
            builder.producerName(queryParams.get("pulsar-producer-name"));
        }

        if (queryParams.containsKey("pulsar-initial-sequence-id")) {
            builder.initialSequenceId(Long.parseLong("pulsar-initial-sequence-id"));
        }

        if (queryParams.containsKey("pulsar-hashing-scheme")) {
            builder.hashingScheme(HashingScheme.valueOf(queryParams.get("pulsar-hashing-scheme")));
        }

        if (queryParams.containsKey("pulsar-send-timeout-millis")) {
            builder.sendTimeout(Integer.parseInt(queryParams.get("pulsar-send-timeout-millis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("pulsar-batching-enabled")) {
            builder.enableBatching(Boolean.parseBoolean(queryParams.get("pulsar-batching-enabled")));
        }

        if (queryParams.containsKey("pulsar-batching-max-messages")) {
            builder.batchingMaxMessages(Integer.parseInt(queryParams.get("pulsar-batching-max-messages")));
        }

        if (queryParams.containsKey("pulsar-max-pending-messages")) {
            builder.maxPendingMessages(Integer.parseInt(queryParams.get("pulsar-max-pending-messages")));
        }

        if (queryParams.containsKey("pulsar-batching-max-publish-delay")) {
            builder.batchingMaxPublishDelay(Integer.parseInt(queryParams.get("pulsar-batching-max-publish-delay")),
                    TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("pulsar-message-routing-mode")) {
            checkArgument(
                    Enums.getIfPresent(MessageRoutingMode.class, queryParams.get("pulsar-message-routing-mode")).isPresent(),
                    "Invalid messageRoutingMode %s", queryParams.get("pulsar-message-routing-mode"));
            MessageRoutingMode routingMode = MessageRoutingMode.valueOf(queryParams.get("pulsar-message-routing-mode"));
            if (!MessageRoutingMode.CustomPartition.equals(routingMode)) {
                builder.messageRoutingMode(routingMode);
            }
        }

        if (queryParams.containsKey("pulsar-compression-type")) {
            checkArgument(Enums.getIfPresent(CompressionType.class, queryParams.get("pulsar-compression-type")).isPresent(),
                    "Invalid compressionType %s", queryParams.get("pulsar-compression-type"));
            builder.compressionType(CompressionType.valueOf(queryParams.get("pulsar-compression-type")));
        }

        return builder;
    }
}
