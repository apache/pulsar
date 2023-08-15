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
package org.apache.pulsar.websocket;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.websocket.WebSocketError.FailedToDeserializeFromJSON;
import static org.apache.pulsar.websocket.WebSocketError.PayloadEncodingError;
import static org.apache.pulsar.websocket.WebSocketError.UnknownError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Enums;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.DummyCryptoKeyReaderImpl;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.service.WSSDummyMessageCryptoImpl;
import org.apache.pulsar.websocket.stats.StatsBuckets;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Websocket end-point url handler to handle incoming message coming from client. Websocket end-point url handler to
 * handle incoming message coming from client.
 * <p>
 * On every produced message from client it calls broker to persists it.
 * </p>
 *
 */

public class ProducerHandler extends AbstractWebSocketHandler {

    private WebSocketService service;
    private Producer<byte[]> producer;
    private final LongAdder numMsgsSent;
    private final LongAdder numMsgsFailed;
    private final LongAdder numBytesSent;
    private final StatsBuckets publishLatencyStatsUSec;
    private volatile long msgPublishedCounter = 0;
    private boolean clientSideEncrypt;
    private static final AtomicLongFieldUpdater<ProducerHandler> MSG_PUBLISHED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerHandler.class, "msgPublishedCounter");

    public static final List<Long> ENTRY_LATENCY_BUCKETS_USEC = Collections.unmodifiableList(Arrays.asList(
            500L, 1_000L, 5_000L, 10_000L, 20_000L, 50_000L, 100_000L, 200_000L, 1000_000L));
    private final ObjectReader producerMessageReader =
            ObjectMapperFactory.getMapper().reader().forType(ProducerMessage.class);

    public ProducerHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
        super(service, request, response);
        this.numMsgsSent = new LongAdder();
        this.numBytesSent = new LongAdder();
        this.numMsgsFailed = new LongAdder();
        this.publishLatencyStatsUSec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
        this.service = service;

        if (!checkAuth(response)) {
            return;
        }

        try {
            this.producer = getProducerBuilder(service.getPulsarClient()).topic(topic.toString()).create();
            if (clientSideEncrypt) {
                log.info("[{}] [{}] The producer session is created with param encryptionKeyValues, which means that"
                                + " message encryption will be done on the client side, then the server will skip "
                                + "batch message processing, message compression processing, and message encryption"
                                + " processing", producer.getTopic(), producer.getProducerName());
            }
            if (!this.service.addProducer(this)) {
                log.warn("[{}:{}] Failed to add producer handler for topic {}", request.getRemoteAddr(),
                        request.getRemotePort(), topic);
            }
        } catch (Exception e) {
            log.warn("[{}:{}] Failed in creating producer on topic {}: {}", request.getRemoteAddr(),
                    request.getRemotePort(), topic, e.getMessage());

            try {
                response.sendError(getErrorCode(e), getErrorMessage(e));
            } catch (IOException e1) {
                log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                        e1.getMessage(), e1);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            if (!this.service.removeProducer(this)) {
                log.warn("[{}] Failed to remove producer handler", producer.getTopic());
            }
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

    @Override
    public void onWebSocketText(String message) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received new message from producer {} ", producer.getTopic(),
                    getRemote().getInetSocketAddress().toString());
        }
        ProducerMessage sendRequest;
        byte[] rawPayload = null;
        String requestContext = null;
        try {
            sendRequest = producerMessageReader.readValue(message);
            requestContext = sendRequest.context;
            rawPayload = Base64.getDecoder().decode(sendRequest.payload);
        } catch (IOException e) {
            sendAckResponse(new ProducerAck(FailedToDeserializeFromJSON, e.getMessage(), null, null));
            return;
        } catch (IllegalArgumentException e) {
            String msg = format("Invalid Base64 message-payload error=%s", e.getMessage());
            sendAckResponse(new ProducerAck(PayloadEncodingError, msg, null, requestContext));
            return;
        } catch (NullPointerException e) {
            // Null payload
            sendAckResponse(new ProducerAck(PayloadEncodingError, e.getMessage(), null, requestContext));
            return;
        }

        final long msgSize = rawPayload.length;
        TypedMessageBuilderImpl<byte[]> builder = (TypedMessageBuilderImpl<byte[]>) producer.newMessage();

        try {
            builder.value(rawPayload);
        } catch (SchemaSerializationException e) {
            sendAckResponse(new ProducerAck(PayloadEncodingError, e.getMessage(), null, requestContext));
            return;
        }

        if (sendRequest.properties != null) {
            builder.properties(sendRequest.properties);
        }
        if (sendRequest.key != null) {
            builder.key(sendRequest.key);
        }
        if (sendRequest.replicationClusters != null) {
            builder.replicationClusters(sendRequest.replicationClusters);
        }
        if (sendRequest.eventTime != null) {
            try {
                builder.eventTime(DateFormatter.parse(sendRequest.eventTime));
            } catch (DateTimeParseException e) {
                sendAckResponse(new ProducerAck(PayloadEncodingError, e.getMessage(), null, requestContext));
                return;
            }
        }
        if (sendRequest.deliverAt > 0) {
            builder.deliverAt(sendRequest.deliverAt);
        }
        if (sendRequest.deliverAfterMs > 0) {
            builder.deliverAfter(sendRequest.deliverAfterMs, TimeUnit.MILLISECONDS);
        }

        // If client-side encryption is enabled, the attributes "encryptParam", "uncompressedMessageSize",
        // "uncompressedMessageSize" and "batchSize" of message metadata must be set according to the parameters
        // when the client sends messages.
        if (clientSideEncrypt) {
            try {
                if (!StringUtils.isBlank(sendRequest.encryptionParam)) {
                    builder.getMetadataBuilder().setEncryptionParam(Base64.getDecoder()
                            .decode(sendRequest.encryptionParam));
                }
            } catch (Exception e){
                String msg = format("Invalid Base64 encryptionParam error=%s", e.getMessage());
                sendAckResponse(new ProducerAck(PayloadEncodingError, msg, null, requestContext));
                return;
            }
            if (sendRequest.compressionType != null && sendRequest.uncompressedMessageSize != null) {
                // Set compression information.
                builder.getMetadataBuilder().setCompression(sendRequest.compressionType);
                builder.getMetadataBuilder().setUncompressedSize(sendRequest.uncompressedMessageSize);
            }
            if (sendRequest.batchSize != null) {
                builder.getMetadataBuilder().setNumMessagesInBatch(sendRequest.batchSize);
            }
        }

        final long now = System.nanoTime();

        builder.sendAsync().thenAccept(msgId -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Success fully write the message to broker with returned message ID {} from producer {}",
                        producer.getTopic(), msgId, getRemote().getInetSocketAddress().toString());
            }
            updateSentMsgStats(msgSize, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - now));
            if (isConnected()) {
                String messageId = Base64.getEncoder().encodeToString(msgId.toByteArray());
                sendAckResponse(new ProducerAck(messageId, sendRequest.context));
            }
        }).exceptionally(exception -> {
            log.warn("[{}] Error occurred while producer handler was sending msg from {}: {}", producer.getTopic(),
                    getRemote().getInetSocketAddress().toString(), exception.getMessage(), exception);
            numMsgsFailed.increment();
            sendAckResponse(
                    new ProducerAck(UnknownError, exception.getMessage(), null, sendRequest.context));
            return null;
        });
    }

    public Producer<byte[]> getProducer() {
        return this.producer;
    }

    public long getAndResetNumMsgsSent() {
        return numMsgsSent.sumThenReset();
    }

    public long getAndResetNumBytesSent() {
        return numBytesSent.sumThenReset();
    }

    public long getAndResetNumMsgsFailed() {
        return numMsgsFailed.sumThenReset();
    }

    public long[] getAndResetPublishLatencyStatsUSec() {
        publishLatencyStatsUSec.refresh();
        return publishLatencyStatsUSec.getBuckets();
    }

    public StatsBuckets getPublishLatencyStatsUSec() {
        return this.publishLatencyStatsUSec;
    }

    public long getMsgPublishedCounter() {
        return msgPublishedCounter;
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        try {
            return service.getAuthorizationService()
                    .allowTopicOperationAsync(topic, TopicOperation.PRODUCE, authRole, authenticationData)
                    .get(service.getConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (TimeoutException e) {
            log.warn("Time-out {} sec while checking authorization on {} ",
                    service.getConfig().getMetadataStoreOperationTimeoutSeconds(), topic);
            throw e;
        } catch (Exception e) {
            log.warn("Producer-client  with Role - {} failed to get permissions for topic - {}. {}", authRole, topic,
                    e.getMessage());
            throw e;
        }
    }

    private void sendAckResponse(ProducerAck response) {
        try {
            String msg = objectWriter().writeValueAsString(response);
            getSession().getRemote().sendString(msg, new WriteCallback() {
                @Override
                public void writeFailed(Throwable th) {
                    log.warn("[{}] Failed to send ack: {}", producer.getTopic(), th.getMessage());
                }

                @Override
                public void writeSuccess() {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ack was sent successfully to {}", producer.getTopic(),
                                getRemote().getInetSocketAddress().toString());
                    }
                }
            });
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to generate ack json-response: {}", producer.getTopic(), e.getMessage());
        } catch (Exception e) {
            log.warn("[{}] Failed to send ack: {}", producer.getTopic(), e.getMessage());
        }
    }

    private void updateSentMsgStats(long msgSize, long latencyUsec) {
        this.publishLatencyStatsUSec.addValue(latencyUsec);
        this.numBytesSent.add(msgSize);
        this.numMsgsSent.increment();
        MSG_PUBLISHED_COUNTER_UPDATER.getAndIncrement(this);
    }

    protected ProducerBuilder<byte[]> getProducerBuilder(PulsarClient client) {
        ProducerBuilder<byte[]> builder = client.newProducer()
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

        // Set to false to prevent the server thread from being blocked if a lot of messages are pending.
        builder.blockIfQueueFull(false);

        if (queryParams.containsKey("producerName")) {
            builder.producerName(queryParams.get("producerName"));
        }

        if (queryParams.containsKey("initialSequenceId")) {
            builder.initialSequenceId(Long.parseLong(queryParams.get("initialSequenceId")));
        }

        if (queryParams.containsKey("hashingScheme")) {
            builder.hashingScheme(HashingScheme.valueOf(queryParams.get("hashingScheme")));
        }

        if (queryParams.containsKey("sendTimeoutMillis")) {
            builder.sendTimeout(Integer.parseInt(queryParams.get("sendTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("messageRoutingMode")) {
            checkArgument(
                    Enums.getIfPresent(MessageRoutingMode.class, queryParams.get("messageRoutingMode")).isPresent(),
                    "Invalid messageRoutingMode %s", queryParams.get("messageRoutingMode"));
            MessageRoutingMode routingMode = MessageRoutingMode.valueOf(queryParams.get("messageRoutingMode"));
            if (!MessageRoutingMode.CustomPartition.equals(routingMode)) {
                builder.messageRoutingMode(routingMode);
            }
        }

        if (queryParams.containsKey("encryptionKeyValues")) {
            popularProducerBuilderForClientSideEncrypt(builder);
        } else {
            popularProducerBuilderForServerSideEncrypt(builder);
        }
        return builder;
    }

    private void popularProducerBuilderForClientSideEncrypt(ProducerBuilder<byte[]> builder) {
        this.clientSideEncrypt = true;
        final String keyMetadataParamsStr = queryParams.get("encryptionKeyMetadata");
        final String[] keyDataParams = queryParams.get("encryptionKeyValues").split(",");
        final String[] keyMetadataParams = keyMetadataParamsStr == null ? null
                : keyMetadataParamsStr.split(",");
        final String[] keyNameArr = queryParams.get("encryptionKeys").split(",");

        // Check params length.
        checkArgument(keyNameArr.length > 0,
                String.format("Invalid encryptionKeys %s", queryParams.get("encryptionKeys")));
        checkArgument(keyNameArr.length == keyDataParams.length,
                "The count of encryptionKeys and encryptionKeyValues are not equal.");

        // Defer params decoded.
        final List<KeyValue>[] keyMetadataArr = new ArrayList[keyNameArr.length];
        final byte[][] keyDadaArr = new byte[keyNameArr.length][];

        try {
            // Check and decode params.
            for (int i = 0; i < keyNameArr.length; i++) {
                // Check and trim params.
                keyNameArr[i] = StringUtils.trim(keyNameArr[i]);
                keyDataParams[i] = StringUtils.trim(keyDataParams[i]);
                checkArgument(!StringUtils.isBlank(keyNameArr[i]),
                        String.format("There is an blank encryptionKey %s", queryParams.get("encryptionKeys")));
                checkArgument(!StringUtils.isBlank(keyDataParams[i]),
                        String.format("There is an blank encryptionValue %s", queryParams.get("encryptionKeys")));
                if (keyMetadataParams != null && StringUtils.isNoneBlank(keyMetadataParams[i])) {
                    keyMetadataParams[i] = StringUtils.trim(keyMetadataParams[i]);
                }
                // Decode params.
                try {
                    keyDadaArr[i] = Base64.getDecoder().decode(keyDataParams[i]);
                    keyMetadataParams[i] = new String(Base64.getDecoder().decode(keyMetadataParams[i]),
                            StandardCharsets.UTF_8);
                } catch (Exception base64DecodeEx) {
                    log.error("Could not base64 decode the param encryptionKeyValues or encryptionKeyMetadata",
                            base64DecodeEx);
                    throw new IllegalArgumentException(
                            "Could not base64 decode the param encryptionKeyValues or encryptionKeyMetadata");
                }
                if (keyMetadataParams != null && StringUtils.isNoneBlank(keyMetadataParams[i])) {
                    keyMetadataArr[i] = ObjectMapperFactory.getMapper().getObjectMapper()
                            .readValue(keyMetadataParams[i], new TypeReference<List<KeyValue>>() {
                            });
                } else {
                    keyMetadataArr[i] = Collections.emptyList();
                }
                // Add encrypt key.
                builder.addEncryptionKey(keyNameArr[i]);
            }
        } catch (JsonProcessingException jsonProcessingException) {
            log.error("The parameter encryptionKeyMetadata is not a correct json", jsonProcessingException);
            throw new IllegalArgumentException("The parameter encryptionKeyMetadata is not a correct json");
        }
        // Disable server-side batch process, compression, and encryption, and only set the message metadata that
        // which client-side provided.
        builder.enableBatching(false);
        builder.compressionType(CompressionType.NONE);
        builder.cryptoKeyReader(DummyCryptoKeyReaderImpl.INSTANCE);
        // Inject encryption metadata decorator.
        builder.messageCrypto(new WSSDummyMessageCryptoImpl(msgMetadata -> {
            for (int i = 0; i < keyNameArr.length; i++) {
                msgMetadata.addEncryptionKey().setKey(keyNameArr[i]).setValue(keyDadaArr[i])
                        .addAllMetadatas(keyMetadataArr[i]);
            }
        }));
        // Do warning param check and print warning log.
        printWarnLogIfSettingBatchedParams();
        printWarnLogIfSettingCompressionParams();
    }

    private void popularProducerBuilderForServerSideEncrypt(ProducerBuilder<byte[]> builder) {
        this.clientSideEncrypt = false;
        if (queryParams.containsKey("batchingEnabled")) {
            boolean batchingEnabled = Boolean.parseBoolean(queryParams.get("batchingEnabled"));
            if (batchingEnabled) {
                builder.enableBatching(true);
                if (queryParams.containsKey("batchingMaxMessages")) {
                    builder.batchingMaxMessages(Integer.parseInt(queryParams.get("batchingMaxMessages")));
                }

                if (queryParams.containsKey("maxPendingMessages")) {
                    builder.maxPendingMessages(Integer.parseInt(queryParams.get("maxPendingMessages")));
                }

                if (queryParams.containsKey("batchingMaxPublishDelay")) {
                    builder.batchingMaxPublishDelay(Integer.parseInt(queryParams.get("batchingMaxPublishDelay")),
                            TimeUnit.MILLISECONDS);
                }
            } else {
                builder.enableBatching(false);
                printWarnLogIfSettingBatchedParams();
            }
        }

        if (queryParams.containsKey("compressionType")) {
            checkArgument(Enums.getIfPresent(CompressionType.class, queryParams.get("compressionType")).isPresent(),
                    "Invalid compressionType %s", queryParams.get("compressionType"));
            builder.compressionType(CompressionType.valueOf(queryParams.get("compressionType")));
        }

        if (queryParams.containsKey("encryptionKeys")) {
            builder.cryptoKeyReader(service.getCryptoKeyReader().orElseThrow(() -> new IllegalStateException(
                    "Can't add encryption key without configuring cryptoKeyReaderFactoryClassName")));
            String[] keys = queryParams.get("encryptionKeys").split(",");
            for (String key : keys) {
                builder.addEncryptionKey(key);
            }
        }
    }

    private void printWarnLogIfSettingBatchedParams() {
        if (clientSideEncrypt && queryParams.containsKey("batchingMaxMessages")) {
            log.warn("Since clientSideEncrypt is true, the param batchingEnabled of producer will be ignored");
        }
        if (queryParams.containsKey("batchingMaxMessages")) {
            log.warn("Since batchingEnabled is false, the param batchingMaxMessages of producer will be ignored");
        }
        if (queryParams.containsKey("maxPendingMessages")) {
            log.warn("Since batchingEnabled is false, the param maxPendingMessages of producer will be ignored");
        }
        if (queryParams.containsKey("batchingMaxPublishDelay")) {
            log.warn("Since batchingEnabled is false, the param batchingMaxPublishDelay of producer will be ignored");
        }
    }

    private void printWarnLogIfSettingCompressionParams() {
        if (clientSideEncrypt && queryParams.containsKey("compressionType")) {
            log.warn("Since clientSideEncrypt is true, the param compressionType of producer will be ignored");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

}
