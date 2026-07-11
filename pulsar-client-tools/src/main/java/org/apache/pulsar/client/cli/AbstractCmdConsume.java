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
package org.apache.pulsar.client.cli;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.EncryptionKey;
import org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Field;
import org.apache.pulsar.client.api.v5.schema.GenericRecord;
import org.apache.pulsar.client.api.v5.schema.KeyValue;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * common part of consume command and read command of pulsar-client.
 *
 */
public abstract class AbstractCmdConsume extends AbstractCmd {

    protected static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    protected static final String MESSAGE_BOUNDARY = "----- got message -----";

    protected PulsarClientBuilder clientBuilder;
    protected Authentication authentication;
    protected String serviceURL;

    public AbstractCmdConsume() {
        // Do nothing
    }

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(PulsarClientBuilder clientBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = clientBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
    }

    /**
     * Interprets the message to create a string representation.
     *
     * @param message
     *            The message to interpret
     * @param displayHex
     *            Whether to display BytesMessages in hexdump style, ignored for simple text messages
     * @return String representation of the message
     */
    protected String interpretMessage(Message<?> message, boolean displayHex, boolean printMetadata)
            throws IOException {
        StringBuilder sb = new StringBuilder();

        String properties = Arrays.toString(message.properties().entrySet().toArray());

        Object value = message.value();
        String data;
        if (value == null) {
            data = "null";
        } else if (value instanceof byte[]) {
            data = interpretByteArray(displayHex, (byte[]) value);
        } else if (value instanceof GenericRecord) {
            data = genericObjectToMap((GenericRecord) value, displayHex).toString();
        } else {
            data = value.toString();
        }

        sb.append("publishTime:[").append(message.publishTime()).append("], ");
        sb.append("eventTime:[").append(message.eventTime().orElse(null)).append("], ");
        sb.append("key:[").append(message.key().orElse(null)).append("], ");
        if (!properties.isEmpty()) {
            sb.append("properties:").append(properties).append(", ");
        }
        sb.append("content:").append(data);

        if (printMetadata) {
            sb.append(", ").append("message-id:").append(message.id());
            sb.append(", ").append("producer-name:").append(message.producerName().orElse(null));
            sb.append(", ").append("sequence-id:").append(message.sequenceId());
            sb.append(", ").append("replicated-from:").append(message.replicatedFrom().orElse(null));
            sb.append(", ").append("redelivery-count:").append(message.redeliveryCount());
        }

        return sb.toString();
    }

    protected static String interpretByteArray(boolean displayHex, byte[] msgData) throws IOException {
        if (!displayHex) {
            return new String(msgData);
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HexDump.dump(msgData, 0, out, 0);
            return out.toString();
        }
    }

    /**
     * Render an {@code auto_consume} {@link GenericRecord} value into a {@link Map} for display.
     * The shape is dispatched on the record's runtime schema type: structured records become a map
     * of their fields, key/value records become a {@code {key, value}} map, and primitives are
     * wrapped in a single {@code value} entry.
     */
    protected static Map<String, Object> genericObjectToMap(GenericRecord value, boolean displayHex)
            throws IOException {
        switch (value.schemaType()) {
            case AVRO:
            case JSON:
            case PROTOBUF_NATIVE:
                return genericRecordToMap(value, displayHex);
            case KEY_VALUE:
                return keyValueToMap((KeyValue<?, ?>) value.nativeObject(), displayHex);
            default:
                return primitiveValueToMap(value.nativeObject(), displayHex);
        }
    }

    protected static Map<String, Object> keyValueToMap(KeyValue<?, ?> value, boolean displayHex)
            throws IOException {
        if (value == null) {
            return Map.of("value", "NULL");
        }
        return Map.of("key", primitiveValueToMap(value.key(), displayHex),
                "value", primitiveValueToMap(value.value(), displayHex));
    }

    protected static Map<String, Object> primitiveValueToMap(Object value, boolean displayHex)
            throws IOException {
        if (value == null) {
            return Map.of("value", "NULL");
        }
        if (value instanceof GenericRecord) {
            return genericObjectToMap((GenericRecord) value, displayHex);
        }
        if (value instanceof byte[]) {
            value = interpretByteArray(displayHex, (byte[]) value);
        }
        return Map.of("value", value.toString(), "type", value.getClass());
    }

    protected static Map<String, Object> genericRecordToMap(GenericRecord value, boolean displayHex)
            throws IOException {
        Map<String, Object> res = new HashMap<>();
        for (Field f : value.fields()) {
            Object fieldValue = value.field(f);
            if (fieldValue instanceof GenericRecord) {
                fieldValue = genericRecordToMap((GenericRecord) fieldValue, displayHex);
            } else if (fieldValue == null) {
                fieldValue = "NULL";
            } else if (fieldValue instanceof byte[]) {
                fieldValue = interpretByteArray(displayHex, (byte[]) fieldValue);
            }
            res.put(f.name(), fieldValue);
        }
        return res;
    }

    /**
     * Build a consumer-side decryption policy from a {@code file://} key URI, mirroring the v4
     * {@code defaultCryptoKeyReader(uri)} semantics: the private key is loaded once and returned
     * for any key name. (The producer's logical key name travels in the message metadata, so a
     * name-keyed provider would not resolve it; the CLI's file-based flow has a single key.)
     */
    protected static ConsumerEncryptionPolicy buildFileDecryptionPolicy(
            String keyUri, ConsumerCryptoFailureAction failureAction) {
        final byte[] keyBytes;
        try {
            keyBytes = Files.readAllBytes(fileUriToPath(keyUri));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read decryption key from " + keyUri, e);
        }
        PrivateKeyProvider provider = (keyName, metadata) ->
                CompletableFuture.completedFuture(EncryptionKey.of(keyBytes));
        return ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(provider)
                .failureAction(failureAction)
                .build();
    }

    @WebSocket
    @CustomLog
    public static class ConsumerSocket {
        private static final String X_PULSAR_MESSAGE_ID = "messageId";
        private final CountDownLatch closeLatch;
        private Session session;
        private CompletableFuture<Void> connected;
        final BlockingQueue<String> incomingMessages;

        public ConsumerSocket(CompletableFuture<Void> connected) {
            this.closeLatch = new CountDownLatch(1);
            this.connected = connected;
            this.incomingMessages = new GrowableArrayBlockingQueue<>();
        }

        public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
            return this.closeLatch.await(duration, unit);
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason) {
            log.info().attr("statusCode", statusCode).attr("reason", reason)
                    .log("Connection closed");
            this.session = null;
            this.closeLatch.countDown();
        }

        @OnWebSocketOpen
        public void onConnect(Session session) throws InterruptedException {
            log.info().attr("session", session).log("Got connect");
            this.session = session;
            this.connected.complete(null);
        }

        @OnWebSocketMessage
        public synchronized void onMessage(String msg) throws Exception {
            JsonObject message = new Gson().fromJson(msg, JsonObject.class);
            JsonObject ack = new JsonObject();
            String messageId = message.get(X_PULSAR_MESSAGE_ID).getAsString();
            ack.add("messageId", new JsonPrimitive(messageId));
            // Acking the proxy
            this.getSession().sendText(ack.toString(), Callback.NOOP);
            this.incomingMessages.put(msg);
        }

        public String receive(long timeout, TimeUnit unit) throws Exception {
            return incomingMessages.poll(timeout, unit);
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            this.session.close();
        }

    }

}
