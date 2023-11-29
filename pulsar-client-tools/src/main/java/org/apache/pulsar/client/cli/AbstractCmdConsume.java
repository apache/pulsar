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

import static org.apache.pulsar.client.internal.PulsarClientImplementationBinding.getBytes;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * common part of consume command and read command of pulsar-client.
 *
 */
public abstract class AbstractCmdConsume {

    protected static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    protected static final String MESSAGE_BOUNDARY = "----- got message -----";

    protected ClientBuilder clientBuilder;
    protected Authentication authentication;
    protected String serviceURL;

    public AbstractCmdConsume() {
        // Do nothing
    }

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(ClientBuilder clientBuilder, Authentication authentication, String serviceURL) {
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
    protected String interpretMessage(Message<?> message, boolean displayHex) throws IOException {
        StringBuilder sb = new StringBuilder();

        String properties = Arrays.toString(message.getProperties().entrySet().toArray());

        String data;
        Object value = message.getValue();
        if (value == null) {
            data = "null";
        } else if (value instanceof byte[]) {
            byte[] msgData = (byte[]) value;
            data = interpretByteArray(displayHex, msgData);
        } else if (value instanceof GenericObject) {
            Map<String, Object> asMap = genericObjectToMap((GenericObject) value, displayHex);
            data = asMap.toString();
        } else if (value instanceof ByteBuffer) {
            data = new String(getBytes((ByteBuffer) value));
        } else {
            data = value.toString();
        }

        String key = null;
        if (message.hasKey()) {
            key = message.getKey();
        }

        sb.append("key:[").append(key).append("], ");
        if (!properties.isEmpty()) {
            sb.append("properties:").append(properties).append(", ");
        }
        sb.append("content:").append(data);

        return sb.toString();
    }

    protected static String interpretByteArray(boolean displayHex, byte[] msgData) throws IOException {
        String data;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!displayHex) {
            return new String(msgData);
        } else {
            HexDump.dump(msgData, 0, out, 0);
            return out.toString();
        }
    }

    protected static Map<String, Object> genericObjectToMap(GenericObject value, boolean displayHex)
            throws IOException {
        switch (value.getSchemaType()) {
            case AVRO:
            case JSON:
            case PROTOBUF_NATIVE:
                    return genericRecordToMap((GenericRecord) value, displayHex);
            case KEY_VALUE:
                    return keyValueToMap((KeyValue) value.getNativeObject(), displayHex);
            default:
                return primitiveValueToMap(value.getNativeObject(), displayHex);
        }
    }

    protected static Map<String, Object> keyValueToMap(KeyValue value, boolean displayHex) throws IOException {
        if (value == null) {
            return ImmutableMap.of("value", "NULL");
        }
        return ImmutableMap.of("key", primitiveValueToMap(value.getKey(), displayHex),
                "value", primitiveValueToMap(value.getValue(), displayHex));
    }

    protected static Map<String, Object> primitiveValueToMap(Object value, boolean displayHex) throws IOException {
        if (value == null) {
            return ImmutableMap.of("value", "NULL");
        }
        if (value instanceof GenericObject) {
            return genericObjectToMap((GenericObject) value, displayHex);
        }
        if (value instanceof byte[]) {
            value = interpretByteArray(displayHex, (byte[]) value);
        }
        return ImmutableMap.of("value", value.toString(), "type", value.getClass());
    }

    protected static Map<String, Object> genericRecordToMap(GenericRecord value, boolean displayHex)
            throws IOException {
        Map<String, Object> res = new HashMap<>();
        for (Field f : value.getFields()) {
            Object fieldValue = value.getField(f);
            if (fieldValue instanceof GenericRecord) {
                fieldValue = genericRecordToMap((GenericRecord) fieldValue, displayHex);
            } else if (fieldValue == null) {
                fieldValue =  "NULL";
            } else if (fieldValue instanceof byte[]) {
                fieldValue = interpretByteArray(displayHex, (byte[]) fieldValue);
            }
            res.put(f.getName(), fieldValue);
        }
        return res;
    }

    @WebSocket(maxTextMessageSize = 64 * 1024)
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
            log.info("Connection closed: {} - {}", statusCode, reason);
            this.session = null;
            this.closeLatch.countDown();
        }

        @OnWebSocketConnect
        public void onConnect(Session session) throws InterruptedException {
            log.info("Got connect: {}", session);
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
            this.getRemote().sendString(ack.toString());
            this.incomingMessages.put(msg);
        }

        public String receive(long timeout, TimeUnit unit) throws Exception {
            return incomingMessages.poll(timeout, unit);
        }

        public RemoteEndpoint getRemote() {
            return this.session.getRemote();
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            this.session.close();
        }

        private static final Logger log = LoggerFactory.getLogger(ConsumerSocket.class);
    }

}
