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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.internal.PulsarClientImplementationBinding.getBytes;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pulsar-client consume command implementation.
 *
 */
@Parameters(commandDescription = "Consume messages from a specified topic")
public class CmdConsume {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final String MESSAGE_BOUNDARY = "----- got message -----";

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-m", "--subscription-mode" }, description = "Subscription mode.")
    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @Parameter(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "-n",
            "--num-messages" }, description = "Number of messages to consume, 0 means to consume forever.")
    private int numMessagesToConsume = 1;

    @Parameter(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Parameter(names = { "--hide-content" }, description = "Do not write the message to console.")
    private boolean hideContent = false;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to consume, "
            + "value 0 means to consume messages as fast as possible.")
    private double consumeRate = 0;

    @Parameter(names = { "--regex" }, description = "Indicate the topic name is a regex pattern")
    private boolean isRegex = false;

    @Parameter(names = {"-q", "--queue-size"}, description = "Consumer receiver queue size.")
    private int receiverQueueSize = 0;

    @Parameter(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
    private int maxPendingChunkedMessage = 0;

    @Parameter(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Parameter(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of private key to decrypt payload, for example "
                    + "file:///path/to/private.key or data:application/x-pem-file;base64,*****")
    private String encKeyValue;

    @Parameter(names = { "-st", "--schema-type"},
            description = "Set a schema type on the consumer, it can be 'bytes' or 'auto_consume'")
    private String schematype = "bytes";

    @Parameter(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = 1)
    private boolean poolMessages = true;

    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;

    public CmdConsume() {
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
    private String interpretMessage(Message<?> message, boolean displayHex) throws IOException {
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

    private static String interpretByteArray(boolean displayHex, byte[] msgData) throws IOException {
        String data;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!displayHex) {
            return new String(msgData);
        } else {
            HexDump.dump(msgData, 0, out, 0);
            return out.toString();
        }
    }

    private static Map<String, Object> genericObjectToMap(GenericObject value, boolean displayHex) throws IOException {
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

    private static Map<String, Object> keyValueToMap(KeyValue value, boolean displayHex) throws IOException {
        if (value == null) {
            return ImmutableMap.of("value", "NULL");
        }
        return ImmutableMap.of("key", primitiveValueToMap(value.getKey(), displayHex),
                "value", primitiveValueToMap(value.getValue(), displayHex));
    }

    private static Map<String, Object> primitiveValueToMap(Object value, boolean displayHex) throws IOException {
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

    private static Map<String, Object> genericRecordToMap(GenericRecord value, boolean displayHex) throws IOException {
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

    /**
     * Run the consume command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws PulsarClientException, IOException {
        if (mainOptions.size() != 1) {
            throw (new ParameterException("Please provide one and only one topic name."));
        }
        if (this.subscriptionName == null || this.subscriptionName.isEmpty()) {
            throw (new ParameterException("Subscription name is not provided."));
        }
        if (this.numMessagesToConsume < 0) {
            throw (new ParameterException("Number of messages should be zero or positive."));
        }

        String topic = this.mainOptions.get(0);

        if (this.serviceURL.startsWith("ws")) {
            return consumeFromWebSocket(topic);
        } else {
            return consume(topic);
        }
    }

    private int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try {
            ConsumerBuilder<?> builder;
            PulsarClient client = clientBuilder.build();
            Schema<?> schema = poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(schematype)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(schematype)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume");
            }
            builder = client.newConsumer(schema)
                    .subscriptionName(this.subscriptionName)
                    .subscriptionType(subscriptionType)
                    .subscriptionMode(subscriptionMode)
                    .subscriptionInitialPosition(subscriptionInitialPosition)
                    .poolMessages(poolMessages);

            if (isRegex) {
                builder.topicsPattern(Pattern.compile(topic));
            } else {
                builder.topic(topic);
            }

            if (this.maxPendingChunkedMessage > 0) {
                builder.maxPendingChunkedMessage(this.maxPendingChunkedMessage);
            }
            if (this.receiverQueueSize > 0) {
                builder.receiverQueueSize(this.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull);

            if (isNotBlank(this.encKeyValue)) {
                builder.defaultCryptoKeyReader(this.encKeyValue);
            }

            Consumer<?> consumer = builder.subscribe();
            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                if (limiter != null) {
                    limiter.acquire();
                }

                Message<?> msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.debug("No message to consume after waiting for 5 seconds.");
                } else {
                    try {
                        numMessagesConsumed += 1;
                        if (!hideContent) {
                            System.out.println(MESSAGE_BOUNDARY);
                            String output = this.interpretMessage(msg, displayHex);
                            System.out.println(output);
                        } else if (numMessagesConsumed % 1000 == 0) {
                            System.out.println("Received " + numMessagesConsumed + " messages");
                        }
                        consumer.acknowledge(msg);
                    } finally {
                        msg.release();
                    }
                }
            }
            client.close();
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;

    }

    @SuppressWarnings("deprecation")
    @VisibleForTesting
    public String getWebSocketConsumeUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic;
        if (topicName.isV2()) {
            wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                    topicName.getNamespacePortion(), topicName.getLocalName());
        } else {
            wsTopic = String.format("%s/%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                    topicName.getCluster(), topicName.getNamespacePortion(), topicName.getLocalName());
        }

        String uriFormat = "%s/ws" + (topicName.isV2() ? "/v2/" : "/")
                + "consumer/%s/%s?subscriptionType=%s&subscriptionMode=%s";
        return String.format(uriFormat, serviceURLWithoutTrailingSlash, wsTopic, subscriptionName,
                subscriptionType.toString(), subscriptionMode.toString());
    }

    @SuppressWarnings("deprecation")
    private int consumeFromWebSocket(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        URI consumerUri = URI.create(getWebSocketConsumeUri(topic));

        WebSocketClient consumeClient = new WebSocketClient(new SslContextFactory(true));
        ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForHttp()) {
                    for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                        consumeRequest.setHeader(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Authentication plugin error: " + e.getMessage());
            return -1;
        }
        CompletableFuture<Void> connected = new CompletableFuture<>();
        ConsumerSocket consumerSocket = new ConsumerSocket(connected);
        try {
            consumeClient.start();
        } catch (Exception e) {
            LOG.error("Failed to start websocket-client", e);
            return -1;
        }

        try {
            LOG.info("Trying to create websocket session..{}", consumerUri);
            consumeClient.connect(consumerSocket, consumerUri, consumeRequest);
            connected.get();
        } catch (Exception e) {
            LOG.error("Failed to create web-socket session", e);
            return -1;
        }

        try {
            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                if (limiter != null) {
                    limiter.acquire();
                }
                String msg = consumerSocket.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.debug("No message to consume after waiting for 5 seconds.");
                } else {
                    try {
                        String output = interpretByteArray(displayHex, Base64.getDecoder().decode(msg));
                        System.out.println(output); // print decode
                    } catch (Exception e) {
                        System.out.println(msg);
                    }
                    numMessagesConsumed += 1;
                }
            }
            consumerSocket.awaitClose(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;
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
