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
package org.apache.pulsar.websocket.proxy;

import static org.testng.Assert.assertTrue;
import static org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import static org.apache.pulsar.websocket.proxy.WssClientSideEncryptUtils.EncryptedPayloadAndParam;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

@Slf4j
@WebSocket(maxTextMessageSize = 64 * 1024)
public class ClientSideEncryptionWssProducer extends WebSocketAdapter implements Closeable {

    private Session session;
    private volatile CompletableFuture<MessageIdData> sendFuture;
    private final ScheduledExecutorService executor;
    private final CryptoKeyReader cryptoKeyReader;
    private final String topicName;
    private final String producerName;
    private final String webSocketProxyHost;
    private final int webSocketProxyPort;
    private final String keyName;
    private WebSocketClient wssClient;
    private final MessageCryptoBc msgCrypto;

    public ClientSideEncryptionWssProducer(String webSocketProxyHost, int webSocketProxyPort, String topicName,
                                           String producerName, CryptoKeyReader cryptoKeyReader, String keyName,
                                           ScheduledExecutorService executor) {
        this.webSocketProxyHost = webSocketProxyHost;
        this.webSocketProxyPort = webSocketProxyPort;
        this.topicName = topicName;
        this.producerName = producerName;
        this.msgCrypto = new MessageCryptoBc("[" + topicName + "] [" + producerName + "]", true);
        this.cryptoKeyReader = cryptoKeyReader;
        this.keyName = keyName;
        this.executor = executor;
    }

    public void start() throws Exception {
        wssClient = new WebSocketClient();
        wssClient.start();
        session = wssClient.connect(this, buildConnectURL(), new ClientUpgradeRequest()).get();
        assertTrue(session.isOpen());
    }

    private URI buildConnectURL() throws PulsarClientException.CryptoException {
        final String protocolAndHostPort = "ws://" + webSocketProxyHost + ":" + webSocketProxyPort;

        // Encode encrypted public key data.
        final byte[] keyValue = WssClientSideEncryptUtils.calculateEncryptedKeyValue(msgCrypto, cryptoKeyReader,
                keyName);
        EncryptionKey encryptionKey = new EncryptionKey();
        encryptionKey.setKeyValue(keyValue);
        encryptionKey.setMetadata(cryptoKeyReader.getPublicKey(keyName, Collections.emptyMap()).getMetadata());
        Map<String, EncryptionKey> encryptionKeyMap = new HashMap<>();
        encryptionKeyMap.put(keyName, encryptionKey);

        final String encryptionKeys =
                WssClientSideEncryptUtils.toJSONAndBase64AndUrlEncode(encryptionKeyMap);

        // Build the URL for producer.
        final StringBuilder producerUrL = new StringBuilder(protocolAndHostPort)
                .append("/ws/v2/producer/persistent/")
                .append(topicName)
                .append("?")
                .append("encryptionKeys=").append(encryptionKeys);
        return URI.create(producerUrL.toString());
    }

    public synchronized MessageIdData sendMessage(ProducerMessage msg) throws Exception {
        if (sendFuture != null && !sendFuture.isDone() && !sendFuture.isCancelled()) {
            throw new IllegalArgumentException("There is a message still in sending.");
        }
        if (msg.payload == null) {
            throw new IllegalArgumentException("Null value message is not supported.");
        }
        // Compression.
        byte[] unCompressedPayload = msg.payload.getBytes(StandardCharsets.UTF_8);
        byte[] compressedPayload = WssClientSideEncryptUtils.compressionIfNeeded(msg.compressionType,
                unCompressedPayload);
        if (msg.compressionType != null && !CompressionType.NONE.equals(msg.compressionType)) {
            msg.uncompressedMessageSize = unCompressedPayload.length;
        }
        // Encrypt.
        EncryptedPayloadAndParam encryptedPayloadAndParam = WssClientSideEncryptUtils.encryptPayload(
                cryptoKeyReader, msgCrypto, compressedPayload, keyName);
        msg.payload = encryptedPayloadAndParam.encryptedPayload;
        msg.encryptionParam = encryptedPayloadAndParam.encryptionParam;
        // Do send.
        sendFuture = new CompletableFuture<>();
        String jsonMsg = ObjectMapperFactory.getMapper().writer().writeValueAsString(msg);
        this.session.getRemote().sendString(jsonMsg);
        // Wait for response.
        executor.schedule(() -> {
            synchronized (ClientSideEncryptionWssProducer.this) {
                if (!sendFuture.isDone() && !sendFuture.isCancelled()) {
                    sendFuture.completeExceptionally(new TimeoutException("Send timeout"));
                }
            }
        }, 50, TimeUnit.SECONDS);
        return sendFuture.get();
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        log.info("Connection closed: {} - {}", statusCode, reason);
        this.session = null;
        if (!sendFuture.isDone() && !sendFuture.isCancelled()) {
            sendFuture.completeExceptionally(new RuntimeException("Connection was closed"));
        }
    }

    @Override
    public void onWebSocketConnect(Session session) {
        log.info("Got connect: {}", session);
        this.session = session;
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        log.error("Received an error", cause);
    }

    @Override
    public void onWebSocketText(String text) {
        try {
            ResponseOfSend responseOfSend =
                    ObjectMapperFactory.getMapper().reader().readValue(text, ResponseOfSend.class);
            if (responseOfSend.getErrorCode() != 0 || responseOfSend.getErrorMsg() != null) {
                sendFuture.completeExceptionally(new RuntimeException(text));
            } else {
                byte[] bytes = Base64.getDecoder().decode(responseOfSend.getMessageId());
                MessageIdData messageIdData = new MessageIdData();
                messageIdData.parseFrom(bytes);
                sendFuture.complete(messageIdData);
            }
        } catch (Exception ex) {
            log.error("Could not extract the response payload: {}", text);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            wssClient.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Data
    public static class ResponseOfSend {
        private String result;
        private String messageId;
        private String errorMsg;
        private int errorCode = -1;
        private int schemaVersion;
    }
}
