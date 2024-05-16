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
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

@Slf4j
@WebSocket(maxTextMessageSize = 64 * 1024)
public class ClientSideEncryptionWssConsumer extends WebSocketAdapter implements Closeable {

    private Session session;
    private final CryptoKeyReader cryptoKeyReader;
    private final String topicName;
    private final String subscriptionName;
    private final SubscriptionType subscriptionType;
    private final String webSocketProxyHost;
    private final int webSocketProxyPort;
    private WebSocketClient wssClient;
    private final MessageCryptoBc msgCrypto;
    private final LinkedBlockingQueue<ConsumerMessage> incomingMessages = new LinkedBlockingQueue<>();

    public ClientSideEncryptionWssConsumer(String webSocketProxyHost, int webSocketProxyPort, String topicName,
                                           String subscriptionName, SubscriptionType subscriptionType,
                                           CryptoKeyReader cryptoKeyReader) {
        this.webSocketProxyHost = webSocketProxyHost;
        this.webSocketProxyPort = webSocketProxyPort;
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.subscriptionType = subscriptionType;
        this.msgCrypto = new MessageCryptoBc("[" + topicName + "] [" + subscriptionName + "]", false);
        this.cryptoKeyReader = cryptoKeyReader;
    }

    public void start() throws Exception {
        wssClient = new WebSocketClient();
        wssClient.start();
        session = wssClient.connect(this, buildConnectURL(), new ClientUpgradeRequest()).get();
        assertTrue(session.isOpen());
    }

    private URI buildConnectURL() throws PulsarClientException.CryptoException {
        final String protocolAndHostPort = "ws://" + webSocketProxyHost + ":" + webSocketProxyPort;

        // Build the URL for producer.
        final StringBuilder consumerUri = new StringBuilder(protocolAndHostPort)
                .append("/ws/v2/consumer/persistent/")
                .append(topicName)
                .append("/")
                .append(subscriptionName)
                .append("?")
                .append("subscriptionType=").append(subscriptionType.toString())
                .append("&").append("cryptoFailureAction=CONSUME");
        return URI.create(consumerUri.toString());
    }

    public synchronized ConsumerMessage receive(int timeout, TimeUnit unit) throws Exception {
        ConsumerMessage msg = incomingMessages.poll(timeout, unit);
        return msg;
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        log.info("Connection closed: {} - {}", statusCode, reason);
        this.session = null;
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
            ConsumerMessage msg =
                    ObjectMapperFactory.getMapper().reader().readValue(text, ConsumerMessage.class);
            if (msg.messageId == null) {
                log.error("Consumer[{}-{}] Could not extract the response payload: {}", topicName, subscriptionName,
                        text);
                return;
            }
            // Decrypt.
            byte[] decryptedPayload = WssClientSideEncryptUtils.decryptMsgPayload(msg.payload, msg.encryptionContext,
                    cryptoKeyReader, msgCrypto);
            // Un-compression if needed.
            byte[] unCompressedPayload = WssClientSideEncryptUtils.unCompressionIfNeeded(decryptedPayload,
                    msg.encryptionContext);
            // Extract batch messages if needed.
            if (msg.encryptionContext.getBatchSize().isPresent()) {
                List<ConsumerMessage> singleMsgs = WssClientSideEncryptUtils.extractBatchMessagesIfNeeded(
                        unCompressedPayload, msg.encryptionContext);
                for (ConsumerMessage singleMsg : singleMsgs) {
                    incomingMessages.add(singleMsg);
                }
            } else {
                msg.payload = new String(unCompressedPayload, StandardCharsets.UTF_8);
                incomingMessages.add(msg);
            }
        } catch (Exception ex) {
            log.error("Consumer[{}-{}] Could not extract the response payload: {}", topicName, subscriptionName, text);
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
}
