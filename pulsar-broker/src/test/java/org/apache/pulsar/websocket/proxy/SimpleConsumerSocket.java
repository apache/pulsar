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
package org.apache.pulsar.websocket.proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleConsumerSocket {
    private static final String X_PULSAR_MESSAGE_ID = "messageId";
    private final CountDownLatch closeLatch;
    private Session session;
    private final ArrayList<String> consumerBuffer;
    final ArrayList<JsonObject> messages;
    private final AtomicInteger receivedMessages = new AtomicInteger();
    // Custom message handler to override standard message processing, if it's needed
    private SimpleConsumerMessageHandler customMessageHandler;

    public SimpleConsumerSocket() {
        this.closeLatch = new CountDownLatch(1);
        consumerBuffer = new ArrayList<>();
        this.messages = new ArrayList<>();
    }

    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        return this.closeLatch.await(duration, unit);
    }

    public void setMessageHandler(SimpleConsumerMessageHandler handler) {
        customMessageHandler = handler;
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
        log.debug("Got connected: {}", session);
    }

    @OnWebSocketMessage
    public synchronized void onMessage(String msg) throws JsonParseException, IOException {
        receivedMessages.incrementAndGet();
        JsonObject message = new Gson().fromJson(msg, JsonObject.class);
        this.messages.add(message);
        if (message.get(X_PULSAR_MESSAGE_ID) != null) {
            String messageId = message.get(X_PULSAR_MESSAGE_ID).getAsString();
            consumerBuffer.add(messageId);
            if (customMessageHandler != null) {
                this.getRemote().sendString(customMessageHandler.handle(messageId, message));
            } else {
                JsonObject ack = new JsonObject();
                ack.add("messageId", new JsonPrimitive(messageId));
                // Acking the proxy
                this.getRemote().sendString(ack.toString());
            }
        } else {
            consumerBuffer.add(message.toString());
        }
    }

    public void sendPermits(int nbPermits) throws IOException {
        JsonObject permitMessage = new JsonObject();
        permitMessage.add("type", new JsonPrimitive("permit"));
        permitMessage.add("permitMessages", new JsonPrimitive(nbPermits));
        this.getRemote().sendString(permitMessage.toString());
    }

    public void unsubscribe() throws IOException {
        JsonObject message = new JsonObject();
        message.add("type", new JsonPrimitive("unsubscribe"));
        this.getRemote().sendString(message.toString());
    }

    public void isEndOfTopic() throws IOException {
        JsonObject message = new JsonObject();
        message.add("type", new JsonPrimitive("isEndOfTopic"));
        this.getRemote().sendString(message.toString());
    }

    public RemoteEndpoint getRemote() {
        return this.session.getRemote();
    }

    public Session getSession() {
        return this.session;
    }

    public synchronized ArrayList<String> getBuffer() {
        return consumerBuffer;
    }

    public int getReceivedMessagesCount() {
        return receivedMessages.get();
    }

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerSocket.class);

}