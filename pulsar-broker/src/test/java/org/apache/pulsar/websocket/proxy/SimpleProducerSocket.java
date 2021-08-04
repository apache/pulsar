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

import static org.apache.pulsar.broker.admin.AdminResource.jsonMapper;

import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleProducerSocket {

    private final CountDownLatch closeLatch;
    private Session session;
    private final ArrayList<String> producerBuffer;
    private final int messagesToSendWhenConnected;

    public SimpleProducerSocket() {
        this(10);
    }

    public SimpleProducerSocket(int messagesToSendWhenConnected) {
        this.closeLatch = new CountDownLatch(1);
        this.producerBuffer = new ArrayList<>();
        this.messagesToSendWhenConnected = messagesToSendWhenConnected;
    }

    private static String getTestJsonPayload(int index) throws JsonProcessingException {
        ProducerMessage msg = new ProducerMessage();
        msg.payload = Base64.getEncoder().encodeToString(("test" + index).getBytes());
        msg.key = Integer.toString(index);
        return jsonMapper().writeValueAsString(msg);
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
    public void onConnect(Session session) throws Exception {
        log.info("Got connect: {}", session);
        this.session = session;
        sendMessage(this.messagesToSendWhenConnected);
    }

    public void sendMessage(int totalMsgs) throws Exception {
        for (int i = 0; i < totalMsgs; i++) {
            this.session.getRemote().sendString(getTestJsonPayload(i));
        }
    }

    @OnWebSocketMessage
    public synchronized void onMessage(String msg) throws JsonParseException {
        JsonObject ack = new Gson().fromJson(msg, JsonObject.class);
        producerBuffer.add(ack.get("messageId").getAsString());
    }

    public RemoteEndpoint getRemote() {
        return this.session.getRemote();
    }

    public Session getSession() {
        return this.session;
    }

    public synchronized ArrayList<String> getBuffer() {
        return producerBuffer;
    }

    private static final Logger log = LoggerFactory.getLogger(SimpleProducerSocket.class);

}
