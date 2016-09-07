/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.websocket.proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Base64.getEncoder;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleProducerSocket {

    private final static String message = getEncoder().encodeToString("test".getBytes());
    private final static String testJsonString = "{\"content\": \"" + message
            + "\", \"properties\" : {\"test\" :\"test\"}}";
    private final CountDownLatch closeLatch;
    private Session session;
    private ArrayList<String> producerBuffer;

    public SimpleProducerSocket() {
        this.closeLatch = new CountDownLatch(1);
        producerBuffer = new ArrayList<String>();
    }

    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        return this.closeLatch.await(duration, unit);
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.info("Connection closed: %d - %s%n", statusCode, reason);
        this.session = null;
        this.closeLatch.countDown();
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws InterruptedException, IOException, JSONException {
        log.info("Got connect: %s%n", session);
        this.session = session;
        String sampleMsg = new JSONObject(testJsonString).toString();
        for (int i = 0; i < 10; i++) {
            this.session.getRemote().sendString(sampleMsg);
        }

    }

    @OnWebSocketMessage
    public void onMessage(String msg) throws JSONException {
        JSONObject ack = new JSONObject(msg);
        producerBuffer.add((String) ack.get("messageId"));
    }

    public RemoteEndpoint getRemote() {
        return this.session.getRemote();
    }

    public Session getSession() {
        return this.session;
    }

    public ArrayList<String> getBuffer() {
        return producerBuffer;
    }

    private static final Logger log = LoggerFactory.getLogger(SimpleProducerSocket.class);

}
