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
package org.apache.pulsar.proxy.socket.client;

import static java.util.Base64.getEncoder;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleTestProducerSocket {
    public static Recorder recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);

    private final CountDownLatch closeLatch;
    private volatile Session session;
    private ConcurrentHashMap<String, Long> startTimeMap = new ConcurrentHashMap<>();
    private static final String CONTEXT = "context";

    public SimpleTestProducerSocket() {
        this.closeLatch = new CountDownLatch(2);
    }

    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        return this.closeLatch.await(duration, unit);
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.info("Connection closed: {} - {}", statusCode, reason);
        this.session.close();
        this.closeLatch.countDown();
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws InterruptedException, IOException, JsonParseException {
        log.info("Got conneceted to the proxy");
        this.session = session;
    }

    @OnWebSocketMessage
    public void onMessage(String msg) throws JsonParseException {
        JsonObject json = new Gson().fromJson(msg, JsonObject.class);
        long endTimeNs = System.nanoTime();
        long startTime = endTimeNs;
        if (startTimeMap.get(json.get(CONTEXT).getAsString()) != null) {
            startTime = startTimeMap.get(json.get(CONTEXT).getAsString());
        }
        long latencyNs = endTimeNs - startTime;
        recorder.recordValue(NANOSECONDS.toMicros(latencyNs));
    }

    public RemoteEndpoint getRemote() {
        return this.session.getRemote();
    }

    public Session getSession() {
        return this.session;
    }

    public void sendMsg(String context, byte[] payloadData)
            throws IOException, JsonParseException, InterruptedException, ExecutionException {
        String message = getEncoder().encodeToString(payloadData);
        String timeStamp = "{\"payload\": \"" + message + "\",\"context\": \"" + context + "\"}";
        String sampleMsg = new Gson().fromJson(timeStamp, JsonObject.class).toString();
        if (this.session != null && this.session.isOpen() && this.session.getRemote() != null) {
            startTimeMap.put(context, System.nanoTime());
            this.session.getRemote().sendStringByFuture(sampleMsg).get();
        } else {
            log.error("Session is already closed");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SimpleTestProducerSocket.class);

}
