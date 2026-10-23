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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.Authentication;
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
 * Client-agnostic part shared by the consume and read commands of pulsar-client: the connection
 * details the WebSocket path needs, the byte rendering, and the WebSocket consumer socket itself.
 * The message rendering is client-specific and lives in {@link V5MessageSupport} /
 * {@link V4MessageSupport}.
 */
public abstract class AbstractCmdConsume extends AbstractCmd {

    protected static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    protected static final String MESSAGE_BOUNDARY = "----- got message -----";

    protected Authentication authentication;
    protected String serviceURL;

    public AbstractCmdConsume() {
        // Do nothing
    }

    /** Record the client-generation-independent configuration. */
    protected void updateSharedConfig(Authentication authentication, String serviceURL) {
        this.authentication = authentication;
        this.serviceURL = serviceURL;
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

    /** WebSocket client socket used by the {@code ws://} consume and read paths. */
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
