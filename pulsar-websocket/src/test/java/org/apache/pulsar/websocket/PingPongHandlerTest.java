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
package org.apache.pulsar.websocket;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WebSocketPingPongListener;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PingPongHandlerTest {

    private static Server server;

    private static final WebExecutorThreadPool executor = new WebExecutorThreadPool(6, "pulsar-websocket-web-test");

    @BeforeClass
    public static void setup() throws Exception {
        server = new Server(executor);
        List<ServerConnector> connectors = new ArrayList<>();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8080);
        connectors.add(connector);
        connectors.forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));

        WebSocketService service = mock(WebSocketService.class);
        ServiceConfiguration config = mock(ServiceConfiguration.class);

        when(service.getConfig()).thenReturn(config);
        when(config.getWebSocketMaxTextFrameSize()).thenReturn(1048576);
        when(config.getWebSocketSessionIdleTimeoutMillis()).thenReturn(300000);

        ServletHolder servletHolder = new ServletHolder("ws-events", new WebSocketPingPongServlet(service));
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(WebSocketPingPongServlet.SERVLET_PATH);
        context.addServlet(servletHolder, "/*");
        server.setHandler(context);
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass(alwaysRun = true)
    public static void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        executor.stop();
    }

    @Test
    public void testPingPong() throws Exception {
        HttpClient httpClient = new HttpClient();
        WebSocketClient webSocketClient = new WebSocketClient(httpClient);
        webSocketClient.start();
        MyWebSocket myWebSocket = new MyWebSocket();
        String webSocketUri = "ws://localhost:8080/ws/pingpong";
        Future<Session> sessionFuture = webSocketClient.connect(myWebSocket, URI.create(webSocketUri));
        sessionFuture.get().getRemote().sendPing(ByteBuffer.wrap("test".getBytes()));
        assertTrue(myWebSocket.getResponse().contains("test"));
    }

    @WebSocket
    public static class MyWebSocket extends WebSocketAdapter implements WebSocketPingPongListener {

        ArrayBlockingQueue<String> incomingMessages = new ArrayBlockingQueue<>(10);

        @Override
        public void onWebSocketClose(int i, String s) {
        }

        @Override
        public void onWebSocketConnect(Session session) {
        }

        @Override
        public void onWebSocketError(Throwable throwable) {
        }

        @Override
        public void onWebSocketPing(ByteBuffer payload) {
        }

        @Override
        public void onWebSocketPong(ByteBuffer payload) {
            incomingMessages.add(BufferUtil.toDetailString(payload));
        }

        public String getResponse() throws InterruptedException {
            return incomingMessages.take();
        }
    }
}
