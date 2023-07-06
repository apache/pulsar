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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
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
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test to ensure {@link AbstractWebSocketHandler} has ping/pong support
 */
public class PingPongSupportTest {

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

        ServletHolder servletHolder = new ServletHolder("ws-events", new GenericWebSocketServlet(service));
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/ws");
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

    /**
     * We test these different endpoints because they are parsed in the AbstractWebSocketHandler. Technically, we are
     * not testing these implementations, but the ping/pong support is guaranteed as part of the framework.
     */
    @DataProvider(name = "endpoint")
    public static Object[][] cacheEnable() {
        return new Object[][] { { "producer" }, { "consumer" }, { "reader" } };
    }

    @Test(dataProvider = "endpoint")
    public void testPingPong(String endpoint) throws Exception {
        HttpClient httpClient = new HttpClient();
        WebSocketClient webSocketClient = new WebSocketClient(httpClient);
        webSocketClient.start();
        MyWebSocket myWebSocket = new MyWebSocket();
        String webSocketUri = "ws://localhost:8080/ws/v2/" + endpoint + "/persistent/my-property/my-ns/my-topic";
        Future<Session> sessionFuture = webSocketClient.connect(myWebSocket, URI.create(webSocketUri));
        sessionFuture.get().getRemote().sendPing(ByteBuffer.wrap("test".getBytes()));
        assertTrue(myWebSocket.getResponse().contains("test"));
    }

    public static class GenericWebSocketHandler extends AbstractWebSocketHandler {

        public GenericWebSocketHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
            super(service, request, response);
        }

        @Override
        protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
            return true;
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static class GenericWebSocketServlet extends WebSocketServlet {

        private static final long serialVersionUID = 1L;
        private final WebSocketService service;

        public GenericWebSocketServlet(WebSocketService service) {
            this.service = service;
        }

        @Override
        public void configure(WebSocketServletFactory factory) {
            factory.setCreator((request, response) ->
                    new GenericWebSocketHandler(service, request.getHttpServletRequest(), response));
        }
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
