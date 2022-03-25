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
package org.apache.pulsar.tests.integration.proxy;

import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.awaitility.Awaitility;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

/**
 * Test cases for proxy.
 */
public class TestProxyWithWebSocket extends PulsarTestSuite {
    private static final Logger log = LoggerFactory.getLogger(TestProxyWithWebSocket.class);

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        Map<String, String> envs = new HashMap<>();
        envs.put("webSocketServiceEnabled", "true");
        specBuilder.proxyEnvs(envs);
        return super.beforeSetupCluster(clusterName, specBuilder);
    }

    @Test
    public void testWebSocket() throws Exception {

        final String tenant = "proxy-test-" + randomName(10);
        final String namespace = tenant + "/ns1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        HttpClient httpClient = new HttpClient();
        WebSocketClient webSocketClient = new WebSocketClient(httpClient);
        webSocketClient.start();
        MyWebSocket myWebSocket = new MyWebSocket();
        String webSocketUri = pulsarCluster.getProxy().getHttpServiceUrl().replaceFirst("http", "ws")
                + "/ws/v2/producer/persistent/" + namespace + "/my-topic";
        Future<Session> sessionFuture =  webSocketClient.connect(myWebSocket,
                URI.create(webSocketUri));
        sessionFuture.get().getRemote().sendString("{\n" +
                "  \"payload\": \"SGVsbG8gV29ybGQ=\",\n" +
                "  \"properties\": {\"key1\": \"value1\", \"key2\": \"value2\"},\n" +
                "  \"context\": \"1\"\n" +
                "}");

        Awaitility.await().untilAsserted(() -> {
            String response = myWebSocket.getResponse();
            Assert.assertNotNull(response);
            Assert.assertTrue(response.contains("ok"));
        });
    }

    @WebSocket
    public static class MyWebSocket implements WebSocketListener {
        Queue<String> incomingMessages = new ArrayBlockingQueue<>(10);
        @Override
        public void onWebSocketBinary(byte[] bytes, int i, int i1) {
        }

        @Override
        public void onWebSocketText(String s) {
            incomingMessages.add(s);
        }

        @Override
        public void onWebSocketClose(int i, String s) {
        }

        @Override
        public void onWebSocketConnect(Session session) {

        }

        @Override
        public void onWebSocketError(Throwable throwable) {

        }

        public String getResponse() {
            return incomingMessages.poll();
        }
    }
}
