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
package org.apache.pulsar.tests.integration.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class WebSocketTestSuite extends PulsarTestSuite {
    private static final Logger log = LoggerFactory.getLogger(WebSocketTestSuite.class);

    protected void testWebSocket(String url) throws Exception {

        final String tenant = "websocket-test-" + randomName(10);
        final String namespace = tenant + "/ns1";
        final String topic = namespace + "/topic-" + randomName(5);

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        log.debug("Using url {}", url);

        @Cleanup
        WebSocketConsumer consumer = new WebSocketConsumer(url, topic);
        log.debug("Created ws consumer");

        @Cleanup
        WebSocketPublisher publisher = new WebSocketPublisher(url, topic);
        log.debug("Created ws publisher");

        publisher.send("SGVsbG8gV29ybGQ=");
        log.debug("Sent message through publisher");

        Map<String, Object> response = publisher.getResponse();
        Assert.assertEquals(response.get("result"), "ok", "Bad response: " + response);
        log.debug("Publisher received response {}", response);

        String received = consumer.getPayloadFromResponse();
        log.debug("Consumer received message {} ", received);
        Assert.assertEquals(received, "SGVsbG8gV29ybGQ=");
    }

    @WebSocket
    public static class Client extends WebSocketAdapter implements AutoCloseable {
        final BlockingQueue<String> incomingMessages = new ArrayBlockingQueue<>(10);
        private final WebSocketClient client;

        Client(String webSocketUri) throws Exception {
            HttpClient httpClient = new HttpClient();
            client = new WebSocketClient(httpClient);
            client.start();
            client.connect(this, URI.create(webSocketUri)).get();
        }

        void sendText(String payload) throws IOException {
            getSession().getRemote().sendString(payload);
        }

        @Override
        public void onWebSocketText(String s) {
            incomingMessages.add(s);
        }

        Map<String, Object> getResponse() throws Exception {
            String response =  incomingMessages.poll(5, TimeUnit.SECONDS);
            if (response == null) {
                Assert.fail("Did not get websocket response within timeout");
            }
            return ObjectMapperFactory.getMapper().getObjectMapper().readValue(response, new TypeReference<>() {});

        }

        @Override
        public void close() throws Exception {
            client.stop();
        }
    }

    @WebSocket
    protected static class WebSocketPublisher extends Client {

        WebSocketPublisher(String url, String topic) throws Exception {
            super(url + "/ws/v2/producer/persistent/" + topic);
        }

        void send(String payload) throws IOException {
            sendText("{\n" +
                    "  \"payload\": \"" + payload + "\",\n" +
                    "  \"properties\": {\"key1\": \"value1\", \"key2\": \"value2\"},\n" +
                    "  \"context\": \"1\"\n" +
                    "}");
        }
    }

    @WebSocket
    protected static class WebSocketConsumer extends Client {

        WebSocketConsumer(String url, String topic) throws Exception {
            super(url + "/ws/v2/consumer/persistent/" + topic + "/" + randomName(8));
        }

        String getPayloadFromResponse() throws Exception {
            Map<String, Object> response =  getResponse();
            return String.valueOf(response.get("payload"));
        }
    }

}
