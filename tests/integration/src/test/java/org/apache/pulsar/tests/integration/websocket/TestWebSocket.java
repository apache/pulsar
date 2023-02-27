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


import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.apache.pulsar.tests.integration.containers.WebSocketContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.annotations.Test;
import java.util.Collections;
import java.util.Map;

/**
 * Test cases for websocket.
 */
public class TestWebSocket extends WebSocketTestSuite {

    public static final String WEBSOCKET = "websocket";

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {

        Map<String, String> enableWebSocket = Collections.singletonMap("webSocketServiceEnabled", "true");
        specBuilder.brokerEnvs(enableWebSocket);
        specBuilder.proxyEnvs(enableWebSocket);

        specBuilder.externalService(WEBSOCKET, new WebSocketContainer(clusterName, WEBSOCKET));
        specBuilder.externalServiceEnv(WEBSOCKET, ImmutableMap.<String, String>builder()
                .put("configurationMetadataStoreUrl", CSContainer.NAME + ":" + CSContainer.CS_PORT)
                .put("webServicePort", "" + WebSocketContainer.BROKER_HTTP_PORT)
                .put("clusterName", clusterName)
                .build());
        return super.beforeSetupCluster(clusterName, specBuilder);
    }

    @Test
    public void testExternalService() throws Exception {
        WebSocketContainer service = (WebSocketContainer) pulsarCluster.getExternalServices().get(WEBSOCKET);
        testWebSocket(service.getWSUrl());
    }

    @Test
    public void testBroker() throws Exception {
        BrokerContainer broker = pulsarCluster.getAnyBroker();
        String url = "ws://" + broker.getHost() + ":" + broker.getMappedPort(BrokerContainer.BROKER_HTTP_PORT);
        testWebSocket(url);
    }

    @Test
    public void testProxy() throws Exception {
        String url = pulsarCluster.getProxy().getHttpServiceUrl().replaceFirst("http", "ws");
        testWebSocket(url);
    }
}
