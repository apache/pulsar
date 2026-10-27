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

import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.apache.pulsar.tests.integration.containers.PulsarContainer.CS_PORT;
import java.util.Map;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testcontainers.images.builder.Transferable;
import org.testng.annotations.Test;

/**
 * The broker's WebSocket service when the broker reaches the configuration store over TLS, which only the client
 * configuration file in {@code configurationStoreConfigPath} sets up. The WebSocket service has to share the
 * broker's configuration store client: a client of its own would connect in plaintext and fail the broker startup.
 */
public class WebSocketTlsConfigurationStoreTest extends WebSocketTestSuite {

    private static final int CS_TLS_PORT = 2281;
    private static final String CS_CLIENT_CONFIG_PATH = "/pulsar/conf/configuration-store-client.conf";

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.numBrokers(1);
        specBuilder.numProxies(0);
        specBuilder.brokerEnvs(Map.of(
                "webSocketServiceEnabled", "true",
                "configurationMetadataStoreUrl", CSContainer.NAME + ":" + CS_TLS_PORT,
                // Not in broker.conf, so it is added with the prefix
                "PULSAR_PREFIX_configurationStoreConfigPath", CS_CLIENT_CONFIG_PATH,
                // The test certificate is for localhost. Netty 4.2 clients check the hostname by default, which
                // zookeeper.ssl.hostnameVerification=false doesn't turn off
                "PULSAR_EXTRA_OPTS", "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"));
        return super.beforeSetupCluster(clusterName, specBuilder);
    }

    @Override
    protected void beforeStartCluster() {
        pulsarCluster.forEachContainer(container -> {
            if (container instanceof CSContainer) {
                // A TLS-only port for the broker, and the plaintext one for the cluster metadata initialization
                container.withCopyToContainer(Transferable.of("""
                        tickTime=2000
                        dataDir=data/global-zookeeper
                        clientPort=%d
                        secureClientPort=%d
                        serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
                        ssl.keyStore.location=/pulsar/certificate-authority/jks/broker.keystore.jks
                        ssl.keyStore.password=111111
                        ssl.clientAuth=none
                        """.formatted(CS_PORT, CS_TLS_PORT)), "/pulsar/conf/global_zookeeper.conf");
            } else if (container instanceof BrokerContainer) {
                container.withCopyToContainer(Transferable.of("""
                        zookeeper.client.secure=true
                        zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
                        zookeeper.ssl.trustStore.location=/pulsar/certificate-authority/jks/broker.truststore.jks
                        zookeeper.ssl.trustStore.password=111111
                        # The certificate is for localhost, not for the configuration store's hostname
                        zookeeper.ssl.hostnameVerification=false
                        """), CS_CLIENT_CONFIG_PATH);
            }
        });
    }

    @Test
    public void testBroker() throws Exception {
        BrokerContainer broker = pulsarCluster.getAnyBroker();
        testWebSocket("ws://" + broker.getHost() + ":" + broker.getMappedPort(BROKER_HTTP_PORT));
    }
}
