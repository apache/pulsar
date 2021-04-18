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
package org.apache.pulsar.client.api;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PulsarMultiListenersWithInternalListenerNameTest extends MockedPulsarServiceBaseTest {
    private final boolean withInternalListener;
    private ExecutorService executorService;
    private String hostAndBrokerPort;
    private String hostAndBrokerPortSsl;

    public PulsarMultiListenersWithInternalListenerNameTest() {
        this(true);
    }

    protected PulsarMultiListenersWithInternalListenerNameTest(boolean withInternalListener) {
        this.withInternalListener = withInternalListener;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.executorService = Executors.newFixedThreadPool(1);
        this.isTcpLookup = true;
        String host = InetAddress.getLocalHost().getHostAddress();
        int brokerPort = getFreePort();
        hostAndBrokerPort = host + ":" + brokerPort;
        int brokerPortSsl = getFreePort();
        hostAndBrokerPortSsl = host + ":" + brokerPortSsl;
        super.internalSetup();
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setClusterName("localhost");
        this.conf.setAdvertisedAddress(null);
        this.conf.setAdvertisedListeners(String.format("internal:pulsar://%s,internal:pulsar+ssl://%s",
                hostAndBrokerPort, hostAndBrokerPortSsl));
        if (withInternalListener) {
            this.conf.setInternalListenerName("internal");
        }
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.listenerName("internal");
    }

    @Test
    public void testFindBrokerWithListenerName() throws Throwable {
        admin.clusters().createCluster("localhost", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("localhost"));
        this.admin.tenants().createTenant("public", tenantInfo);
        this.admin.namespaces().createNamespace("public/default");
        @Cleanup
        LookupService lookupService = new BinaryProtoLookupService((PulsarClientImpl) this.pulsarClient,
                lookupUrl.toString(), "internal", false, this.executorService);
        // test request 1
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
                    lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey().toString(), hostAndBrokerPort);
            Assert.assertEquals(result.getValue().toString(), hostAndBrokerPort);
        }
        // test request 2
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
                    lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey().toString(), hostAndBrokerPort);
            Assert.assertEquals(result.getValue().toString(), hostAndBrokerPort);
        }
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }
        super.internalCleanup();
    }

}
