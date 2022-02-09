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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.HttpLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

@Test(groups = "broker-api")
public class PulsarMultiListenersWithInternalListenerNameTest extends MockedPulsarServiceBaseTest {
    private final boolean withInternalListener;
    private ExecutorService executorService;
    private InetSocketAddress brokerAddress;
    private InetSocketAddress brokerSslAddress;
    private EventLoopGroup eventExecutors;

    public PulsarMultiListenersWithInternalListenerNameTest() {
        this(true);
    }

    protected PulsarMultiListenersWithInternalListenerNameTest(boolean withInternalListener) {
        this.withInternalListener = withInternalListener;
        // enable port forwarding from the configured advertised port to the dynamic listening port
        this.enableBrokerGateway = true;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.executorService = Executors.newFixedThreadPool(1);
        this.eventExecutors = new NioEventLoopGroup();
        this.isTcpLookup = true;
        String host = InetAddress.getLocalHost().getHostAddress();
        int brokerPort = getFreePort();
        brokerAddress = InetSocketAddress.createUnresolved(host, brokerPort);
        int brokerPortSsl = getFreePort();
        brokerSslAddress = InetSocketAddress.createUnresolved(host, brokerPortSsl);
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
        this.conf.setAdvertisedListeners(String.format("internal:pulsar://%s:%s,internal:pulsar+ssl://%s:%s",
                brokerAddress.getHostString(), brokerAddress.getPort(),
                brokerSslAddress.getHostString(), brokerSslAddress.getPort()));
        if (withInternalListener) {
            this.conf.setInternalListenerName("internal");
        }
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.listenerName("internal");
    }
    @Test
    public void testFindBrokerWithListenerName() throws Exception {
        admin.clusters().createCluster("localhost", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton("localhost"))
                .build();
        this.admin.tenants().createTenant("public", tenantInfo);
        this.admin.namespaces().createNamespace("public/default");

        doFindBrokerWithListenerName(true);
        doFindBrokerWithListenerName(false);
    }

    private void doFindBrokerWithListenerName(boolean useHttp) throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setListenerName("internal");
        conf.setServiceUrl(pulsar.getWebServiceAddress());
        conf.setMaxLookupRedirects(10);

        @Cleanup
        LookupService lookupService = useHttp ? new HttpLookupService(conf, eventExecutors) :
                new BinaryProtoLookupService((PulsarClientImpl) this.pulsarClient,
                lookupUrl.toString(), "internal", false, this.executorService);
        // test request 1
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
                    lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey(), brokerAddress);
            Assert.assertEquals(result.getValue(), brokerAddress);
        }
        // test request 2
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
                    lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey(), brokerAddress);
            Assert.assertEquals(result.getValue(), brokerAddress);
        }
    }

    @Test
    public void testHttpLookupRedirect() throws Exception {
        admin.clusters().createCluster("localhost", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton("localhost"))
                .build();
        this.admin.tenants().createTenant("public", tenantInfo);
        this.admin.namespaces().createNamespace("public/default");
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setListenerName("internal");
        conf.setServiceUrl(pulsar.getWebServiceAddress());
        conf.setMaxLookupRedirects(10);

        @Cleanup
        HttpLookupService lookupService = new HttpLookupService(conf, eventExecutors);
        NamespaceService namespaceService = pulsar.getNamespaceService();

        LookupResult lookupResult = new LookupResult(pulsar.getWebServiceAddress(), null,
                pulsar.getBrokerServiceUrl(), null, true);
        Optional<LookupResult> optional = Optional.of(lookupResult);
        InetSocketAddress address = InetSocketAddress.createUnresolved("192.168.0.1", 8080);
        NamespaceEphemeralData namespaceEphemeralData =
                new NamespaceEphemeralData("pulsar://" + address.getHostName() + ":" + address.getPort(),
                null, "http://192.168.0.1:8081", null, false);
        LookupResult lookupResult2 = new LookupResult(namespaceEphemeralData);
        Optional<LookupResult> optional2 = Optional.of(lookupResult2);
        doReturn(CompletableFuture.completedFuture(optional), CompletableFuture.completedFuture(optional2))
                .when(namespaceService).getBrokerServiceUrlAsync(any(), any());

        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
                lookupService.getBroker(TopicName.get("persistent://public/default/test"));

        Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
        Assert.assertEquals(result.getKey(), address);
        Assert.assertEquals(result.getValue(), address);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }
        if (eventExecutors != null) {
            eventExecutors.shutdownGracefully();
        }
        super.internalCleanup();
    }
}
