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
package org.apache.pulsar.client.api;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
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
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
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
        Pair<Integer, Integer> freePorts = getFreePorts();
        int brokerPort = freePorts.getLeft();
        brokerAddress = InetSocketAddress.createUnresolved(host, brokerPort);
        int brokerPortSsl = freePorts.getRight();
        brokerSslAddress = InetSocketAddress.createUnresolved(host, brokerPortSsl);
        super.internalSetup();
    }

    private static Pair<Integer, Integer> getFreePorts() {
        try (ServerSocket serverSocket = new ServerSocket(); ServerSocket serverSocket2 = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(0));
            serverSocket2.setReuseAddress(true);
            serverSocket2.bind(new InetSocketAddress(0));
            return Pair.of(serverSocket.getLocalPort(), serverSocket2.getLocalPort());
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
        TopicName topicName = TopicName.get("persistent://public/default/test");

        // test request 1
        {
            var result = lookupService.getBroker(topicName).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getLogicalAddress(), brokerAddress);
            Assert.assertEquals(result.getPhysicalAddress(), brokerAddress);
            Assert.assertEquals(result.isUseProxy(), false);
        }
        // test request 2
        {
            var result = lookupService.getBroker(topicName).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getLogicalAddress(), brokerAddress);
            Assert.assertEquals(result.getPhysicalAddress(), brokerAddress);
            Assert.assertEquals(result.isUseProxy(), false);
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

        var result =
                lookupService.getBroker(TopicName.get("persistent://public/default/test")).get(10, TimeUnit.SECONDS);
        Assert.assertEquals(result.getLogicalAddress(), address);
        Assert.assertEquals(result.getPhysicalAddress(), address);
        Assert.assertEquals(result.isUseProxy(), false);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        pulsar.close();
        GracefulExecutorServicesShutdown.initiate()
                .timeout(Duration.ZERO)
                .shutdown(executorService)
                .handle().get();
        EventLoopUtil.shutdownGracefully(eventExecutors).get();
        super.internalCleanup();
    }
}
