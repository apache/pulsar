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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.resolver.DefaultNameResolver;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.HttpLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PulsarMultiListenersWithInternalListenerNameTest extends MockedPulsarServiceBaseTest {
    private final boolean withLegacyListeners;
    private ExecutorService executorService;
    private InetSocketAddress brokerAddress;
    private InetSocketAddress brokerSslAddress;
    private InetSocketAddress webServiceAddress;
    private EventLoopGroup eventExecutors;

    public PulsarMultiListenersWithInternalListenerNameTest() {
        this(true);
    }

    protected PulsarMultiListenersWithInternalListenerNameTest(boolean withLegacyListeners) {
        this.withLegacyListeners = withLegacyListeners;
        this.isTcpLookup = true;
    }

    @Override
    protected URI resolveLookupUrl(boolean usePulsarBinaryProtocol) {
        if (usePulsarBinaryProtocol) {
            if (withLegacyListeners) {
                return URI.create(pulsar.getBrokerServiceUrl());
            } else {
                return URI.create("pulsar://" + brokerAddress.getHostString() + ":" + brokerAddress.getPort());
            }
        } else {
            throw new IllegalStateException("Not supported in this test");
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.executorService = Executors.newFixedThreadPool(1);
        this.eventExecutors = new NioEventLoopGroup();
        InetAddress host = InetAddress.getLocalHost();
        String hostAddress = host.getHostAddress();
        List<Integer> freePorts = getFreePorts(host, 3);
        brokerAddress = InetSocketAddress.createUnresolved(hostAddress, freePorts.get(0));
        brokerSslAddress = InetSocketAddress.createUnresolved(hostAddress, freePorts.get(1));
        webServiceAddress = InetSocketAddress.createUnresolved(hostAddress, freePorts.get(2));
        super.internalSetup();
    }

    protected void doInitConf() throws Exception {
        super.doInitConf();
        if (!withLegacyListeners) {
            conf.setBrokerServicePort(Optional.empty());
            conf.setWebServicePort(Optional.empty());
        }
        this.conf.setClusterName("localhost");
        this.conf.setAdvertisedListeners(
                String.format("internal:pulsar://%s:%s,internal:pulsar+ssl://%s:%s,internal:http://%s:%s",
                        brokerAddress.getHostString(), brokerAddress.getPort(), brokerSslAddress.getHostString(),
                        brokerSslAddress.getPort(), webServiceAddress.getHostString(), webServiceAddress.getPort()));
        String bindAll = "0.0.0.0";
        this.conf.setBindAddresses(
                String.format("internal:pulsar://%s:%s,internal:pulsar+ssl://%s:%s,internal:http://%s:%s",
                        bindAll, brokerAddress.getPort(),
                        bindAll, brokerSslAddress.getPort(),
                        bindAll, webServiceAddress.getPort()));
        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        GracefulExecutorServicesShutdown.initiate()
                .timeout(Duration.ZERO)
                .shutdown(executorService)
                .handle().get();
        EventLoopUtil.shutdownGracefully(eventExecutors).get();
        super.internalCleanup();
    }

    private static List<Integer> getFreePorts(InetAddress host, int numberOfPorts) {
        List<ServerSocket> sockets = new ArrayList<>(numberOfPorts);
        try {
            List<Integer> ports = new ArrayList<>(numberOfPorts);
            for (int i = 0; i < numberOfPorts; i++) {
                ServerSocket socket = new ServerSocket();
                sockets.add(socket);
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(host, 0));
                ports.add(socket.getLocalPort());
            }
            return List.copyOf(ports);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // ignore
                }
            }
        }
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.listenerName("internal");
    }

    @Test
    public void testFindBrokerWithListenerName() throws Exception {
        admin.clusters().createCluster("localhost", ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton("localhost"))
                .build();
        this.admin.tenants().createTenant("public", tenantInfo);
        this.admin.namespaces().createNamespace("public/default");

        doFindBrokerWithListenerName(true);
        doFindBrokerWithListenerName(false);
    }
    @SuppressWarnings("deprecation")

    private void doFindBrokerWithListenerName(boolean useHttp) throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setListenerName("internal");
        conf.setServiceUrl(pulsar.getWebServiceAddress());
        conf.setMaxLookupRedirects(10);

        @Cleanup
        LookupService lookupService = useHttp ? new HttpLookupService(InstrumentProvider.NOOP, conf, eventExecutors,
                null, new DefaultNameResolver(ImmediateEventExecutor.INSTANCE)) :
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
        admin.clusters().createCluster("localhost", ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
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
        HttpLookupService lookupService = new HttpLookupService(InstrumentProvider.NOOP, conf, eventExecutors, null,
                new DefaultNameResolver(ImmediateEventExecutor.INSTANCE));
        NamespaceService namespaceService = pulsar.getNamespaceService();

        LookupResult lookupResult = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(pulsar.getWebServiceAddress())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                .authoritativeRedirect(true)
                .build();
        Optional<LookupResult> optional = Optional.of(lookupResult);
        InetSocketAddress address = InetSocketAddress.createUnresolved("192.168.0.1", 8080);
        NamespaceEphemeralData namespaceEphemeralData = new NamespaceEphemeralData("192.168.0.1:8081",
                "pulsar://" + address.getHostName() + ":" + address.getPort(), null,
                "http://192.168.0.1:8081", null, false);
        // The redirect target serves the second lookup as a final broker URL response so that
        // HttpLookupService terminates the redirect chain at the broker that owns the topic.
        LookupResult lookupResult2 = LookupResult.create(namespaceEphemeralData, null);
        Optional<LookupResult> optional2 = Optional.of(lookupResult2);
        doReturn(CompletableFuture.completedFuture(optional), CompletableFuture.completedFuture(optional2))
                .when(namespaceService).getBrokerServiceUrlAsync(any(), any());

        var result =
                lookupService.getBroker(TopicName.get("persistent://public/default/test")).get(10, TimeUnit.SECONDS);
        Assert.assertEquals(result.getLogicalAddress(), address);
        Assert.assertEquals(result.getPhysicalAddress(), address);
        Assert.assertEquals(result.isUseProxy(), false);
    }
}
