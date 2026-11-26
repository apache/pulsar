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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** TC control-plane capability probing with a fake connection pool; no sockets are opened. */
public class PulsarClientTcWatchConnectionTest {
    private EventLoopGroup eventLoop;
    private PulsarClientImpl client;
    private LookupService lookup;
    private ConnectionPool pool;
    private InetSocketAddress first;
    private InetSocketAddress second;

    @BeforeMethod
    public void setUp() throws Exception {
        ClientConfigurationData configuration = new ClientConfigurationData();
        configuration.setServiceUrl("pulsar://seed-a.invalid:6650,seed-b.invalid:6650");
        eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("tc-watch-test", true));
        pool = mock(ConnectionPool.class);
        when(pool.genRandomKeyToSelectCon()).thenReturn(7);
        client = new PulsarClientImpl(configuration, eventLoop, pool);
        lookup = mock(LookupService.class);
        when(lookup.isBinaryProtoLookupService()).thenReturn(true);
        first = InetSocketAddress.createUnresolved("seed-a.invalid", 6650);
        second = InetSocketAddress.createUnresolved("seed-b.invalid", 6650);
        when(lookup.resolveHost()).thenReturn(first, second);
        client.setLookup(lookup);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (eventLoop != null) {
                eventLoop.shutdownGracefully(0, 5, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void directBrokerUsesOrdinaryConnectionWithoutAnyBrokerPairing() throws Exception {
        ClientCnx direct = mock(ClientCnx.class);
        when(direct.isSupportsTcMetadataDiscovery()).thenReturn(true);
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.completedFuture(direct));
        // Keep the old route usable so a baseline fails on the wrong connection, not a null mock future.
        ClientCnx incorrectlyPaired = mock(ClientCnx.class);
        when(incorrectlyPaired.isProxied()).thenReturn(true);
        when(pool.getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, first, 7))
                .thenReturn(CompletableFuture.completedFuture(incorrectlyPaired));
        assertThat(client.getTcAssignmentsWatchConnection().get(5, TimeUnit.SECONDS)).isSameAs(direct);
        verify(lookup, times(1)).resolveHost();
        verify(pool, never()).getConnection(eq(PulsarChannelInitializer.PROXY_TO_ANY_BROKER), any(), anyInt());
        verify(pool, never()).releaseConnection(direct);
        verify(direct, never()).close();
    }

    @Test
    public void proxyControlProbePairsThroughTheSameResolvedServiceEndpoint() throws Exception {
        // A proxy's lookup connection advertises no broker features, so every capability flag stays false.
        ClientCnx control = mock(ClientCnx.class);
        ClientCnx paired = mock(ClientCnx.class);
        when(paired.isSupportsTcMetadataDiscovery()).thenReturn(true);
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.completedFuture(control));
        when(pool.getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, first, 7))
                .thenReturn(CompletableFuture.completedFuture(paired));
        assertThat(client.getTcAssignmentsWatchConnection().get(5, TimeUnit.SECONDS)).isSameAs(paired);
        verify(lookup, times(1)).resolveHost();
        verify(pool).releaseConnection(control);
        verify(pool, never()).releaseConnection(paired);
        verify(pool, never()).getConnection(any(), eq(second), anyInt());
        verify(control, never()).close();
    }

    @DataProvider
    public Object[][] brokerFeaturesWithoutTcDiscovery() {
        // {supports scalable topics, supports topic watchers}: what a broker without TC discovery advertises
        return new Object[][] {{true, true}, {false, true}, {true, false}};
    }

    @Test(dataProvider = "brokerFeaturesWithoutTcDiscovery")
    public void directBrokerWithoutTcDiscoveryFailsWithoutAnyBrokerPairing(boolean scalableTopics,
                                                                          boolean topicWatchers) {
        ClientCnx unsupported = mock(ClientCnx.class);
        when(unsupported.isSupportsScalableTopics()).thenReturn(scalableTopics);
        when(unsupported.isSupportsTopicWatchers()).thenReturn(topicWatchers);
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.completedFuture(unsupported));
        // Behind a load balancer a new pairing connection can reach another broker that supports TC discovery.
        ClientCnx otherBroker = mock(ClientCnx.class);
        when(otherBroker.isSupportsTcMetadataDiscovery()).thenReturn(true);
        when(pool.getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, first, 7))
                .thenReturn(CompletableFuture.completedFuture(otherBroker));
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.NotSupportedException.class);
        verify(pool, never()).getConnection(eq(PulsarChannelInitializer.PROXY_TO_ANY_BROKER), any(), anyInt());
        verify(pool).releaseConnection(unsupported);
        verify(lookup, times(1)).resolveHost();
    }

    @Test
    public void onlyTheAnyBrokerPairingMarksAConnectionProxied() throws Exception {
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved("service.invalid", 6650);
        assertThat(initializeClientCnx(endpoint, endpoint).isProxied()).isFalse();
        assertThat(initializeClientCnx(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, endpoint).isProxied()).isTrue();
    }

    private ClientCnx initializeClientCnx(InetSocketAddress logicalAddress, InetSocketAddress physicalAddress)
            throws Exception {
        ClientConfigurationData configuration = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, configuration, eventLoop);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            channel.pipeline().addLast("handler", cnx);
            CompletableFuture<Channel> initialized = new PulsarChannelInitializer(configuration, () -> cnx, null)
                    .initializeClientCnx(channel, logicalAddress, physicalAddress);
            channel.runPendingTasks();
            initialized.get(5, TimeUnit.SECONDS);
            return cnx;
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void unsupportedProbeAndPairedBrokerFailAndReleaseBothUnusedConnections() {
        ClientCnx control = mock(ClientCnx.class);
        ClientCnx unsupported = mock(ClientCnx.class);
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.completedFuture(control));
        when(pool.getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, first, 7))
                .thenReturn(CompletableFuture.completedFuture(unsupported));
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.NotSupportedException.class);
        verify(pool).releaseConnection(control);
        verify(pool).releaseConnection(unsupported);
        verify(lookup, times(1)).resolveHost();
    }

    @Test
    public void synchronousEndpointResolutionFailureIsReturnedAsAFailedFuture() {
        when(lookup.resolveHost()).thenThrow(new IllegalStateException("endpoint resolution failed"));
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class).withCauseInstanceOf(IllegalStateException.class);
        verify(pool, never()).getConnection(any(), any(), anyInt());
    }

    @Test
    public void synchronousPairingFailureStillReleasesTheUnusedProbe() {
        ClientCnx control = mock(ClientCnx.class);
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.completedFuture(control));
        when(pool.getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, first, 7))
                .thenThrow(new IllegalStateException("pairing failed before a future was returned"));
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class).withCauseInstanceOf(IllegalStateException.class);
        verify(pool).releaseConnection(control);
        verify(lookup, times(1)).resolveHost();
    }

    @Test
    public void failedProbeDoesNotRetryThroughADifferentEndpointOrAnyBrokerTunnel() {
        when(pool.getConnection(first, first, 7)).thenReturn(CompletableFuture.failedFuture(
                new PulsarClientException.ConnectException("probe disconnected")));
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.ConnectException.class);
        verify(pool, never()).getConnection(eq(PulsarChannelInitializer.PROXY_TO_ANY_BROKER), any(), anyInt());
        verify(lookup, times(1)).resolveHost();
    }

    @Test
    public void nonBinaryServiceUrlFailsWithoutResolvingOrConnecting() {
        when(lookup.isBinaryProtoLookupService()).thenReturn(false);
        assertThat(client.getTcAssignmentsWatchConnection()).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.InvalidServiceURL.class);
        verify(lookup, never()).resolveHost();
        verify(pool, never()).getConnection(any(), any(), anyInt());
    }
}
