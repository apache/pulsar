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
package org.apache.pulsar.client.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class WatchTcAssignmentsDiscoveryTest {

    @DataProvider
    public Object[][] watchRoutes() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "watchRoutes")
    public void testAssignmentLeaderUsesTheDiscoveredDirectOrProxyRoute(boolean proxy, boolean tls) throws Exception {
        PulsarClientImpl client = spy((PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://service.invalid:6650").build());
        WatchTcAssignmentsDiscovery discovery = new WatchTcAssignmentsDiscovery(client);
        try {
            client.getConfiguration().setUseTls(tls);
            InetSocketAddress service = InetSocketAddress.createUnresolved("service.invalid", tls ? 6651 : 6650);
            InetSocketAddress elected = InetSocketAddress.createUnresolved("elected.invalid", tls ? 6651 : 6650);
            LookupService lookup = mock(LookupService.class);
            when(lookup.isBinaryProtoLookupService()).thenReturn(true);
            when(lookup.resolveHost()).thenReturn(service);
            client.setLookup(lookup);
            ClientCnx control = mock(ClientCnx.class);
            when(control.isSupportsTcMetadataDiscovery()).thenReturn(true);
            when(control.isProxied()).thenReturn(proxy);
            Channel controlChannel = mock(Channel.class);
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            when(control.ctx()).thenReturn(context);
            when(context.writeAndFlush(any())).thenAnswer(invocation -> {
                ReferenceCountUtil.release(invocation.getArgument(0));
                return new DefaultChannelPromise(controlChannel, ImmediateEventExecutor.INSTANCE).setSuccess();
            });
            AtomicReference<ClientCnx.TcAssignmentsWatcherSession> session = new AtomicReference<>();
            doAnswer(invocation -> {
                session.set(invocation.getArgument(1));
                return null;
            }).when(control).registerTcAssignmentsWatcher(anyLong(), any());
            doReturn(CompletableFuture.completedFuture(control)).when(client).getTcAssignmentsWatchConnection();
            // Before the route fix the watch always requested an any-broker pairing. Such a
            // ClientCnx is marked proxied even when the service endpoint is a direct broker.
            ClientCnx oldPaired = mock(ClientCnx.class);
            when(oldPaired.isSupportsTcMetadataDiscovery()).thenReturn(true);
            when(oldPaired.isProxied()).thenReturn(true);
            when(oldPaired.ctx()).thenReturn(context);
            doAnswer(invocation -> {
                session.set(invocation.getArgument(1));
                return null;
            }).when(oldPaired).registerTcAssignmentsWatcher(anyLong(), any());
            doReturn(CompletableFuture.completedFuture(oldPaired)).when(client).getAnyBrokerProxyConnection();
            // Routing is observable before coordinator handshake completion. Leave that future
            // pending so this subpackage fixture needs no access to ClientCnx's internal SPI.
            doReturn(new CompletableFuture<ClientCnx>()).when(client)
                    .getConnection(any(InetSocketAddress.class), any(InetSocketAddress.class), anyInt());

            CompletableFuture<Void> started = discovery.start();
            assertThat(session.get()).isNotNull();
            session.get().onSnapshot(1, Map.of(0L, new String[] {
                    "pulsar://elected.invalid:6650", "pulsar+ssl://elected.invalid:6651"}));
            started.get(5, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    verify(client).getConnection(eq(elected), eq(proxy ? service : elected), anyInt()));
            if (proxy) {
                verify(client).getProxyConnection(eq(elected), anyInt());
            } else {
                verify(client, never()).getProxyConnection(any(), anyInt());
                verify(lookup, never()).resolveHost();
                verify(client, never()).getAnyBrokerProxyConnection();
            }
        } finally {
            try {
                discovery.close();
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void testUnsupportedBrokerFailsWithoutRetrying() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ClientCnx cnx = mock(ClientCnx.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.getTcAssignmentsWatchConnection()).thenReturn(CompletableFuture.completedFuture(cnx));
        WatchTcAssignmentsDiscovery discovery = new WatchTcAssignmentsDiscovery(client);
        try {
            CompletableFuture<Void> start = discovery.start();
            assertThat(start).isCompletedExceptionally();
            assertThatThrownBy(start::join)
                    .hasCauseInstanceOf(PulsarClientException.NotSupportedException.class)
                    .hasMessageContaining("Broker does not support scalable-topics transactions");
            verify(client, never()).timer();
            verify(cnx, never()).registerTcAssignmentsWatcher(anyLong(), any());
            verify(cnx, never()).ctx();
        } finally {
            discovery.close();
        }
    }
}
