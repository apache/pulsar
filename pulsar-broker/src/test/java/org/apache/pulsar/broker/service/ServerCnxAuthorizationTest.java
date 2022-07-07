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

package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import javax.crypto.SecretKey;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServerCnxAuthorizationTest {
    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String CLIENT_PRINCIPAL = "client";
    private final String PROXY_PRINCIPAL = "proxy";
    private final String CLIENT_TOKEN = Jwts.builder().setSubject(CLIENT_PRINCIPAL).signWith(SECRET_KEY).compact();
    private final String PROXY_TOKEN = Jwts.builder().setSubject(PROXY_PRINCIPAL).signWith(SECRET_KEY).compact();

    private PulsarService pulsar;
    private PulsarResources pulsarResources;
    private BrokerService brokerService;
    private ServiceConfiguration svcConfig;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        svcConfig = spy(ServiceConfiguration.class);
        svcConfig.setKeepAliveIntervalSeconds(0);
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("pulsar-cluster");
        svcConfig.setSuperUserRoles(Collections.singleton(PROXY_PRINCIPAL));
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        svcConfig.setAuthorizationEnabled(true);
        svcConfig.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        svcConfig.setProperties(properties);

        pulsar = spyWithClassAndConstructorArgs(PulsarService.class, svcConfig);
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();

        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(PulsarResources.class)).when(pulsar).getPulsarResources();

        ManagedLedgerFactory mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1).build();
        doReturn(createMockBookKeeper(executor))
                .when(pulsar).getBookKeeperClient();

        MetadataStore store = new ZKMetadataStore(mockZk);

        doReturn(store).when(pulsar).getLocalMetadataStore();
        doReturn(store).when(pulsar).getConfigurationMetadataStore();

        pulsarResources = spyWithClassAndConstructorArgs(PulsarResources.class, store, store);
        doReturn(pulsarResources).when(pulsar).getPulsarResources();
        NamespaceResources namespaceResources =
                spyWithClassAndConstructorArgs(NamespaceResources.class, store, store, 30);
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();

        TenantResources tenantResources = spyWithClassAndConstructorArgs(TenantResources.class, store, 30);
        doReturn(tenantResources).when(pulsarResources).getTenantResources();

        doReturn(CompletableFuture.completedFuture(Optional.of(TenantInfo.builder().build()))).when(tenantResources)
                .getTenantAsync("public");

        brokerService = spyWithClassAndConstructorArgs(BrokerService.class, pulsar, eventLoopGroup);
        BrokerInterceptor interceptor = mock(BrokerInterceptor.class);
        doReturn(interceptor).when(brokerService).getInterceptor();
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(executor).when(pulsar).getOrderedExecutor();
    }

    @Test
    public void testVerifyOriginalPrincipalWithAuthDataForwardedFromProxy() throws Exception {
        doReturn(true).when(svcConfig).isAuthenticateOriginalAuthData();

        ServerCnx serverCnx = spyWithClassAndConstructorArgs(ServerCnx.class, pulsar);
        ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(null).when(channelPipeline).get(PulsarChannelInitializer.TLS_HANDLER);

        SocketAddress socketAddress = new InetSocketAddress(0);
        doReturn(socketAddress).when(channel).remoteAddress();
        doReturn(channel).when(channelHandlerContext).channel();
        channelHandlerContext.channel().remoteAddress();
        serverCnx.channelActive(channelHandlerContext);

        // connect
        AuthenticationToken clientAuthenticationToken = new AuthenticationToken(CLIENT_TOKEN);
        AuthenticationToken proxyAuthenticationToken = new AuthenticationToken(PROXY_TOKEN);
        CommandConnect connect = new CommandConnect();
        connect.setAuthMethodName(proxyAuthenticationToken.getAuthMethodName());
        connect.setAuthData(proxyAuthenticationToken.getAuthData().getCommandData().getBytes(StandardCharsets.UTF_8));
        connect.setClientVersion("test");
        connect.setProtocolVersion(1);
        connect.setOriginalPrincipal(CLIENT_PRINCIPAL);
        connect.setOriginalAuthData(clientAuthenticationToken.getAuthData().getCommandData());
        connect.setOriginalAuthMethod(clientAuthenticationToken.getAuthMethodName());

        serverCnx.handleConnect(connect);
        assertEquals(serverCnx.getOriginalAuthData().getCommandData(),
                clientAuthenticationToken.getAuthData().getCommandData());
        assertEquals(serverCnx.getOriginalAuthState().getAuthRole(), CLIENT_PRINCIPAL);
        assertEquals(serverCnx.getOriginalPrincipal(), CLIENT_PRINCIPAL);
        assertEquals(serverCnx.getAuthData().getCommandData(),
                proxyAuthenticationToken.getAuthData().getCommandData());
        assertEquals(serverCnx.getAuthRole(), PROXY_PRINCIPAL);
        assertEquals(serverCnx.getAuthState().getAuthRole(), PROXY_PRINCIPAL);

        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsarResources);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();

        // lookup
        CommandLookupTopic commandLookupTopic = new CommandLookupTopic();
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        commandLookupTopic.setTopic(topicName.toString());
        commandLookupTopic.setRequestId(1);
        serverCnx.handleLookup(commandLookupTopic);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                CLIENT_PRINCIPAL,
                serverCnx.getOriginalAuthData());
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                PROXY_PRINCIPAL,
                serverCnx.getAuthData());

        // producer
        CommandProducer commandProducer = new CommandProducer();
        commandProducer.setRequestId(1);
        commandProducer.setProducerId(1);
        commandProducer.setProducerName("test-producer");
        commandProducer.setTopic(topicName.toString());
        serverCnx.handleProducer(commandProducer);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.PRODUCE,
                CLIENT_PRINCIPAL,
                serverCnx.getOriginalAuthData());
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                PROXY_PRINCIPAL,
                serverCnx.getAuthData());

        // consumer
        CommandSubscribe commandSubscribe = new CommandSubscribe();
        commandSubscribe.setTopic(topicName.toString());
        commandSubscribe.setRequestId(1);
        commandSubscribe.setConsumerId(1);
        final String subscriptionName = "test-subscribe";
        commandSubscribe.setSubscription("test-subscribe");
        commandSubscribe.setSubType(CommandSubscribe.SubType.Shared);
        serverCnx.handleSubscribe(commandSubscribe);

        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(CLIENT_PRINCIPAL), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    try {
                        assertEquals(arg.getCommandData(), clientAuthenticationToken.getAuthData().getCommandData());
                    } catch (PulsarClientException e) {
                        fail(e.getMessage());
                    }
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(PROXY_PRINCIPAL), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    try {
                        assertEquals(arg.getCommandData(), proxyAuthenticationToken.getAuthData().getCommandData());
                    } catch (PulsarClientException e) {
                        fail(e.getMessage());
                    }
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
    }

    @Test
    public void testVerifyOriginalPrincipalWithoutAuthDataForwardedFromProxy() throws Exception {
        doReturn(false).when(svcConfig).isAuthenticateOriginalAuthData();

        ServerCnx serverCnx = spyWithClassAndConstructorArgs(ServerCnx.class, pulsar);
        ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(null).when(channelPipeline).get(PulsarChannelInitializer.TLS_HANDLER);

        SocketAddress socketAddress = new InetSocketAddress(0);
        doReturn(socketAddress).when(channel).remoteAddress();
        doReturn(channel).when(channelHandlerContext).channel();
        channelHandlerContext.channel().remoteAddress();
        serverCnx.channelActive(channelHandlerContext);

        // connect
        AuthenticationToken proxyAuthenticationToken = new AuthenticationToken(PROXY_TOKEN);
        CommandConnect connect = new CommandConnect();
        connect.setAuthMethodName(proxyAuthenticationToken.getAuthMethodName());
        connect.setAuthData(proxyAuthenticationToken.getAuthData().getCommandData().getBytes(StandardCharsets.UTF_8));
        connect.setClientVersion("test");
        connect.setProtocolVersion(1);
        connect.setOriginalPrincipal(CLIENT_PRINCIPAL);
        serverCnx.handleConnect(connect);
        assertNull(serverCnx.getOriginalAuthData());
        assertNull(serverCnx.getOriginalAuthState());
        assertEquals(serverCnx.getOriginalPrincipal(), CLIENT_PRINCIPAL);
        assertEquals(serverCnx.getAuthData().getCommandData(),
                proxyAuthenticationToken.getAuthData().getCommandData());
        assertEquals(serverCnx.getAuthRole(), PROXY_PRINCIPAL);
        assertEquals(serverCnx.getAuthState().getAuthRole(), PROXY_PRINCIPAL);

        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsarResources);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();

        // lookup
        CommandLookupTopic commandLookupTopic = new CommandLookupTopic();
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        commandLookupTopic.setTopic(topicName.toString());
        commandLookupTopic.setRequestId(1);
        serverCnx.handleLookup(commandLookupTopic);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                CLIENT_PRINCIPAL,
                serverCnx.getAuthData());
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                PROXY_PRINCIPAL,
                serverCnx.getAuthData());

        // producer
        CommandProducer commandProducer = new CommandProducer();
        commandProducer.setRequestId(1);
        commandProducer.setProducerId(1);
        commandProducer.setProducerName("test-producer");
        commandProducer.setTopic(topicName.toString());
        serverCnx.handleProducer(commandProducer);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.PRODUCE,
                CLIENT_PRINCIPAL,
                serverCnx.getAuthData());
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                PROXY_PRINCIPAL,
                serverCnx.getAuthData());

        // consumer
        CommandSubscribe commandSubscribe = new CommandSubscribe();
        commandSubscribe.setTopic(topicName.toString());
        commandSubscribe.setRequestId(1);
        commandSubscribe.setConsumerId(1);
        final String subscriptionName = "test-subscribe";
        commandSubscribe.setSubscription("test-subscribe");
        commandSubscribe.setSubType(CommandSubscribe.SubType.Shared);
        serverCnx.handleSubscribe(commandSubscribe);

        ArgumentMatcher<AuthenticationDataSource> authenticationDataSourceArgumentMatcher = arg -> {
            assertTrue(arg instanceof AuthenticationDataSubscription);
            try {
                assertEquals(arg.getCommandData(), proxyAuthenticationToken.getAuthData().getCommandData());
            } catch (PulsarClientException e) {
                fail(e.getMessage());
            }
            assertEquals(arg.getSubscription(), subscriptionName);
            return true;
        };

        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(CLIENT_PRINCIPAL), argThat(authenticationDataSourceArgumentMatcher));
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(PROXY_PRINCIPAL), argThat(authenticationDataSourceArgumentMatcher));
    }

    @Test
    public void testVerifyAuthRoleAndAuthDataFromDirectConnectionBroker() throws Exception {
        ServerCnx serverCnx = spyWithClassAndConstructorArgs(ServerCnx.class, pulsar);

        ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
        doReturn(channelPipeline).when(channel).pipeline();
        doReturn(null).when(channelPipeline).get(PulsarChannelInitializer.TLS_HANDLER);

        SocketAddress socketAddress = new InetSocketAddress(0);
        doReturn(socketAddress).when(channel).remoteAddress();
        doReturn(channel).when(channelHandlerContext).channel();
        channelHandlerContext.channel().remoteAddress();
        serverCnx.channelActive(channelHandlerContext);

        // connect
        AuthenticationToken clientAuthenticationToken = new AuthenticationToken(CLIENT_TOKEN);
        CommandConnect connect = new CommandConnect();
        connect.setAuthMethodName(clientAuthenticationToken.getAuthMethodName());
        connect.setAuthData(clientAuthenticationToken.getAuthData().getCommandData().getBytes(StandardCharsets.UTF_8));
        connect.setClientVersion("test");
        connect.setProtocolVersion(1);
        serverCnx.handleConnect(connect);
        assertNull(serverCnx.getOriginalAuthData());
        assertNull(serverCnx.getOriginalAuthState());
        assertNull(serverCnx.getOriginalPrincipal());
        assertEquals(serverCnx.getAuthData().getCommandData(),
                clientAuthenticationToken.getAuthData().getCommandData());
        assertEquals(serverCnx.getAuthRole(), CLIENT_PRINCIPAL);
        assertEquals(serverCnx.getAuthState().getAuthRole(), CLIENT_PRINCIPAL);

        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsarResources);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();

        // lookup
        CommandLookupTopic commandLookupTopic = new CommandLookupTopic();
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        commandLookupTopic.setTopic(topicName.toString());
        commandLookupTopic.setRequestId(1);
        serverCnx.handleLookup(commandLookupTopic);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.LOOKUP,
                CLIENT_PRINCIPAL,
                serverCnx.getAuthData());

        // producer
        CommandProducer commandProducer = new CommandProducer();
        commandProducer.setRequestId(1);
        commandProducer.setProducerId(1);
        commandProducer.setProducerName("test-producer");
        commandProducer.setTopic(topicName.toString());
        serverCnx.handleProducer(commandProducer);
        verify(authorizationService, times(1)).allowTopicOperationAsync(topicName, TopicOperation.PRODUCE,
                CLIENT_PRINCIPAL,
                serverCnx.getAuthData());

        // consumer
        CommandSubscribe commandSubscribe = new CommandSubscribe();
        commandSubscribe.setTopic(topicName.toString());
        commandSubscribe.setRequestId(1);
        commandSubscribe.setConsumerId(1);
        final String subscriptionName = "test-subscribe";
        commandSubscribe.setSubscription("test-subscribe");
        commandSubscribe.setSubType(CommandSubscribe.SubType.Shared);
        serverCnx.handleSubscribe(commandSubscribe);

        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(CLIENT_PRINCIPAL), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    try {
                        assertEquals(arg.getCommandData(), clientAuthenticationToken.getAuthData().getCommandData());
                    } catch (PulsarClientException e) {
                        fail(e.getMessage());
                    }
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
    }
}
