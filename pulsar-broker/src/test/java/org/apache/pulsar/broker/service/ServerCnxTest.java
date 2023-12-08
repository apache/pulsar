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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.vertx.core.impl.ConcurrentHashSet;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.auth.MockAlwaysExpiredAuthenticationProvider;
import org.apache.pulsar.broker.auth.MockAuthorizationProvider;
import org.apache.pulsar.broker.auth.MockMutableAuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.auth.MockAuthenticationProvider;
import org.apache.pulsar.broker.auth.MockMultiStageAuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.ServerCnx.State;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.service.utils.ClientChannelHelper;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.AuthMethod;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.protocol.schema.EmptyVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@SuppressWarnings("unchecked")
@Test(groups = "broker")
public class ServerCnxTest {
    protected EmbeddedChannel channel;
    private ServiceConfiguration svcConfig;
    private ServerCnx serverCnx;
    protected BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ClientChannelHelper clientChannelHelper;
    private PulsarService pulsar;
    private MetadataStoreExtended store;
    private ConfigurationCacheService configCacheService;
    private NamespaceResources namespaceResources;
    protected NamespaceService namespaceService;
    private final int currentProtocolVersion = ProtocolVersion.values()[ProtocolVersion.values().length - 1]
            .getValue();

    protected final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    private final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    private final String nonOwnedTopicName = "persistent://prop/use/ns-abc/success-not-owned-topic";
    private final String encryptionRequiredTopicName = "persistent://prop/use/ns-abc/successEncryptionRequiredTopic";
    private final String successSubName = "successSub";
    private final String nonExistentTopicName = "persistent://nonexistent-prop/nonexistent-cluster/nonexistent-namespace/successNonExistentTopic";
    private final String topicWithNonLocalCluster = "persistent://prop/usw/ns-abc/successTopic";

    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;
    private ConcurrentHashSet<EmbeddedChannel> channelsStoppedAnswerHealthCheck = new ConcurrentHashSet<>();

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        channelsStoppedAnswerHealthCheck.clear();
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        svcConfig.setClusterName("use");
        pulsar = spyWithClassAndConstructorArgs(PulsarService.class, svcConfig);
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();
        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(PulsarResources.class)).when(pulsar).getPulsarResources();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(createMockBookKeeper(executor))
            .when(pulsar).getBookKeeperClient();

        store = new ZKMetadataStore(mockZk);

        doReturn(store).when(pulsar).getLocalMetadataStore();
        doReturn(store).when(pulsar).getConfigurationMetadataStore();

        brokerService = spyWithClassAndConstructorArgs(BrokerService.class, pulsar, eventLoopGroup);
        BrokerInterceptor interceptor = mock(BrokerInterceptor.class);
        doReturn(interceptor).when(brokerService).getInterceptor();
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(executor).when(pulsar).getOrderedExecutor();

        PulsarResources pulsarResources = spyWithClassAndConstructorArgs(PulsarResources.class, store, store);
        namespaceResources = spyWithClassAndConstructorArgs(NamespaceResources.class, store, store, 30);
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();
        doReturn(pulsarResources).when(pulsar).getPulsarResources();

        namespaceService = mock(NamespaceService.class);
        doReturn(CompletableFuture.completedFuture(null)).when(namespaceService).getBundleAsync(any());
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any());
        doReturn(true).when(namespaceService).isServiceUnitActive(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespaceService).isServiceUnitActiveAsync(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespaceService).checkTopicOwnership(any());

        setupMLAsyncCallbackMocks();

        clientChannelHelper = new ClientChannelHelper();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (serverCnx != null) {
            serverCnx.close();
        }
        if (channel != null) {
            channel.close();
        }
        pulsar.close();
        brokerService.close();
        executor.shutdownNow();
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully().get();
        }
        store.close();
    }

    @Test(timeOut = 30000)
    public void testConnectCommand() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        assertTrue(getResponse() instanceof CommandConnected);
        channel.finish();
    }

    private static ByteBuf newConnect(AuthMethod authMethod, String authData, int protocolVersion) {
        BaseCommand cmd = new BaseCommand().setType(Type.CONNECT);
        cmd.setConnect()
                .setClientVersion("Pulsar Client")
                .setAuthMethod(authMethod)
                .setAuthData(authData.getBytes(StandardCharsets.UTF_8))
                .setProtocolVersion(protocolVersion);
        return Commands.serializeWithSize(cmd);
    }

    /**
     * Ensure that old clients may still connect to new servers
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testConnectCommandWithEnum() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = newConnect(AuthMethod.AuthMethodNone, "", Commands.getCurrentProtocolVersion());
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        assertTrue(getResponse() instanceof CommandConnected);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithProtocolVersion() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        CommandConnected response = (CommandConnected) getResponse();
        assertEquals(response.getProtocolVersion(), currentProtocolVersion);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testKeepAlive() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        CommandConnected response = (CommandConnected) getResponse();
        assertEquals(response.getProtocolVersion(), currentProtocolVersion);

        // Connection will be closed in 2 seconds, in the meantime give chance to run the cleanup logic
        for (int i = 0; i < 3; i++) {
            channel.runPendingTasks();
            Thread.sleep(1000);
        }

        assertFalse(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testKeepAliveNotEnforcedWithOlderClients() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", ProtocolVersion.v0.getValue(), null, null, null, null, null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        CommandConnected response = (CommandConnected) getResponse();
        // Server is responding with same version as client
        assertEquals(response.getProtocolVersion(), ProtocolVersion.v0.getValue());

        // Connection will *not* be closed in 2 seconds
        for (int i = 0; i < 3; i++) {
            channel.runPendingTasks();
            Thread.sleep(1000);
        }
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testKeepAliveBeforeHandshake() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server doesn't received a CONNECT command and should close the connection after timeout

        // Connection will be closed in 2 seconds, in the meantime give chance to run the cleanup logic
        for (int i = 0; i < 3; i++) {
            channel.runPendingTasks();
            Thread.sleep(1000);
        }

        assertFalse(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithAuthenticationPositive() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.client", null);
        channel.writeInbound(clientCommand);

        assertTrue(getResponse() instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getPrincipal(), "pass.client");
        assertTrue(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithoutOriginalAuthInfoWhenAuthenticateOriginalAuthData() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.client", "");
        channel.writeInbound(clientCommand);

        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getAuthRole(), "pass.client");
        assertEquals(serverCnx.getPrincipal(), "pass.client");
        assertNull(serverCnx.getOriginalPrincipal());
        assertTrue(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithPassingOriginalAuthData() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1, null,
                null, "client", "pass.client", authMethodName);
        channel.writeInbound(clientCommand);

        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        // Note that this value will change to the client's data if the broker sends an AuthChallenge to the
        // proxy/client. Details described here https://github.com/apache/pulsar/issues/19332.
        assertEquals(serverCnx.getAuthRole(), "pass.proxy");
        // These are all taken without verifying the auth data
        assertEquals(serverCnx.getPrincipal(), "pass.client");
        assertEquals(serverCnx.getOriginalPrincipal(), "pass.client");
        assertTrue(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithPassingOriginalAuthDataAndSetAnonymousUserRole() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        String anonymousUserRole = "admin";
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        when(authenticationService.getAnonymousUserRole()).thenReturn(Optional.of(anonymousUserRole));
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));
        svcConfig.setAnonymousUserRole(anonymousUserRole);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // When both the proxy and the broker set the anonymousUserRole option
        // the proxy will use anonymousUserRole to delegate the client's role when connecting.
        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1, null,
                null, anonymousUserRole, null, null);
        channel.writeInbound(clientCommand);

        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getAuthRole(), anonymousUserRole);
        assertEquals(serverCnx.getPrincipal(), anonymousUserRole);
        assertEquals(serverCnx.getOriginalPrincipal(), anonymousUserRole);
        assertTrue(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithPassingOriginalPrincipal() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(false);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1, null,
                null, "client", "pass.client", authMethodName);
        channel.writeInbound(clientCommand);

        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getAuthRole(), "pass.proxy");
        // These are all taken without verifying the auth data
        assertEquals(serverCnx.getPrincipal(), "client");
        assertEquals(serverCnx.getOriginalPrincipal(), "client");
        assertTrue(serverCnx.isActive());
        channel.finish();
    }

    public void testAuthChallengePrincipalChangeFails() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAlwaysExpiredAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticationRefreshCheckSeconds(30);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);
        // Don't want the keep alive task affecting which messages are handled
        serverCnx.cancelKeepAliveTask();

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.client", "");
        channel.writeInbound(clientCommand);

        Object responseConnected = getResponse();
        assertTrue(responseConnected instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getPrincipal(), "pass.client");
        assertTrue(serverCnx.isActive());

        // Trigger the ServerCnx to check if authentication is expired (it is because of our special implementation)
        // and then force channel to run the task
        channel.advanceTimeBy(30, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Object responseAuthChallenge1 = getResponse();
        assertTrue(responseAuthChallenge1 instanceof CommandAuthChallenge);

        // Respond with valid info that will both pass and be the same
        ByteBuf authResponse1 = Commands.newAuthResponse(authMethodName, AuthData.of("pass.client".getBytes()), 1, "");
        channel.writeInbound(authResponse1);

        // Trigger the ServerCnx to check if authentication is expired again
        channel.advanceTimeBy(30, TimeUnit.SECONDS);
        assertTrue(channel.hasPendingTasks(), "This test assumes there are pending tasks to run.");
        channel.runPendingTasks();
        Object responseAuthChallenge2 = getResponse();
        assertTrue(responseAuthChallenge2 instanceof CommandAuthChallenge);

        // Respond with invalid info that will pass but have a different authRole
        ByteBuf authResponse2 = Commands.newAuthResponse(authMethodName, AuthData.of("pass.client2".getBytes()), 1, "");
        channel.writeInbound(authResponse2);

        // Expect the connection to disconnect
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());

        channel.finish();
    }

    public void testAuthChallengeOriginalPrincipalChangeFails() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAlwaysExpiredAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));
        svcConfig.setAuthenticationRefreshCheckSeconds(30);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);
        // Don't want the keep alive task affecting which messages are handled
        serverCnx.cancelKeepAliveTask();

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1, null,
                null, "pass.client", "pass.client", authMethodName);
        channel.writeInbound(clientCommand);

        Object responseConnected = getResponse();
        assertTrue(responseConnected instanceof CommandConnected);
        assertEquals(serverCnx.getState(), State.Connected);
        assertEquals(serverCnx.getAuthRole(), "pass.proxy");
        // These are all taken without verifying the auth data
        assertEquals(serverCnx.getPrincipal(), "pass.client");
        assertEquals(serverCnx.getOriginalPrincipal(), "pass.client");
        assertTrue(serverCnx.isActive());

        // Trigger the ServerCnx to check if authentication is expired (it is because of our special implementation)
        // and then force channel to run the task
        channel.advanceTimeBy(30, TimeUnit.SECONDS);
        assertTrue(channel.hasPendingTasks(), "This test assumes there are pending tasks to run.");
        channel.runPendingTasks();
        Object responseAuthChallenge1 = getResponse();
        assertTrue(responseAuthChallenge1 instanceof CommandAuthChallenge);

        // Respond with valid info that will both pass and be the same
        ByteBuf authResponse1 = Commands.newAuthResponse(authMethodName, AuthData.of("pass.client".getBytes()), 1, "");
        channel.writeInbound(authResponse1);

        // Trigger the ServerCnx to check if authentication is expired again
        channel.advanceTimeBy(30, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Object responseAuthChallenge2 = getResponse();
        assertTrue(responseAuthChallenge2 instanceof CommandAuthChallenge);

        // Respond with invalid info that will pass but have a different authRole
        ByteBuf authResponse2 = Commands.newAuthResponse(authMethodName, AuthData.of("pass.client2".getBytes()), 1, "");
        channel.writeInbound(authResponse2);

        // Expect the connection to disconnect
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());

        channel.finish();
    }

    // This test is different in branch-2.11 and older because the behavior changes after branch-2.11.
    // See https://github.com/apache/pulsar/pull/19830 for additional information.
    @Test(timeOut = 30000)
    public void testConnectCommandWithDifferentRoleCombinations() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(false);
        svcConfig.setAuthorizationEnabled(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        // Invalid combinations where authData is proxy role
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.proxy", "pass.proxy", false);
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.proxy", "", false);
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.proxy", null, false);
        // Only considered valid because there is no requirement for that only a proxy role can pass
        // an original principal
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.client", "pass.proxy", true);
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.client1", "pass.client", true);
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.client", "pass.client", true);
        verifyAuthRoleAndOriginalPrincipalBehavior(authMethodName, "pass.client", "pass.client1", true);
    }

    private void verifyAuthRoleAndOriginalPrincipalBehavior(String authMethodName, String authData,
                                                            String originalPrincipal,
                                                            boolean shouldPass) throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        ByteBuf clientCommand = Commands.newConnect(authMethodName, authData, 1,null,
                null, originalPrincipal, null, null);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        if (shouldPass) {
            assertTrue(response instanceof CommandConnected);
            assertEquals(serverCnx.getState(), State.Connected);
            assertTrue(serverCnx.isActive());
        } else {
            assertTrue(response instanceof CommandError);
            assertEquals(((CommandError) response).getError(), ServerError.AuthorizationError);
            assertEquals(serverCnx.getState(), State.Failed);
        }
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithAuthenticationNegative() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        doReturn(authenticationService).when(brokerService).getAuthenticationService();
        doReturn(Optional.empty()).when(authenticationService).getAnonymousUserRole();
        doReturn(true).when(brokerService).isAuthenticationEnabled();

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Failed);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testConnectCommandWithFailingOriginalAuthData() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1,null,
                null, "client", "fail", authMethodName);
        channel.writeInbound(clientCommand);

        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandError);
        assertEquals(((CommandError) response1).getMessage(), "Unable to authenticate");
        assertEquals(serverCnx.getState(), State.Failed);
        assertFalse(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testAuthResponseWithFailingAuthData() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockMultiStageAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // Trigger connect command to result in AuthChallenge
        ByteBuf clientCommand = Commands.newConnect(authMethodName, "challenge.client", "1");
        channel.writeInbound(clientCommand);

        Object challenge1 = getResponse();
        assertTrue(challenge1 instanceof CommandAuthChallenge);
        assertEquals(serverCnx.getState(), State.Connecting);

        // Trigger another AuthChallenge to verify that code path continues to challenge
        ByteBuf authResponse1 = Commands.newAuthResponse(authMethodName, AuthData.of("challenge.client".getBytes()), 1, "1");
        channel.writeInbound(authResponse1);

        Object challenge2 = getResponse();
        assertTrue(challenge2 instanceof CommandAuthChallenge);
        assertEquals(serverCnx.getState(), State.Connecting);

        // Trigger failure
        ByteBuf authResponse2 = Commands.newAuthResponse(authMethodName, AuthData.of("fail.client".getBytes()), 1, "1");
        channel.writeInbound(authResponse2);

        Object response3 = getResponse();
        assertTrue(response3 instanceof CommandError);
        assertEquals(((CommandError) response3).getMessage(), "Do not pass");
        assertEquals(serverCnx.getState(), State.Failed);
        assertFalse(serverCnx.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testOriginalAuthDataTriggersAuthChallengeFailure() throws Exception {
        // Test verifies the current behavior in the absence of a solution for
        // https://github.com/apache/pulsar/issues/19291. When that issue is completed, we can update this test
        // to correctly verify that behavior.
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockMultiStageAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();

        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // Trigger connect command to result in AuthChallenge
        ByteBuf clientCommand = Commands.newConnect(authMethodName, "pass.proxy", 1, "1",
                "localhost", "client", "challenge.client", authMethodName);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandError);

        assertEquals(((CommandError) response).getMessage(), "Unable to authenticate");
        assertEquals(serverCnx.getState(), State.Failed);
        assertFalse(serverCnx.isActive());
        channel.finish();
    }

    // This test used to be in the ServerCnxAuthorizationTest class, but it was migrated here because the mocking
    // in that class was too extensive. There is some overlap with this test and other tests in this class. The primary
    // role of this test is verifying that the correct role and AuthenticationDataSource are passed to the
    // AuthorizationService.
    @Test
    public void testVerifyOriginalPrincipalWithAuthDataForwardedFromProxy() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.pass"));

        svcConfig.setAuthorizationProvider("org.apache.pulsar.broker.auth.MockAuthorizationProvider");
        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig,
                        pulsar.getPulsarResources());
        when(brokerService.getAuthorizationService()).thenReturn(authorizationService);
        svcConfig.setAuthorizationEnabled(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // Connect
        // This client role integrates with the MockAuthenticationProvider and MockAuthorizationProvider
        // to pass authentication and fail authorization
        String proxyRole = "pass.pass";
        String clientRole = "pass.fail";
        // Submit a failing originalPrincipal to show that it is not used at all.
        ByteBuf connect = Commands.newConnect(authMethodName, proxyRole, "test", "localhost",
                "fail.fail", clientRole, authMethodName);
        channel.writeInbound(connect);
        Object connectResponse = getResponse();
        assertTrue(connectResponse instanceof CommandConnected);
        assertEquals(serverCnx.getOriginalAuthData().getCommandData(), clientRole);
        assertEquals(serverCnx.getOriginalAuthState().getAuthRole(), clientRole);
        assertEquals(serverCnx.getOriginalPrincipal(), clientRole);
        assertEquals(serverCnx.getAuthData().getCommandData(), proxyRole);
        assertEquals(serverCnx.getAuthRole(), proxyRole);
        assertEquals(serverCnx.getAuthState().getAuthRole(), proxyRole);

        // Lookup
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        ByteBuf lookup = Commands.newLookup(topicName.toString(), false, 1);
        channel.writeInbound(lookup);
        Object lookupResponse = getResponse();
        assertTrue(lookupResponse instanceof CommandLookupTopicResponse);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getRequestId(), 1);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, proxyRole, serverCnx.getAuthData());
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, clientRole, serverCnx.getOriginalAuthData());

        // producer
        ByteBuf producer = Commands.newProducer(topicName.toString(), 1, 2, "test-producer", new HashMap<>(), false);
        channel.writeInbound(producer);
        Object producerResponse = getResponse();
        assertTrue(producerResponse instanceof CommandError);
        assertEquals(((CommandError) producerResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) producerResponse).getRequestId(), 2);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.PRODUCE, clientRole, serverCnx.getOriginalAuthData());
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, proxyRole, serverCnx.getAuthData());

        // consumer
        String subscriptionName = "test-subscribe";
        ByteBuf subscribe = Commands.newSubscribe(topicName.toString(), subscriptionName, 1, 3,
                SubType.Shared, 0, "consumer", 0);
        channel.writeInbound(subscribe);
        Object subscribeResponse = getResponse();
        assertTrue(subscribeResponse instanceof CommandError);
        assertEquals(((CommandError) subscribeResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) subscribeResponse).getRequestId(), 3);
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(clientRole), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    assertEquals(arg.getCommandData(), clientRole);
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(proxyRole), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    assertEquals(arg.getCommandData(), proxyRole);
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
    }

    @Test
    public void testDuplicateProducer() throws Exception {
        final String tName = successTopicName;
        final long producerId = 1;
        final MutableInt requestId = new MutableInt(1);
        final MutableInt epoch = new MutableInt(1);
        final Map<String, String> metadata = Collections.emptyMap();
        final String pName = "p1";
        resetChannel();
        setChannelConnected();

        // The producer register using the first connection.
        ByteBuf cmdProducer1 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        channel.writeInbound(cmdProducer1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(tName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // Verify the second producer will be reject due to the previous one still is active.
        // Every second try once, total 10 times, all requests should fail.
        ClientChannel channel2 = new ClientChannel();
        BackGroundExecutor backGroundExecutor1 = startBackgroundExecutorForEmbeddedChannel(channel);
        BackGroundExecutor autoResponseForHeartBeat = autoResponseForHeartBeat(channel, clientChannelHelper);
        BackGroundExecutor backGroundExecutor2 = startBackgroundExecutorForEmbeddedChannel(channel2.channel);
        setChannelConnected(channel2.serverCnx);

        for (int i = 0; i < 10; i++) {
            ByteBuf cmdProducer2 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                    pName, false, metadata, null, epoch.incrementAndGet(), false,
                    ProducerAccessMode.Shared, Optional.empty(), false);
            channel2.channel.writeInbound(cmdProducer2);
            Object response2 = getResponse(channel2.channel, channel2.clientChannelHelper);
            assertTrue(response2 instanceof CommandError);
            assertEquals(topicRef.getProducers().size(), 1);
            assertTrue(channel.isActive());
            Thread.sleep(500);
        }

        // cleanup.
        autoResponseForHeartBeat.close();
        backGroundExecutor1.close();
        backGroundExecutor2.close();
        channel.finish();
        channel2.close();
    }

    @Test
    public void testProducerChangeSocket() throws Exception {
        final String tName = successTopicName;
        final long producerId = 1;
        final MutableInt requestId = new MutableInt(1);
        final MutableInt epoch = new MutableInt(1);
        final Map<String, String> metadata = Collections.emptyMap();
        final String pName = "p1";
        resetChannel();
        setChannelConnected();

        // The producer register using the first connection.
        ByteBuf cmdProducer1 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        channel.writeInbound(cmdProducer1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(tName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // Verify the second producer using a new connection will override the producer who using a stopped channel.
        channelsStoppedAnswerHealthCheck.add(channel);
        ClientChannel channel2 = new ClientChannel();
        BackGroundExecutor backGroundExecutor1 = startBackgroundExecutorForEmbeddedChannel(channel);
        BackGroundExecutor backGroundExecutor2 = startBackgroundExecutorForEmbeddedChannel(channel2.channel);
        setChannelConnected(channel2.serverCnx);

        ByteBuf cmdProducer2 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        channel2.channel.writeInbound(cmdProducer2);
        Object response2 = getResponse(channel2.channel, channel2.clientChannelHelper);
        assertTrue(response2 instanceof CommandProducerSuccess);
        assertEquals(topicRef.getProducers().size(), 1);

        // cleanup.
        channelsStoppedAnswerHealthCheck.clear();
        backGroundExecutor1.close();
        backGroundExecutor2.close();
        channel.finish();
        channel2.close();
    }

    @Test
    public void testHandleConsumerAfterClientChannelInactive() throws Exception {
        final String tName = successTopicName;
        final long consumerId = 1;
        final MutableInt requestId = new MutableInt(1);
        final String sName = successSubName;
        final String cName1 = ConsumerName.generateRandomName();
        final String cName2 = ConsumerName.generateRandomName();
        resetChannel();
        setChannelConnected();

        // The producer register using the first connection.
        ByteBuf cmdSubscribe1 = Commands.newSubscribe(tName, sName, consumerId, requestId.incrementAndGet(),
                SubType.Exclusive, 0, cName1, 0);
        channel.writeInbound(cmdSubscribe1);
        assertTrue(getResponse() instanceof CommandSuccess);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(tName).get();
        assertNotNull(topicRef);
        assertNotNull(topicRef.getSubscription(sName).getConsumers());
        assertEquals(topicRef.getSubscription(sName).getConsumers().size(), 1);
        assertEquals(topicRef.getSubscription(sName).getConsumers().iterator().next().consumerName(), cName1);

        // Verify the second producer using a new connection will override the producer who using a stopped channel.
        channelsStoppedAnswerHealthCheck.add(channel);
        ClientChannel channel2 = new ClientChannel();
        setChannelConnected(channel2.serverCnx);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            channel.runPendingTasks();
            ByteBuf cmdSubscribe2 = Commands.newSubscribe(tName, sName, consumerId, requestId.incrementAndGet(),
                    SubType.Exclusive, 0, cName2, 0);
            channel2.channel.writeInbound(cmdSubscribe2);
            assertTrue(getResponse(channel2.channel, channel2.clientChannelHelper) instanceof CommandSuccess);
            assertEquals(topicRef.getSubscription(sName).getConsumers().size(), 1);
            assertEquals(topicRef.getSubscription(sName).getConsumers().iterator().next().consumerName(), cName2);
        });

        // cleanup.
        channel.finish();
        channel2.close();
    }

    /**
     * When a channel typed "EmbeddedChannel", once we call channel.execute(runnable), there is no background thread
     * to run it.
     * So starting a background thread to trigger the tasks in the queue.
     */
    private BackGroundExecutor startBackgroundExecutorForEmbeddedChannel(final EmbeddedChannel channel) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture scheduledFuture = executor.scheduleWithFixedDelay(() -> {
            channel.runPendingTasks();
        }, 100, 100, TimeUnit.MILLISECONDS);
        return new BackGroundExecutor(executor, scheduledFuture);
    }

    /**
     * Auto answer `Pong` for the `Cmd-Ping`.
     * Node: This will result in additional threads pop Command from the Command queue, so do not call this
     * method if the channel needs to accept other Command.
     */
    private BackGroundExecutor autoResponseForHeartBeat(EmbeddedChannel channel,
                                                        ClientChannelHelper clientChannelHelper) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture scheduledFuture = executor.scheduleWithFixedDelay(() -> {
            tryPeekResponse(channel, clientChannelHelper);
        }, 100, 100, TimeUnit.MILLISECONDS);
        return new BackGroundExecutor(executor, scheduledFuture);
    }

    @AllArgsConstructor
    private static class BackGroundExecutor implements Closeable {

        private ScheduledExecutorService executor;

        private ScheduledFuture scheduledFuture;

        @Override
        public void close() throws IOException {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
            executor.shutdown();
        }
    }

    private class ClientChannel implements Closeable {
        private ClientChannelHelper clientChannelHelper = new ClientChannelHelper();
        private ServerCnx serverCnx = new ServerCnx(pulsar);
        private EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance(),
                new LengthFieldBasedFrameDecoder(
                        5 * 1024 * 1024,
                        0,
                        4,
                        0,
                        4),
                serverCnx);
        public ClientChannel() {
            serverCnx.setAuthRole("");
        }
        public void close(){
            if (channel != null && channel.isActive()) {
                serverCnx.close();
                channel.close();
            }
        }
    }

    @Test
    public void testHandleProducer() throws Exception {
        final String tName = "persistent://public/default/test-topic";
        final long producerId = 1;
        final MutableInt requestId = new MutableInt(1);
        final MutableInt epoch = new MutableInt(1);
        final Map<String, String> metadata = Collections.emptyMap();
        final String pName = "p1";
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // connect.
        ByteBuf cConnect = Commands.newConnect("none", "", null);
        channel.writeInbound(cConnect);
        assertEquals(serverCnx.getState(), State.Connected);
        assertTrue(getResponse() instanceof CommandConnected);

        // There is an in-progress producer registration.
        ByteBuf cProducer1 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        CompletableFuture existingFuture1 = new CompletableFuture();
        serverCnx.getProducers().put(producerId, existingFuture1);
        channel.writeInbound(cProducer1);
        Object response1 = getResponse();
        assertTrue(response1 instanceof CommandError);
        CommandError error1 = (CommandError) response1;
        assertEquals(error1.getError().toString(), ServerError.ServiceNotReady.toString());
        assertTrue(error1.getMessage().contains("already present on the connection"));

        // There is a failed registration.
        ByteBuf cProducer2 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        CompletableFuture existingFuture2 = new CompletableFuture();
        existingFuture2.completeExceptionally(new BrokerServiceException.ProducerBusyException("123"));
        serverCnx.getProducers().put(producerId, existingFuture2);

        channel.writeInbound(cProducer2);
        Object response2 = getResponse();
        assertTrue(response2 instanceof CommandError);
        CommandError error2 = (CommandError) response2;
        assertEquals(error2.getError().toString(), ServerError.ProducerBusy.toString());
        assertTrue(error2.getMessage().contains("already failed to register present on the connection"));

        // There is an successful registration.
        ByteBuf cProducer3 = Commands.newProducer(tName, producerId, requestId.incrementAndGet(),
                pName, false, metadata, null, epoch.incrementAndGet(), false,
                ProducerAccessMode.Shared, Optional.empty(), false);
        CompletableFuture existingFuture3 = new CompletableFuture();
        Producer serviceProducer =
                mock(Producer.class);
        when(serviceProducer.getProducerName()).thenReturn(pName);
        when(serviceProducer.getSchemaVersion()).thenReturn(new EmptyVersion());
        existingFuture3.complete(serviceProducer);
        serverCnx.getProducers().put(producerId, existingFuture3);

        channel.writeInbound(cProducer3);
        Object response3 = getResponse();
        assertTrue(response3 instanceof CommandProducerSuccess);
        CommandProducerSuccess cProducerSuccess = (CommandProducerSuccess) response3;
        assertEquals(cProducerSuccess.getProducerName(), pName);

        // cleanup.
        channel.finish();
    }

    // This test used to be in the ServerCnxAuthorizationTest class, but it was migrated here because the mocking
    // in that class was too extensive. There is some overlap with this test and other tests in this class. The primary
    // role of this test is verifying that the correct role and AuthenticationDataSource are passed to the
    // AuthorizationService.
    public void testVerifyOriginalPrincipalWithoutAuthDataForwardedFromProxy() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(false);
        svcConfig.setProxyRoles(Collections.singleton("pass.pass"));

        svcConfig.setAuthorizationProvider("org.apache.pulsar.broker.auth.MockAuthorizationProvider");
        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig,
                        pulsar.getPulsarResources());
        when(brokerService.getAuthorizationService()).thenReturn(authorizationService);
        svcConfig.setAuthorizationEnabled(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // Connect
        // This client role integrates with the MockAuthenticationProvider and MockAuthorizationProvider
        // to pass authentication and fail authorization
        String proxyRole = "pass.pass";
        String clientRole = "pass.fail";
        ByteBuf connect = Commands.newConnect(authMethodName, proxyRole, "test", "localhost",
                clientRole, null, null);
        channel.writeInbound(connect);
        Object connectResponse = getResponse();
        assertTrue(connectResponse instanceof CommandConnected);
        assertNull(serverCnx.getOriginalAuthData());
        assertNull(serverCnx.getOriginalAuthState());
        assertEquals(serverCnx.getOriginalPrincipal(), clientRole);
        assertEquals(serverCnx.getAuthData().getCommandData(), proxyRole);
        assertEquals(serverCnx.getAuthRole(), proxyRole);
        assertEquals(serverCnx.getAuthState().getAuthRole(), proxyRole);

        // Lookup
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        ByteBuf lookup = Commands.newLookup(topicName.toString(), false, 1);
        channel.writeInbound(lookup);
        Object lookupResponse = getResponse();
        assertTrue(lookupResponse instanceof CommandLookupTopicResponse);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getRequestId(), 1);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, proxyRole, serverCnx.getAuthData());
        // This test is an example of https://github.com/apache/pulsar/issues/19332. Essentially, we're passing
        // the proxy's auth data because it is all we have. This test should be updated when we resolve that issue.
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, clientRole, serverCnx.getAuthData());

        // producer
        ByteBuf producer = Commands.newProducer(topicName.toString(), 1, 2, "test-producer", new HashMap<>(), false);
        channel.writeInbound(producer);
        Object producerResponse = getResponse();
        assertTrue(producerResponse instanceof CommandError);
        assertEquals(((CommandError) producerResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) producerResponse).getRequestId(), 2);
        // See https://github.com/apache/pulsar/issues/19332 for justification of this assertion.
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.PRODUCE, clientRole, serverCnx.getAuthData());
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, proxyRole, serverCnx.getAuthData());

        // consumer
        String subscriptionName = "test-subscribe";
        ByteBuf subscribe = Commands.newSubscribe(topicName.toString(), subscriptionName, 1, 3,
                SubType.Shared, 0, "consumer", 0);
        channel.writeInbound(subscribe);
        Object subscribeResponse = getResponse();
        assertTrue(subscribeResponse instanceof CommandError);
        assertEquals(((CommandError) subscribeResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) subscribeResponse).getRequestId(), 3);
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(clientRole), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    // We assert that the role is clientRole and commandData is proxyRole due to
                    // https://github.com/apache/pulsar/issues/19332.
                    assertEquals(arg.getCommandData(), proxyRole);
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(proxyRole), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    assertEquals(arg.getCommandData(), proxyRole);
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
    }

    // This test used to be in the ServerCnxAuthorizationTest class, but it was migrated here because the mocking
    // in that class was too extensive. There is some overlap with this test and other tests in this class. The primary
    // role of this test is verifying that the correct role and AuthenticationDataSource are passed to the
    // AuthorizationService.
    @Test
    public void testVerifyAuthRoleAndAuthDataFromDirectConnectionBroker() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);

        svcConfig.setAuthorizationProvider("org.apache.pulsar.broker.auth.MockAuthorizationProvider");
        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig,
                        pulsar.getPulsarResources());
        when(brokerService.getAuthorizationService()).thenReturn(authorizationService);
        svcConfig.setAuthorizationEnabled(true);

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // connect
        // This client role integrates with the MockAuthenticationProvider and MockAuthorizationProvider
        // to pass authentication and fail authorization
        String clientRole = "pass.fail";
        ByteBuf connect = Commands.newConnect(authMethodName, clientRole, "test");
        channel.writeInbound(connect);

        Object connectResponse = getResponse();
        assertTrue(connectResponse instanceof CommandConnected);
        assertNull(serverCnx.getOriginalAuthData());
        assertNull(serverCnx.getOriginalAuthState());
        assertNull(serverCnx.getOriginalPrincipal());
        assertEquals(serverCnx.getAuthData().getCommandData(), clientRole);
        assertEquals(serverCnx.getAuthRole(), clientRole);
        assertEquals(serverCnx.getAuthState().getAuthRole(), clientRole);

        // lookup
        TopicName topicName = TopicName.get("persistent://public/default/test-topic");
        ByteBuf lookup = Commands.newLookup(topicName.toString(), false, 1);
        channel.writeInbound(lookup);
        Object lookupResponse = getResponse();
        assertTrue(lookupResponse instanceof CommandLookupTopicResponse);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandLookupTopicResponse) lookupResponse).getRequestId(), 1);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, clientRole, serverCnx.getAuthData());

        // producer
        ByteBuf producer = Commands.newProducer(topicName.toString(), 1, 2, "test-producer", new HashMap<>(), false);
        channel.writeInbound(producer);
        Object producerResponse = getResponse();
        assertTrue(producerResponse instanceof CommandError);
        assertEquals(((CommandError) producerResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) producerResponse).getRequestId(), 2);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.PRODUCE, clientRole, serverCnx.getAuthData());

        // consumer
        String subscriptionName = "test-subscribe";
        ByteBuf subscribe = Commands.newSubscribe(topicName.toString(), subscriptionName, 1, 3,
                SubType.Shared, 0, "consumer", 0);
        channel.writeInbound(subscribe);
        Object subscribeResponse = getResponse();
        assertTrue(subscribeResponse instanceof CommandError);
        assertEquals(((CommandError) subscribeResponse).getError(), ServerError.AuthorizationError);
        assertEquals(((CommandError) subscribeResponse).getRequestId(), 3);
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME),
                eq(clientRole), argThat(arg -> {
                    assertTrue(arg instanceof AuthenticationDataSubscription);
                    assertEquals(arg.getCommandData(), clientRole);
                    assertEquals(arg.getSubscription(), subscriptionName);
                    return true;
                }));
    }

    @Test
    public void testRefreshOriginalPrincipalWithAuthDataForwardedFromProxy() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockMutableAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setAuthenticateOriginalAuthData(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.proxy"));

        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        String proxyRole = "pass.proxy";
        String clientRole = "pass.client";
        ByteBuf connect = Commands.newConnect(authMethodName, proxyRole, "test", "localhost",
                clientRole, clientRole, authMethodName);
        channel.writeInbound(connect);
        Object connectResponse = getResponse();
        assertTrue(connectResponse instanceof CommandConnected);
        assertEquals(serverCnx.getOriginalAuthData().getCommandData(), clientRole);
        assertEquals(serverCnx.getOriginalAuthState().getAuthRole(), clientRole);
        assertEquals(serverCnx.getOriginalPrincipal(), clientRole);
        assertEquals(serverCnx.getAuthData().getCommandData(), proxyRole);
        assertEquals(serverCnx.getAuthRole(), proxyRole);
        assertEquals(serverCnx.getAuthState().getAuthRole(), proxyRole);

        // Request refreshing the original auth.
        // Expected:
        // 1. Original role and original data equals to "pass.RefreshOriginAuthData".
        // 2. The broker disconnects the client, because the new role doesn't equal the old role.
        String newClientRole = "pass.RefreshOriginAuthData";
        ByteBuf refreshAuth = Commands.newAuthResponse(authMethodName,
                AuthData.of(newClientRole.getBytes(StandardCharsets.UTF_8)), 0, "test");
        channel.writeInbound(refreshAuth);

        assertEquals(serverCnx.getOriginalAuthData().getCommandData(), newClientRole);
        assertEquals(serverCnx.getOriginalAuthState().getAuthRole(), newClientRole);
        assertEquals(serverCnx.getAuthData().getCommandData(), proxyRole);
        assertEquals(serverCnx.getAuthRole(), proxyRole);
        assertEquals(serverCnx.getAuthState().getAuthRole(), proxyRole);

        assertFalse(channel.isOpen());
        assertFalse(channel.isActive());
    }

    @Test(timeOut = 30000)
    public void testProducerCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        // test PRODUCER success case
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // test PRODUCER error case
        clientCommand = Commands.newProducer(failTopicName, 2, 2,
                "prod-name-2", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        assertTrue(getResponse() instanceof CommandError);
        assertFalse(pulsar.getBrokerService().getTopicReference(failTopicName).isPresent());

        channel.finish();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test(timeOut = 5000)
    public void testDuplicateConcurrentProducerCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        CompletableFuture<Topic> delayFuture = new CompletableFuture<>();
        doReturn(delayFuture).when(brokerService).getOrCreateTopic(any(String.class));
        // Create producer first time
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        // Create producer second time
        clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandError);
        CommandError error = (CommandError) response;
        assertEquals(error.getError(), ServerError.ServiceNotReady);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerOnNotOwnedTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Force the case where the broker doesn't own any topic
        doReturn(CompletableFuture.completedFuture(false)).when(namespaceService)
                .isServiceUnitActiveAsync(any(TopicName.class));

        // test PRODUCER failure case
        ByteBuf clientCommand = Commands.newProducer(nonOwnedTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertEquals(response.getClass(), CommandError.class);

        CommandError errorResponse = (CommandError) response;
        assertEquals(errorResponse.getError(), ServerError.ServiceNotReady);

        assertFalse(pulsar.getBrokerService().getTopicReference(nonOwnedTopicName).isPresent());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerCommandWithAuthorizationPositive() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService).allowTopicOperationAsync(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        resetChannel();
        setChannelConnected();

        // test PRODUCER success case
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertEquals(getResponse().getClass(), CommandProducerSuccess.class);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        channel.finish();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test(timeOut = 30000)
    public void testNonExistentTopic() throws Exception {
        AuthorizationService authorizationService = spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsar.getPulsarResources());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        svcConfig.setAuthorizationEnabled(true);
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spyWithClassAndConstructorArgs(PulsarAuthorizationProvider.class, svcConfig,
                pulsar.getPulsarResources());
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(newProducerCmd);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();

        // Test consumer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newSubscribeCmd = Commands.newSubscribe(nonExistentTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */, 0);
        channel.writeInbound(newSubscribeCmd);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testClusterAccess() throws Exception {
        svcConfig.setAuthorizationEnabled(true);
        AuthorizationService authorizationService = spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsar.getPulsarResources());
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spyWithClassAndConstructorArgs(PulsarAuthorizationProvider.class, svcConfig,
                pulsar.getPulsarResources());
        providerField.set(authorizationService, authorizationProvider);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider).validateTenantAdminAccess(Mockito.anyString(), Mockito.any(), Mockito.any());
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider).checkPermission(any(TopicName.class), Mockito.anyString(),
                any(AuthAction.class));

        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        resetChannel();
        setChannelConnected();
        clientCommand = Commands.newProducer(topicWithNonLocalCluster, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testNonExistentTopicSuperUserAccess() throws Exception {
        AuthorizationService authorizationService = spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig, pulsar.getPulsarResources());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spyWithClassAndConstructorArgs(PulsarAuthorizationProvider.class, svcConfig, pulsar.getPulsarResources());
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(newProducerCmd);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        channel.finish();

        // Test consumer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newSubscribeCmd = Commands.newSubscribe(nonExistentTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(newSubscribeCmd);
        topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName).get();
        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getSubscription(successSubName).getDispatcher().isConsumerConnected());
        assertTrue(getResponse() instanceof CommandSuccess);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerCommandWithAuthorizationNegative() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService).allowTopicOperationAsync(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn("prod1").when(brokerService).generateUniqueProducerName();
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                null, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test SEND success
        sendMessage();

        assertTrue(getResponse() instanceof CommandSendReceipt);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommandBeforeCreatingProducer() throws Exception {
        resetChannel();
        setChannelConnected();

        // test SEND before producer is created
        sendMessage();

        // Then expect channel to close
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommandAfterBrokerClosedProducer() throws Exception {
        resetChannel();
        setChannelConnected();
        setConnectionVersion(ProtocolVersion.v5.getValue());
        serverCnx.cancelKeepAliveTask();

        String producerName = "my-producer";

        ByteBuf clientCommand1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // Call disconnect method on producer to trigger activity similar to unloading
        Producer producer = serverCnx.getProducers().get(1).get();
        assertNotNull(producer);
        producer.disconnect();
        channel.runPendingTasks();
        assertTrue(getResponse() instanceof CommandCloseProducer);

        // Send message and expect no response
        sendMessage();

        // Move clock forward to trigger scheduled clean up task
        channel.advanceTimeBy(svcConfig.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertTrue(channel.outboundMessages().isEmpty());
        assertTrue(channel.isActive());

        // Send message and expect closed connection
        sendMessage();

        // Then expect channel to close
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testBrokerClosedProducerClientRecreatesProducerThenSendCommand() throws Exception {
        resetChannel();
        setChannelConnected();
        setConnectionVersion(ProtocolVersion.v5.getValue());
        serverCnx.cancelKeepAliveTask();

        String producerName = "my-producer";

        ByteBuf clientCommand1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // Call disconnect method on producer to trigger activity similar to unloading
        Producer producer = serverCnx.getProducers().get(1).get();
        assertNotNull(producer);
        producer.disconnect();
        channel.runPendingTasks();
        assertTrue(getResponse() instanceof CommandCloseProducer);

        // Send message and expect no response
        sendMessage();

        assertTrue(channel.outboundMessages().isEmpty());

        // Move clock forward to trigger scheduled clean up task
        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // Send message and expect success
        sendMessage();

        assertTrue(getResponse() instanceof CommandSendReceipt);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testClientClosedProducerThenSendsMessageAndGetsClosed() throws Exception {
        resetChannel();
        setChannelConnected();
        setConnectionVersion(ProtocolVersion.v5.getValue());
        serverCnx.cancelKeepAliveTask();

        String producerName = "my-producer";

        ByteBuf clientCommand1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        ByteBuf closeProducer = Commands.newCloseProducer(1,2);
        channel.writeInbound(closeProducer);
        assertTrue(getResponse() instanceof CommandSuccess);

        // Send message and get disconnected
        sendMessage();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());
        channel.finish();
    }

    private void sendMessage() {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        ByteBuf clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1,
                ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();
    }

    @Test(timeOut = 30000)
    public void testUseSameProducerName() throws Exception {
        resetChannel();
        setChannelConnected();

        String producerName = "my-producer";

        ByteBuf clientCommand1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        ByteBuf clientCommand2 = Commands.newProducer(successTopicName, 2 /* producer id */, 2 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand2);
        assertTrue(getResponse() instanceof CommandError);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testRecreateSameProducer() throws Exception {
        resetChannel();
        setChannelConnected();

        // Recreating a producer with the same id should succeed

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer1);

        // Producer create succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 2 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);

        // 2nd producer create succeeds as well
        response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 2);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertTrue(channel.outboundMessages().isEmpty());
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeMultipleTimes() throws Exception {
        resetChannel();
        setChannelConnected();

        Object response;

        // Sending multiple subscribe commands for the same consumer should succeed

        ByteBuf subscribe1 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe1);

        // 1st subscribe succeeds
        response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 1);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 2 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe2);

        // 2nd subscribe succeeds
        response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testDuplicateConcurrentSubscribeCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        CompletableFuture<Topic> delayFuture = new CompletableFuture<>();
        doReturn(delayFuture).when(brokerService).getOrCreateTopic(any(String.class));
        // Create subscriber first time
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);

        BackGroundExecutor backGroundExecutor = startBackgroundExecutorForEmbeddedChannel(channel);

        // Create producer second time
        clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 2 /* consumer id */, 2 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);

        Awaitility.await().untilAsserted(() -> {
            Object response = getResponse();
            assertTrue(response instanceof CommandError, "Response is not CommandError but " + response);
            CommandError error = (CommandError) response;
            assertEquals(error.getError(), ServerError.ConsumerBusy);
        });

        // cleanup.
        backGroundExecutor.close();
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testCreateProducerTimeout() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicFuture = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFuture.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);

        // Complete the topic opening: It will make 2nd producer creation successful
        openTopicFuture.get().run();

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // 2nd producer will be successfully created as topic is open by then
        response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 3);

        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testCreateProducerTimeoutThenCreateSameNamedProducerShouldFail() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicFuture = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFuture.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)
        // Then, when another producer is created with the same name, it should fail. Because we only have one
        // channel here, we just use a different producer id

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);

        // Complete the topic opening: It will make 2nd producer creation successful
        openTopicFuture.get().run();

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // 2nd producer will be successfully created as topic is open by then
        response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 3);

        // Send create command after getting the CommandProducerSuccess to ensure correct ordering
        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 2 /* producer id */, 4 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer3);

        // 3nd producer will fail
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 4);

        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000, enabled = false)
    public void testCreateProducerMultipleTimeouts() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CountDownLatch topicCreationDelayLatch = new CountDownLatch(1);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                topicCreationDelayLatch.await();

                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer1 = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);

        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 1 /* producer id */, 4 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer3);

        ByteBuf createProducer4 = Commands.newProducer(successTopicName, 1 /* producer id */, 5 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer4);

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // Now allow topic creation to complete
        topicCreationDelayLatch.countDown();

        // 1st producer it's not acked

        // 2nd producer fails
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 3);

        // 3rd producer fails
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 4);

        // 4nd producer fails
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 5);

        Thread.sleep(100);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertTrue(channel.outboundMessages().isEmpty());
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000, skipFailedInvocations = true)
    public void testCreateProducerBookieTimeout() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openFailedTopic = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openFailedTopic.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create a failure producer which will timeout creation after 100msec
        // 2. close producer
        // 3. Recreate producer (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and create producer again.

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(failTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer2);

        // Now the topic gets opened.. It will make 2nd producer creation successful
        openFailedTopic.get().run();

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // 2nd producer success as topic is opened
        response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 3);

        // Wait till the failtopic timeout interval
        Thread.sleep(500);
        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 1 /* producer id */, 4 /* request id */,
                producerName, Collections.emptyMap(), false);
        channel.writeInbound(createProducer3);

        // 3rd producer succeeds because 2nd is already connected
        response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 4);

        Thread.sleep(500);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertTrue(channel.outboundMessages().isEmpty());
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeTimeout() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicTask = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicTask.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            });

            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe
        // 2. close consumer (when the timeout is triggered, which may be before the consumer was created on the broker)
        // 3. Subscribe (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout

        ByteBuf subscribe1 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe1);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 3 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe2);

        ByteBuf subscribe3 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 4 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe3);

        ByteBuf subscribe4 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 5 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe4);

        openTopicTask.get().run();

        Object response;

        synchronized (this) {

            // All other subscribe should fail
            response = getResponse();
            assertEquals(response.getClass(), CommandError.class);
            assertEquals(((CommandError) response).getRequestId(), 3);

            response = getResponse();
            assertEquals(response.getClass(), CommandError.class);
            assertEquals(((CommandError) response).getRequestId(), 4);

            response = getResponse();
            assertEquals(response.getClass(), CommandError.class);
            assertEquals(((CommandError) response).getRequestId(), 5);

            // We should receive response for 1st producer, since it was not cancelled by the close
            Awaitility.await().untilAsserted(() -> assertFalse(channel.outboundMessages().isEmpty()));

            assertTrue(channel.isActive());
            response = getResponse();
            assertEquals(response.getClass(), CommandSuccess.class);
            assertEquals(((CommandSuccess) response).getRequestId(), 1);
        }

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeBookieTimeout() throws Exception {
        resetChannel();
        setChannelConnected();

        // Delay the topic creation in a deterministic way
        CompletableFuture<Runnable> openTopicSuccess = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicSuccess.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        CompletableFuture<Runnable> openTopicFail = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFail.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe against failtopic which will fail after 100msec
        // 2. close consumer
        // 3. Resubscribe (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and subscribe again.

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout
        ByteBuf subscribe1 = Commands.newSubscribe(failTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe1);

        ByteBuf closeConsumer = Commands.newCloseConsumer(1 /* consumer id */, 2 /* request id */ );
        channel.writeInbound(closeConsumer);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 3 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe2);

        openTopicFail.get().run();

        Object response;

        // Close succeeds
        response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // Subscribe fails
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 3);

        Awaitility.await().until(() -> !serverCnx.hasConsumer(1));

        ByteBuf subscribe3 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 4 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(subscribe3);

        openTopicSuccess.get().run();

        // Subscribe succeeds
        response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 4);

        Thread.sleep(100);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertTrue(channel.outboundMessages().isEmpty());
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeCommand() throws Exception {
        final String failSubName = "failSub";

        resetChannel();
        setChannelConnected();
        doReturn(false).when(brokerService).isAuthenticationEnabled();
        doReturn(false).when(brokerService).isAuthorizationEnabled();
        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getSubscription(successSubName).getDispatcher().isConsumerConnected());

        // test SUBSCRIBE on topic creation success and cursor failure
        clientCommand = Commands.newSubscribe(successTopicName, failSubName, 2, 2, SubType.Exclusive,
                0, "test", 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        // test SUBSCRIBE on topic creation failure
        clientCommand = Commands.newSubscribe(failTopicName, successSubName, 3, 3, SubType.Exclusive,
                0, "test", 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        assertEquals(getResponse().getClass(), CommandError.class);

        // Server will not close the connection
        assertTrue(channel.isOpen());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testUnsupportedBatchMsgSubscribeCommand() throws Exception {
        final String failSubName = "failSub";

        resetChannel();
        setChannelConnected();
        setConnectionVersion(ProtocolVersion.v3.getValue());
        doReturn(false).when(brokerService).isAuthenticationEnabled();
        doReturn(false).when(brokerService).isAuthorizationEnabled();
        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0 /* priority */,
                "test" /* consumer name */, 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();
        topicRef.markBatchMessagePublished();
        // test SUBSCRIBE on topic and cursor creation success
        clientCommand = Commands.newSubscribe(successTopicName, failSubName, 2, 2, SubType.Exclusive, 0 /* priority */,
                "test" /* consumer name */, 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        Object response = getResponse();
        assertTrue(response instanceof CommandError);
        assertEquals(ServerError.UnsupportedVersionError, ((CommandError) response).getError());

        // Server will not close the connection
        assertTrue(channel.isOpen());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeCommandWithAuthorizationPositive() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService).allowTopicOperationAsync(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        resetChannel();
        setChannelConnected();

        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);

        assertTrue(getResponse() instanceof CommandSuccess);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeCommandWithAuthorizationNegative() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService).allowTopicOperationAsync(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        resetChannel();
        setChannelConnected();

        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */, 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testAckCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, successSubName, 1 /* consumer id */,
                1 /*
                   * request id
                   */, SubType.Exclusive, 0, "test" /* consumer name */, 0 /*avoid reseting cursor*/);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PositionImpl pos = new PositionImpl(0, 0);

        clientCommand = Commands.newAck(1 /* consumer id */, pos.getLedgerId(), pos.getEntryId(), null, AckType.Individual,
                                        null, Collections.emptyMap(), -1);
        channel.writeInbound(clientCommand);

        // verify nothing is sent out on the wire after ack
        assertNull(channel.outboundMessages().peek());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testFlowCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, successSubName, //
                1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */,
                0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        clientCommand = Commands.newFlow(1 /* consumer id */, 1 /* message permits */);
        channel.writeInbound(clientCommand);

        // cursor is mocked
        // verify nothing is sent out on the wire after ack
        assertNull(channel.outboundMessages().peek());
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerSuccessOnEncryptionRequiredTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Set encryption_required to true
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterSubscribeRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = Maps.newHashMap();
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(namespaceResources)
                .getPoliciesAsync(TopicName.get(encryptionRequiredTopicName).getNamespaceObject());

        // test success case: encrypted producer can connect
        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "encrypted-producer", true, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerFailureOnEncryptionRequiredTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Set encryption_required to true
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterSubscribeRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = Maps.newHashMap();
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(namespaceResources)
                .getPoliciesAsync(TopicName.get(encryptionRequiredTopicName).getNamespaceObject());

        // test failure case: unencrypted producer cannot connect
        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 2 /* producer id */, 2 /* request id */,
                "unencrypted-producer", false, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        CommandError errorResponse = (CommandError) response;
        assertEquals(errorResponse.getError(), ServerError.MetadataError);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 0);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerFailureOnEncryptionRequiredOnBroker() throws Exception {
        // (a) Set encryption-required at broker level
        pulsar.getConfig().setEncryptionRequireOnProducer(true);
        resetChannel();
        setChannelConnected();

        // (b) Set encryption_required to false on policy
        Policies policies = mock(Policies.class);
        // Namespace policy doesn't require encryption
        policies.encryption_required = false;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterSubscribeRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        policies.clusterDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = Maps.newHashMap();
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(namespaceResources)
                .getPoliciesAsync(TopicName.get(encryptionRequiredTopicName).getNamespaceObject());

        // test failure case: unencrypted producer cannot connect
        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 2 /* producer id */, 2 /* request id */,
                "unencrypted-producer", false, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        CommandError errorResponse = (CommandError) response;
        assertEquals(errorResponse.getError(), ServerError.MetadataError);
        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(encryptionRequiredTopicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 0);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendSuccessOnEncryptionRequiredTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Set encryption_required to true
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterSubscribeRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = Maps.newHashMap();
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(namespaceResources)
                .getPoliciesAsync(TopicName.get(encryptionRequiredTopicName).getNamespaceObject());

        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", true, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test success case: encrypted messages can be published
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        messageMetadata.addEncryptionKey()
                .setKey("testKey")
                .setValue("testVal".getBytes());
        ByteBuf data = Unpooled.buffer(1024);

        clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();
        assertTrue(getResponse() instanceof CommandSendReceipt);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendFailureOnEncryptionRequiredTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Set encryption_required to true
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        policies.clusterSubscribeRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = Maps.newHashMap();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = Maps.newHashMap();
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(namespaceResources)
                .getPoliciesAsync(TopicName.get(encryptionRequiredTopicName).getNamespaceObject());

        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", true, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test failure case: unencrypted messages cannot be published
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();
        assertTrue(getResponse() instanceof CommandSendError);
        channel.finish();
    }

    protected void resetChannel() throws Exception {
        int MaxMessageSize = 5 * 1024 * 1024;
        if (channel != null && channel.isActive()) {
            serverCnx.close();
            channel.close().get();
        }
        serverCnx = new ServerCnx(pulsar);
        serverCnx.setAuthRole("");
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(
                MaxMessageSize,
                0,
                4,
                0,
                4),
                (ChannelHandler) serverCnx);
    }

    protected void setChannelConnected() throws Exception {
        setChannelConnected(serverCnx);
    }

    protected void setChannelConnected(ServerCnx serverCnx) throws Exception {
        Field channelState = ServerCnx.class.getDeclaredField("state");
        channelState.setAccessible(true);
        channelState.set(serverCnx, State.Connected);
    }

    private void setConnectionVersion(int version) throws Exception {
        PulsarHandler cnx = serverCnx;
        Field versionField = PulsarHandler.class.getDeclaredField("remoteEndpointProtocolVersion");
        versionField.setAccessible(true);
        versionField.set(cnx, version);
    }

    protected Object getResponse() throws Exception {
        return getResponse(channel, clientChannelHelper);
    }

    protected Object getResponse(EmbeddedChannel channel, ClientChannelHelper clientChannelHelper) throws Exception {
        // Wait at most for 10s to get a response
        final long sleepTimeMs = 10;
        final long iterations = TimeUnit.SECONDS.toMillis(10) / sleepTimeMs;
        for (int i = 0; i < iterations; i++) {
            if (!channel.outboundMessages().isEmpty()) {
                Object outObject = channel.outboundMessages().remove();
                Object cmd = clientChannelHelper.getCommand(outObject);
                if (cmd instanceof CommandPing) {
                    if (channelsStoppedAnswerHealthCheck.contains(channel)) {
                        continue;
                    }
                    channel.writeInbound(Commands.newPong());
                    continue;
                }
                return cmd;
            } else {
                Thread.sleep(sleepTimeMs);
            }
        }

        throw new IOException("Failed to get response from socket within 10s");
    }

    protected Object tryPeekResponse(EmbeddedChannel channel, ClientChannelHelper clientChannelHelper) {
        while (true) {
            if (channel.outboundMessages().isEmpty()) {
                return null;
            } else {
                Object outObject = channel.outboundMessages().peek();
                Object cmd = clientChannelHelper.getCommand(outObject);
                if (cmd instanceof CommandPing) {
                    if (channelsStoppedAnswerHealthCheck.contains(channel)) {
                        continue;
                    }
                    channel.writeInbound(Commands.newPong());
                    channel.outboundMessages().remove();
                    continue;
                }
                return cmd;
            }
        }
    }

    private void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                new Thread(() -> {
                    ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                            .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                }).start();

                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(
                        new PositionImpl(-1, -1),
                        null,
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> true).when(cursorMock).isDurable();

        doAnswer((Answer<Object>) invocationOnMock -> {
            Thread.sleep(300);
            ((OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
            return null;
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            Thread.sleep(300);
            ((OpenCursorCallback) invocationOnMock.getArguments()[4]).openCursorComplete(cursorMock, null);
            return null;
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class), any(Map.class),
                any(OpenCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            Thread.sleep(300);
            ((OpenCursorCallback) invocationOnMock.getArguments()[3])
                    .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
            return null;
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            Thread.sleep(300);
            ((OpenCursorCallback) invocationOnMock.getArguments()[3])
                    .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
            return null;
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(InitialPosition.class), any(Map.class), any(Map.class),
                any(OpenCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
            return null;
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            ((DeleteCursorCallback) invocationOnMock.getArguments()[1])
                    .deleteCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
            return null;
        }).when(ledgerMock).asyncDeleteCursor(matches(".*fail.*"), any(DeleteCursorCallback.class), any());

        doAnswer((Answer<Object>) invocationOnMock -> {
            ((CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
            return null;
        }).when(cursorMock).asyncClose(any(CloseCallback.class), any());

        doReturn(successSubName).when(cursorMock).getName();
    }

    @Test(timeOut = 30000)
    public void testInvalidTopicOnLookup() throws Exception {
        resetChannel();
        setChannelConnected();

        String invalidTopicName = "xx/ass/aa/aaa";

        resetChannel();
        setChannelConnected();


        channel.writeInbound(Commands.newLookup(invalidTopicName, true, 1));
        Object obj = getResponse();
        assertEquals(obj.getClass(), CommandLookupTopicResponse.class);
        CommandLookupTopicResponse res = (CommandLookupTopicResponse) obj;
        assertEquals(res.getError(), ServerError.InvalidTopicName);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testInvalidTopicOnProducer() throws Exception {
        resetChannel();
        setChannelConnected();

        String invalidTopicName = "xx/ass/aa/aaa";

        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(invalidTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);
        Object obj = getResponse();
        assertEquals(obj.getClass(), CommandError.class);
        CommandError res = (CommandError) obj;
        assertEquals(res.getError(), ServerError.InvalidTopicName);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testInvalidTopicOnSubscribe() throws Exception {
        resetChannel();
        setChannelConnected();

        String invalidTopicName = "xx/ass/aa/aaa";

        resetChannel();
        setChannelConnected();

        channel.writeInbound(Commands.newSubscribe(invalidTopicName, "test-subscription", 1, 1, SubType.Exclusive, 0,
                "consumerName", 0 /*avoid reseting cursor*/));
        Object obj = getResponse();
        assertEquals(obj.getClass(), CommandError.class);
        CommandError res = (CommandError) obj;
        assertEquals(res.getError(), ServerError.InvalidTopicName);

        channel.finish();
    }

    @Test
    public void testDelayedClosedProducer() throws Exception {
        resetChannel();
        setChannelConnected();

        CompletableFuture<Topic> delayFuture = new CompletableFuture<>();
        doReturn(delayFuture).when(brokerService).getOrCreateTopic(any(String.class));
        // Create producer first time
        int producerId = 1;
        ByteBuf clientCommand = Commands.newProducer(successTopicName, producerId /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        ByteBuf closeProducerCmd = Commands.newCloseProducer(producerId, 2);
        channel.writeInbound(closeProducerCmd);

        Topic topic = mock(Topic.class);
        doReturn(CompletableFuture.completedFuture(topic)).when(brokerService).getOrCreateTopic(any(String.class));
        doReturn(CompletableFuture.completedFuture(false)).when(topic).hasSchema();

        clientCommand = Commands.newProducer(successTopicName, producerId /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap(), false);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandSuccess);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testTopicIsNotReady() throws Exception {
        resetChannel();
        setChannelConnected();

        // 1st subscribe command to load the topic
        ByteBuf clientCommand1 = Commands.newSubscribe(successTopicName, successSubName, 1 /* consumer id */,
                1 /* request id */, SubType.Shared, 0 /* priority level */, "c1" /* consumer name */,
                0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand1);

        Object response1 = getResponse();
        assertEquals(response1.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response1).getRequestId(), 1);

        // Force the checkTopicNsOwnership method to throw ServiceUnitNotReadyException
        doReturn(FutureUtil.failedFuture(new ServiceUnitNotReadyException("Service unit is not ready")))
                .when(brokerService).checkTopicNsOwnership(anyString());

        // 2nd subscribe command when the service unit is not ready
        ByteBuf clientCommand2 = Commands.newSubscribe(successTopicName, successSubName, 2 /* consumer id */,
                2 /* request id */, SubType.Shared, 0 /* priority level */, "c2" /* consumer name */,
                0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand2);

        Object response2 = getResponse();
        assertEquals(response2.getClass(), CommandError.class);
        assertEquals(((CommandError) response2).getError(), ServerError.ServiceNotReady);
        assertEquals(((CommandError) response2).getRequestId(), 2);

        // Producer command when the service unit is not ready
        ByteBuf clientCommand3 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                "p1" /* producer name */, Collections.emptyMap(), false);
        channel.writeInbound(clientCommand3);

        Object response3 = getResponse();
        assertEquals(response3.getClass(), CommandError.class);
        assertEquals(((CommandError) response3).getError(), ServerError.ServiceNotReady);
        assertEquals(((CommandError) response3).getRequestId(), 3);

        channel.finish();
    }

    @Test
    public void testNeverDelayConsumerFutureWhenNotFail() throws Exception{
        // Mock ServerCnx.field: consumers
        ConcurrentLongHashMap.Builder mapBuilder = Mockito.mock(ConcurrentLongHashMap.Builder.class);
        Mockito.when(mapBuilder.expectedItems(Mockito.anyInt())).thenReturn(mapBuilder);
        Mockito.when(mapBuilder.concurrencyLevel(Mockito.anyInt())).thenReturn(mapBuilder);
        ConcurrentLongHashMap consumers = Mockito.mock(ConcurrentLongHashMap.class);
        Mockito.when(mapBuilder.build()).thenReturn(consumers);
        ArgumentCaptor<Long> ignoreArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final ArgumentCaptor<CompletableFuture> deleteTimesMark = ArgumentCaptor.forClass(CompletableFuture.class);
        Mockito.when(consumers.remove(ignoreArgumentCaptor.capture())).thenReturn(true);
        Mockito.when(consumers.remove(ignoreArgumentCaptor.capture(), deleteTimesMark.capture())).thenReturn(true);
        // case1: exists existingConsumerFuture, already complete or delay done after execute 'isDone()' many times
        // case2: exists existingConsumerFuture, delay complete after execute 'isDone()' many times
        // Why is the design so complicated, see: https://github.com/apache/pulsar/pull/15051
        // Try a delay of 3 stages. The simulation is successful after repeated judgments.
        for(AtomicInteger futureWillDoneAfterDelayTimes = new AtomicInteger(1);
                                            futureWillDoneAfterDelayTimes.intValue() <= 3;
                                            futureWillDoneAfterDelayTimes.incrementAndGet()){
            final AtomicInteger futureCallTimes = new AtomicInteger();
            final Consumer mockConsumer = Mockito.mock(Consumer.class);
            CompletableFuture existingConsumerFuture = new CompletableFuture<Consumer>(){

                private boolean complete;

                // delay complete after execute 'isDone()' many times
                @Override
                public boolean isDone() {
                    if (complete) {
                        return true;
                    }
                    int executeIsDoneCommandTimes = futureCallTimes.incrementAndGet();
                    return executeIsDoneCommandTimes >= futureWillDoneAfterDelayTimes.intValue();
                }

                // if trig "getNow()", then complete
                @Override
                public Consumer get(){
                    complete = true;
                    return mockConsumer;
                }

                // if trig "get()", then complete
                @Override
                public Consumer get(long timeout, TimeUnit unit){
                    complete = true;
                    return mockConsumer;
                }

                // if trig "get()", then complete
                @Override
                public Consumer getNow(Consumer ifAbsent){
                    complete = true;
                    return mockConsumer;
                }

                // never fail
                public boolean isCompletedExceptionally(){
                    return false;
                }
            };
            Mockito.when(consumers.putIfAbsent(Mockito.anyLong(), Mockito.any())).thenReturn(existingConsumerFuture);
            // do test: delay complete after execute 'isDone()' many times
            // Why is the design so complicated, see: https://github.com/apache/pulsar/pull/15051
            try (MockedStatic<ConcurrentLongHashMap> theMock = Mockito.mockStatic(ConcurrentLongHashMap.class)) {
                // Inject consumers to ServerCnx
                theMock.when(ConcurrentLongHashMap::newBuilder).thenReturn(mapBuilder);
                // reset channels( serverChannel, clientChannel )
                resetChannel();
                setChannelConnected();
                // auth check disable
                doReturn(false).when(brokerService).isAuthenticationEnabled();
                doReturn(false).when(brokerService).isAuthorizationEnabled();
                // do subscribe
                ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                        successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                        "test" /* consumer name */, 0 /* avoid reseting cursor */);
                channel.writeInbound(clientCommand);
                Object responseObj = getResponse();
                Predicate<Object> responseAssert = obj -> {
                    if (responseObj instanceof CommandSuccess) {
                        return true;
                    }
                    if (responseObj instanceof CommandError) {
                        CommandError commandError = (CommandError) responseObj;
                        return ServerError.ServiceNotReady == commandError.getError();
                    }
                    return false;
                };
                // assert no consumer-delete event occur
                assertFalse(deleteTimesMark.getAllValues().contains(existingConsumerFuture));
                // assert without another error occur
                assertTrue(responseAssert.test(responseAssert));
                // Server will not close the connection
                assertTrue(channel.isOpen());
                channel.finish();
            }
        }
        // case3: exists existingConsumerFuture, already complete and exception
        CompletableFuture existingConsumerFuture = Mockito.mock(CompletableFuture.class);
        Mockito.when(consumers.putIfAbsent(Mockito.anyLong(), Mockito.any())).thenReturn(existingConsumerFuture);
        // make consumerFuture delay finish
        Mockito.when(existingConsumerFuture.isDone()).thenReturn(true);
        // when sync get return, future will return success value.
        Mockito.when(existingConsumerFuture.get()).thenThrow(new NullPointerException());
        Mockito.when(existingConsumerFuture.get(Mockito.anyLong(), Mockito.any())).
                thenThrow(new NullPointerException());
        Mockito.when(existingConsumerFuture.isCompletedExceptionally()).thenReturn(true);
        Mockito.when(existingConsumerFuture.getNow(Mockito.any())).thenThrow(new NullPointerException());
        try (MockedStatic<ConcurrentLongHashMap> theMock = Mockito.mockStatic(ConcurrentLongHashMap.class)) {
            // Inject consumers to ServerCnx
            theMock.when(ConcurrentLongHashMap::newBuilder).thenReturn(mapBuilder);
            // reset channels( serverChannel, clientChannel )
            resetChannel();
            setChannelConnected();
            // auth check disable
            doReturn(false).when(brokerService).isAuthenticationEnabled();
            doReturn(false).when(brokerService).isAuthorizationEnabled();
            // do subscribe
            ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                    successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                    "test" /* consumer name */, 0 /* avoid reseting cursor */);
            channel.writeInbound(clientCommand);
            Object responseObj = getResponse();
            Predicate<Object> responseAssert = obj -> {
                if (responseObj instanceof CommandError) {
                    CommandError commandError = (CommandError) responseObj;
                    return ServerError.ServiceNotReady != commandError.getError();
                }
                return false;
            };
            // assert error response
            assertTrue(responseAssert.test(responseAssert));
            // assert consumer-delete event occur
            assertEquals(1L,
                    deleteTimesMark.getAllValues().stream().filter(f -> f == existingConsumerFuture).count());
            // Server will not close the connection
            assertTrue(channel.isOpen());
            channel.finish();
        }
    }

    @Test
    public void testHandleAuthResponseWithoutClientVersion() {
        ServerCnx cnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        CommandAuthResponse authResponse = mock(CommandAuthResponse.class);
        org.apache.pulsar.common.api.proto.AuthData authData = mock(org.apache.pulsar.common.api.proto.AuthData.class);
        when(authResponse.getResponse()).thenReturn(authData);
        when(authResponse.hasResponse()).thenReturn(true);
        when(authResponse.getResponse().hasAuthMethodName()).thenReturn(true);
        when(authResponse.getResponse().hasAuthData()).thenReturn(true);
        when(authResponse.hasClientVersion()).thenReturn(false);
        try {
            cnx.handleAuthResponse(authResponse);
        } catch (Exception ignore) {
        }
        verify(authResponse, times(1)).hasClientVersion();
        verify(authResponse, times(0)).getClientVersion();
    }

    @Test(timeOut = 30000)
    public void sendAddPartitionToTxnResponseFailedAuth() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);
        svcConfig.setProxyRoles(Collections.singleton("pass.fail"));

        svcConfig.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        AuthorizationService authorizationService =
                spyWithClassAndConstructorArgs(AuthorizationService.class, svcConfig,
                        pulsar.getPulsarResources());
        when(brokerService.getAuthorizationService()).thenReturn(authorizationService);
        svcConfig.setAuthorizationEnabled(true);

        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.verifyTxnOwnership(any(), any())).thenReturn(CompletableFuture.completedFuture(false));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();

        ByteBuf connect = Commands.newConnect(authMethodName, "pass.fail", "test", "localhost",
                "pass.pass", "pass.pass", authMethodName);
        channel.writeInbound(connect);
        Object connectResponse = getResponse();
        assertTrue(connectResponse instanceof CommandConnected);

        ByteBuf clientCommand = Commands.newAddPartitionToTxn(89L, 1L, 12L,
                Collections.singletonList("tenant/ns/topic1"));
        channel.writeInbound(clientCommand);
        CommandAddPartitionToTxnResponse response = (CommandAddPartitionToTxnResponse) getResponse();

        assertEquals(response.getError(), ServerError.TransactionNotFound);
        verify(txnStore, never()).addProducedPartitionToTxn(any(TxnID.class), any());

        channel.finish();
    }

}
