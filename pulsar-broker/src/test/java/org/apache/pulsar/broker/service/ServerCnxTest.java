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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.util.OrderedExecutor;
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
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.ServerCnx.State;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.service.utils.ClientChannelHelper;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.AuthMethod;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
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
    private final List<String> matchingTopics = Arrays.asList(
            "persistent://use/ns-abc/topic-1",
            "persistent://use/ns-abc/topic-2");

    private final List<String> topics = Arrays.asList(
            "persistent://use/ns-abc/topic-1",
            "persistent://use/ns-abc/topic-2",
            "persistent://use/ns-abc/topic");

    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        svcConfig = spy(ServiceConfiguration.class);
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("pulsar-cluster");
        pulsar = spyWithClassAndConstructorArgs(PulsarService.class, svcConfig);
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();

        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(PulsarResources.class)).when(pulsar).getPulsarResources();

        doReturn("use").when(svcConfig).getClusterName();

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
        doReturn(CompletableFuture.completedFuture(Collections.emptyMap())).when(brokerService).fetchTopicPropertiesAsync(anyObject());

        doReturn(executor).when(pulsar).getOrderedExecutor();

        PulsarResources pulsarResources = spyWithClassAndConstructorArgs(PulsarResources.class, store, store);
        namespaceResources = spyWithClassAndConstructorArgs(NamespaceResources.class, store, 30);
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();
        doReturn(pulsarResources).when(pulsar).getPulsarResources();

        namespaceService = mock(NamespaceService.class);
        doReturn(CompletableFuture.completedFuture(null)).when(namespaceService).getBundleAsync(any());
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any());
        doReturn(true).when(namespaceService).isServiceUnitActive(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespaceService).isServiceUnitActiveAsync(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespaceService).checkTopicOwnership(any());
        doReturn(CompletableFuture.completedFuture(topics)).when(namespaceService).getListOfTopics(
                NamespaceName.get("use", "ns-abc"), CommandGetTopicsOfNamespace.Mode.ALL);
        doReturn(CompletableFuture.completedFuture(topics)).when(namespaceService).getListOfPersistentTopics(
                NamespaceName.get("use", "ns-abc"));

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
        brokerService.close();
        pulsar.close();
        GracefulExecutorServicesShutdown.initiate()
                .timeout(Duration.ZERO)
                .shutdown(executor)
                .handle().get();
        EventLoopUtil.shutdownGracefully(eventLoopGroup).get();
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
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        AuthenticationState authenticationState = mock(AuthenticationState.class);
        AuthenticationDataSource authenticationDataSource = mock(AuthenticationDataSource.class);
        AuthData authData = AuthData.of(null);

        doReturn(authenticationService).when(brokerService).getAuthenticationService();
        doReturn(authenticationProvider).when(authenticationService).getAuthenticationProvider(Mockito.anyString());
        doReturn(authenticationState).when(authenticationProvider)
            .newAuthState(Mockito.any(), Mockito.any(), Mockito.any());
        doReturn(authData).when(authenticationState)
            .authenticate(authData);
        doReturn(true).when(authenticationState)
            .isComplete();

        doReturn("appid1").when(authenticationState)
            .getAuthRole();

        doReturn(true).when(brokerService).isAuthenticationEnabled();

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

        assertEquals(serverCnx.getState(), State.Start);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();
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
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();

        assertTrue(getResponse() instanceof CommandSendReceipt);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommandBeforeCreatingProducer() throws Exception {
        resetChannel();
        setChannelConnected();

        // test SEND before producer is created
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0);
        ByteBuf data = Unpooled.buffer(1024);

        ByteBuf clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1,
                ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();

        // Then expect channel to close
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !channel.isActive());
        channel.finish();
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

        // Create producer second time
        clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 2 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);

        Awaitility.await().untilAsserted(() -> {
            Object response = getResponse();
            assertTrue(response instanceof CommandError, "Response is not CommandError but " + response);
            CommandError error = (CommandError) response;
            assertEquals(error.getError(), ServerError.ConsumerBusy);
        });
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
        policies.topicDispatchRate = new HashMap<>();
        policies.clusterSubscribeRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = new HashMap<>();
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
        policies.topicDispatchRate = new HashMap<>();
        policies.clusterSubscribeRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = new HashMap<>();
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
        policies.topicDispatchRate = new HashMap<>();
        policies.clusterSubscribeRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        policies.clusterDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = new HashMap<>();
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
        policies.topicDispatchRate = new HashMap<>();
        policies.clusterSubscribeRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = new HashMap<>();
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
        policies.topicDispatchRate = new HashMap<>();
        policies.clusterSubscribeRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.getPoliciesDispatchRate`
        policies.clusterDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceSubscriptionDispatchRate`
        policies.subscriptionDispatchRate = new HashMap<>();
        // add `clusterDispatchRate` otherwise there will be a NPE
        // `org.apache.pulsar.broker.service.AbstractTopic.updateNamespaceReplicatorDispatchRate`
        policies.replicatorDispatchRate = new HashMap<>();
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
        // Wait at most for 10s to get a response
        final long sleepTimeMs = 10;
        final long iterations = TimeUnit.SECONDS.toMillis(10) / sleepTimeMs;
        for (int i = 0; i < iterations; i++) {
            if (!channel.outboundMessages().isEmpty()) {
                Object outObject = channel.outboundMessages().remove();
                return clientChannelHelper.getCommand(outObject);
            } else {
                Thread.sleep(sleepTimeMs);
            }
        }

        throw new IOException("Failed to get response from socket within 10s");
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
        doAnswer((Answer<Object>) invocationOnMock -> {
            Thread.sleep(300);
            new Thread(() -> ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                    .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null)).start();

            return null;
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
    public void testGetTopicsOfNamespace() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newGetTopicsOfNamespaceRequest(
                "use/ns-abc", 1, CommandGetTopicsOfNamespace.Mode.ALL, null, null);
        channel.writeInbound(clientCommand);
        CommandGetTopicsOfNamespaceResponse response = (CommandGetTopicsOfNamespaceResponse) getResponse();

        assertEquals(response.getTopicsList(), topics);
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(topics));
        assertTrue(response.isChanged());
        assertFalse(response.isFiltered());

        channel.finish();
    }

    @Test
    public void testGetTopicsOfNamespaceDisabledFiltering() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(false);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newGetTopicsOfNamespaceRequest(
                "use/ns-abc", 1, CommandGetTopicsOfNamespace.Mode.ALL,
                "use/ns-abc/topic-.*", null);
        channel.writeInbound(clientCommand);
        CommandGetTopicsOfNamespaceResponse response = (CommandGetTopicsOfNamespaceResponse) getResponse();

        assertEquals(response.getTopicsList(), topics);
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(topics));
        assertTrue(response.isChanged());
        assertFalse(response.isFiltered());

        channel.finish();
    }

    @Test
    public void testGetTopicsOfNamespaceLongPattern() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        svcConfig.setSubscriptionPatternMaxLength(10);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newGetTopicsOfNamespaceRequest(
                "use/ns-abc", 1, CommandGetTopicsOfNamespace.Mode.ALL,
                "use/ns-abc/(t|o|to|p|i|c)+-?)+!", null);
        channel.writeInbound(clientCommand);
        CommandGetTopicsOfNamespaceResponse response = (CommandGetTopicsOfNamespaceResponse) getResponse();

        assertEquals(response.getTopicsList(), topics);
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(topics));
        assertTrue(response.isChanged());
        assertFalse(response.isFiltered());

        channel.finish();
    }

    @Test
    public void testGetTopicsOfNamespaceFiltering() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newGetTopicsOfNamespaceRequest(
                "use/ns-abc", 1, CommandGetTopicsOfNamespace.Mode.ALL,
                "use/ns-abc/topic-.*", "SOME_HASH");
        channel.writeInbound(clientCommand);
        CommandGetTopicsOfNamespaceResponse response = (CommandGetTopicsOfNamespaceResponse) getResponse();

        assertEquals(response.getTopicsList(), matchingTopics);
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(matchingTopics));
        assertTrue(response.isChanged());
        assertTrue(response.isFiltered());

        channel.finish();
    }

    @Test
    public void testGetTopicsOfNamespaceNoChange() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newGetTopicsOfNamespaceRequest(
                "use/ns-abc", 1, CommandGetTopicsOfNamespace.Mode.ALL,
                "use/ns-abc/topic-.*", TopicList.calculateHash(matchingTopics));
        channel.writeInbound(clientCommand);
        CommandGetTopicsOfNamespaceResponse response = (CommandGetTopicsOfNamespaceResponse) getResponse();

        assertEquals(response.getTopicsList(), Collections.emptyList());
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(matchingTopics));
        assertFalse(response.isChanged());
        assertTrue(response.isFiltered());

        channel.finish();
    }

    @Test
    public void testWatchTopicList() throws Exception {
        svcConfig.setEnableBrokerSideSubscriptionPatternEvaluation(true);
        resetChannel();
        setChannelConnected();
        BaseCommand command = Commands.newWatchTopicList(1, 3, "use/ns-abc", "use/ns-abc/topic-.*", null);
        ByteBuf serializedCommand = Commands.serializeWithSize(command);

        channel.writeInbound(serializedCommand);
        Object resp = getResponse();
        System.out.println(resp);
        CommandWatchTopicListSuccess response = (CommandWatchTopicListSuccess) resp;

        assertEquals(response.getTopicsList(), matchingTopics);
        assertEquals(response.getTopicsHash(), TopicList.calculateHash(matchingTopics));
        assertEquals(response.getWatcherId(), 3);

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
            Mockito.when(consumers.putIfAbsent(anyLong(), Mockito.any())).thenReturn(existingConsumerFuture);
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
        Mockito.when(consumers.putIfAbsent(anyLong(), Mockito.any())).thenReturn(existingConsumerFuture);
        // make consumerFuture delay finish
        Mockito.when(existingConsumerFuture.isDone()).thenReturn(true);
        // when sync get return, future will return success value.
        Mockito.when(existingConsumerFuture.get()).thenThrow(new NullPointerException());
        Mockito.when(existingConsumerFuture.get(anyLong(), Mockito.any())).
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

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleLookup() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleLookup(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandlePartitionMetadataRequest() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handlePartitionMetadataRequest(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleConsumerStats() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleConsumerStats(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleGetTopicsOfNamespace() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleGetTopicsOfNamespace(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleGetSchema() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleGetSchema(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleGetOrCreateSchema() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleGetOrCreateSchema(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleTcClientConnectRequest() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleTcClientConnectRequest(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleNewTxn() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleNewTxn(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleAddPartitionToTxn() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleAddPartitionToTxn(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleEndTxn() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleEndTxn(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleEndTxnOnPartition() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleEndTxnOnPartition(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleEndTxnOnSubscription() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleEndTxnOnSubscription(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleAddSubscriptionToTxn() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleAddSubscriptionToTxn(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleCommandWatchTopicList() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleCommandWatchTopicList(any());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldFailHandleCommandWatchTopicListClose() throws Exception {
        ServerCnx serverCnx = mock(ServerCnx.class, CALLS_REAL_METHODS);
        Field stateUpdater = ServerCnx.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(serverCnx, ServerCnx.State.Failed);
        serverCnx.handleCommandWatchTopicListClose(any());
    }

    @Test(timeOut = 30000)
    public void handleConnectWithServiceNotReady() throws Exception {
        resetChannel();
        doReturn(false).when(pulsar).isRunning();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect("none", "", null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Start);
        Object response = getResponse();
        assertTrue(response instanceof CommandError);
        CommandError error = (CommandError) response;
        assertEquals(error.getError(), ServerError.ServiceNotReady);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void handlePartitionMetadataRequestWithServiceNotReady() throws Exception {
        resetChannel();
        setChannelConnected();
        doReturn(false).when(pulsar).isRunning();
        assertTrue(channel.isActive());

        ByteBuf clientCommand = Commands.newPartitionMetadataRequest(successTopicName, 1);
        channel.writeInbound(clientCommand);
        Object response = getResponse();
        assertTrue(response instanceof CommandPartitionedTopicMetadataResponse);
        assertEquals(((CommandPartitionedTopicMetadataResponse) response).getError(), ServerError.ServiceNotReady);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendAddPartitionToTxnResponse() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.addProducedPartitionToTxn(any(TxnID.class), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newAddPartitionToTxn(89L, 1L, 12L,
                List.of("tenant/ns/topic1"));
        channel.writeInbound(clientCommand);
        CommandAddPartitionToTxnResponse response = (CommandAddPartitionToTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertFalse(response.hasError());
        assertFalse(response.hasMessage());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendAddPartitionToTxnResponseFailed() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.addProducedPartitionToTxn(any(TxnID.class), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("server error")));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newAddPartitionToTxn(89L, 1L, 12L,
                List.of("tenant/ns/topic1"));
        channel.writeInbound(clientCommand);
        CommandAddPartitionToTxnResponse response = (CommandAddPartitionToTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertEquals(response.getError().getValue(), 0);
        assertEquals(response.getMessage(), "server error");

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendAddSubscriptionToTxnResponse() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.addAckedPartitionToTxn(any(TxnID.class), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        final Subscription sub = new Subscription();
        sub.setTopic("topic1");
        sub.setSubscription("sub1");
        ByteBuf clientCommand = Commands.newAddSubscriptionToTxn(89L, 1L, 12L,
                List.of(sub));
        channel.writeInbound(clientCommand);
        CommandAddSubscriptionToTxnResponse response = (CommandAddSubscriptionToTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertFalse(response.hasError());
        assertFalse(response.hasMessage());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendAddSubscriptionToTxnResponseFailed() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.addAckedPartitionToTxn(any(TxnID.class), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("server error")));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        final Subscription sub = new Subscription();
        sub.setTopic("topic1");
        sub.setSubscription("sub1");
        ByteBuf clientCommand = Commands.newAddSubscriptionToTxn(89L, 1L, 12L,
                List.of(sub));
        channel.writeInbound(clientCommand);
        CommandAddSubscriptionToTxnResponse response = (CommandAddSubscriptionToTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertEquals(response.getError().getValue(), 0);
        assertEquals(response.getMessage(), "server error");

        channel.finish();
    }


    @Test(timeOut = 30000)
    public void sendEndTxnResponse() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.serializeWithSize(Commands.newEndTxn(89L, 1L, 12L,
                TxnAction.COMMIT));
        channel.writeInbound(clientCommand);
        CommandEndTxnResponse response = (CommandEndTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertFalse(response.hasError());
        assertFalse(response.hasMessage());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendEndTxnResponseFailed() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("server error")));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);
        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.serializeWithSize(Commands.newEndTxn(89L, 1L, 12L,
                TxnAction.COMMIT));
        channel.writeInbound(clientCommand);
        CommandEndTxnResponse response = (CommandEndTxnResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertEquals(response.getError().getValue(), 0);
        assertEquals(response.getMessage(), "server error");

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendEndTxnOnPartitionResponse() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);

        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        Topic topic = mock(Topic.class);
        doReturn(CompletableFuture.completedFuture(null)).when(topic).endTxn(any(TxnID.class), anyInt(), anyLong());
        doReturn(CompletableFuture.completedFuture(Optional.of(topic))).when(brokerService).getTopicIfExists(any(String.class));
        ByteBuf clientCommand = Commands.newEndTxnOnPartition(89L, 1L, 12L,
                successTopicName, TxnAction.COMMIT, 1L);
        channel.writeInbound(clientCommand);
        CommandEndTxnOnPartitionResponse response = (CommandEndTxnOnPartitionResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertFalse(response.hasError());
        assertFalse(response.hasMessage());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendEndTxnOnPartitionResponseFailed() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);

        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        Topic topic = mock(Topic.class);
        doReturn(CompletableFuture.failedFuture(new RuntimeException("server error"))).when(topic).endTxn(any(TxnID.class), anyInt(), anyLong());
        doReturn(CompletableFuture.completedFuture(Optional.of(topic))).when(brokerService).getTopicIfExists(any(String.class));
        ByteBuf clientCommand = Commands.newEndTxnOnPartition(89L, 1L, 12L,
                successTopicName, TxnAction.COMMIT, 1L);
        channel.writeInbound(clientCommand);
        CommandEndTxnOnPartitionResponse response = (CommandEndTxnOnPartitionResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertEquals(response.getError().getValue(), 0);
        assertEquals(response.getMessage(), "server error");

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void sendEndTxnOnSubscription() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);

        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        Topic topic = mock(Topic.class);
        final org.apache.pulsar.broker.service.Subscription sub = mock(org.apache.pulsar.broker.service.Subscription.class);
        doReturn(sub).when(topic).getSubscription(any());
        doReturn(CompletableFuture.completedFuture(null))
                .when(sub).endTxn(anyLong(), anyLong(), anyInt(), anyLong());
        doReturn(CompletableFuture.completedFuture(Optional.of(topic))).when(brokerService).getTopicIfExists(any(String.class));

        ByteBuf clientCommand = Commands.newEndTxnOnSubscription(89L, 1L, 12L,
                successTopicName, successSubName, TxnAction.COMMIT, 1L);
        channel.writeInbound(clientCommand);
        CommandEndTxnOnSubscriptionResponse response = (CommandEndTxnOnSubscriptionResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertFalse(response.hasError());
        assertFalse(response.hasMessage());

        channel.finish();
    }


    @Test(timeOut = 30000)
    public void sendEndTxnOnSubscriptionFailed() throws Exception {
        final TransactionMetadataStoreService txnStore = mock(TransactionMetadataStoreService.class);
        when(txnStore.endTransaction(any(TxnID.class), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsar.getTransactionMetadataStoreService()).thenReturn(txnStore);

        svcConfig.setTransactionCoordinatorEnabled(true);
        resetChannel();
        setChannelConnected();
        Topic topic = mock(Topic.class);

        final org.apache.pulsar.broker.service.Subscription sub = mock(org.apache.pulsar.broker.service.Subscription.class);
        doReturn(sub).when(topic).getSubscription(any());
        doReturn(CompletableFuture.failedFuture(new RuntimeException("server error")))
                .when(sub).endTxn(anyLong(), anyLong(), anyInt(), anyLong());
        doReturn(CompletableFuture.completedFuture(Optional.of(topic))).when(brokerService).getTopicIfExists(any(String.class));

        ByteBuf clientCommand = Commands.newEndTxnOnSubscription(89L, 1L, 12L,
                successTopicName, successSubName, TxnAction.COMMIT, 1L);
        channel.writeInbound(clientCommand);
        CommandEndTxnOnSubscriptionResponse response = (CommandEndTxnOnSubscriptionResponse) getResponse();

        assertEquals(response.getRequestId(), 89L);
        assertEquals(response.getTxnidLeastBits(), 1L);
        assertEquals(response.getTxnidMostBits(), 12L);
        assertEquals(response.getError().getValue(), 0);
        assertEquals(response.getMessage(), "Handle end txn on subscription failed: server error");

        channel.finish();
    }

}
