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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.ServerCnx.State;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.service.utils.ClientChannelHelper;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.AuthMethod;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnected;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.EncryptionKeys;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 */
@Test
@SuppressWarnings("unchecked")
public class ServerCnxTest {
    protected EmbeddedChannel channel;
    private ServiceConfiguration svcConfig;
    private ServerCnx serverCnx;
    protected BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ClientChannelHelper clientChannelHelper;
    private PulsarService pulsar;
    private ConfigurationCacheService configCacheService;
    protected NamespaceService namespaceService;
    private final int currentProtocolVersion = ProtocolVersion.values()[ProtocolVersion.values().length - 1]
            .getNumber();

    protected final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    private final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    private final String nonOwnedTopicName = "persistent://prop/use/ns-abc/success-not-owned-topic";
    private final String encryptionRequiredTopicName = "persistent://prop/use/ns-abc/successEncryptionRequiredTopic";
    private final String successSubName = "successSub";
    private final String nonExistentTopicName = "persistent://nonexistent-prop/nonexistent-cluster/nonexistent-namespace/successNonExistentTopic";
    private final String topicWithNonLocalCluster = "persistent://prop/usw/ns-abc/successTopic";

    private ManagedLedger ledgerMock = mock(ManagedLedger.class);
    private ManagedCursor cursorMock = mock(ManagedCursor.class);

    private OrderedExecutor executor;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        pulsar.setShutdownService(new NoOpShutdownService());
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();

        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        doReturn(svcConfig).when(pulsar).getConfiguration();

        doReturn("use").when(svcConfig).getClusterName();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();
        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, ForkJoinPool.commonPool()))
            .when(pulsar).getBookKeeperClient();

        configCacheService = mock(ConfigurationCacheService.class);
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(Optional.empty()).when(zkDataCache).get(any());
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();

        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(executor).when(pulsar).getOrderedExecutor();

        namespaceService = mock(NamespaceService.class);
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any());
        doReturn(true).when(namespaceService).isServiceUnitActive(any());

        setupMLAsyncCallbackMocks();

        clientChannelHelper = new ClientChannelHelper();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @AfterMethod
    public void teardown() throws Exception {
        serverCnx.close();
        channel.close();
        pulsar.close();
        brokerService.close();
        executor.shutdownNow();
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
        PulsarApi.CommandConnect.Builder connectBuilder = PulsarApi.CommandConnect.newBuilder();
        connectBuilder.setClientVersion("Pulsar Client");
        connectBuilder.setAuthMethod(authMethod);
        connectBuilder.setAuthData(ByteString.copyFromUtf8(authData));
        connectBuilder.setProtocolVersion(protocolVersion);
        PulsarApi.CommandConnect connect = connectBuilder.build();
        ByteBuf res = Commands.serializeWithSize(PulsarApi.BaseCommand.newBuilder().setType(PulsarApi.BaseCommand.Type.CONNECT).setConnect(connect));
        connect.recycle();
        connectBuilder.recycle();
        return res;
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
        ByteBuf clientCommand = Commands.newConnect("none", "", ProtocolVersion.v0.getNumber(), null, null, null, null, null);
        channel.writeInbound(clientCommand);

        assertEquals(serverCnx.getState(), State.Connected);
        CommandConnected response = (CommandConnected) getResponse();
        // Server is responding with same version as client
        assertEquals(response.getProtocolVersion(), ProtocolVersion.v0.getNumber());

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
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // test PRODUCER error case
        clientCommand = Commands.newProducer(failTopicName, 2, 2,
                "prod-name-2", Collections.emptyMap());
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
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);

        // Create producer second time
        clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandError);
        CommandError error = (CommandError) response;
        assertEquals(error.getError(), ServerError.ServiceNotReady);
    }

    @Test(timeOut = 30000)
    public void testProducerOnNotOwnedTopic() throws Exception {
        resetChannel();
        setChannelConnected();

        // Force the case where the broker doesn't own any topic
        doReturn(false).when(namespaceService).isServiceUnitActive(any(TopicName.class));

        // test PRODUCER failure case
        ByteBuf clientCommand = Commands.newProducer(nonOwnedTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
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
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService).canProduceAsync(Mockito.any(),
                Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        resetChannel();
        setChannelConnected();

        // test PRODUCER success case
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
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
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        ConfigurationCacheService configCacheService = mock(ConfigurationCacheService.class);
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(matches(".*nonexistent.*"));

        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        svcConfig.setAuthorizationEnabled(true);
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
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
    }

    @Test(timeOut = 30000)
    public void testClusterAccess() throws Exception {
        svcConfig.setAuthorizationEnabled(true);
        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider).checkPermission(any(TopicName.class), Mockito.anyString(),
                any(AuthAction.class));

        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        resetChannel();
        setChannelConnected();
        clientCommand = Commands.newProducer(topicWithNonLocalCluster, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);
    }

    @Test(timeOut = 30000)
    public void testNonExistentTopicSuperUserAccess() throws Exception {
        AuthorizationService authorizationService = spy(new AuthorizationService(svcConfig, configCacheService));
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        Field providerField = AuthorizationService.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        PulsarAuthorizationProvider authorizationProvider = spy(new PulsarAuthorizationProvider(svcConfig, configCacheService));
        providerField.set(authorizationService, authorizationProvider);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationProvider).isSuperUser(Mockito.anyString(), Mockito.any(), Mockito.any());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
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
    }

    public void testProducerCommandWithAuthorizationNegative() throws Exception {
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService).canProduceAsync(Mockito.any(),
                Mockito.any(), Mockito.any());
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn("prod1").when(brokerService).generateUniqueProducerName();
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                null, Collections.emptyMap());
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test SEND success
        MessageMetadata messageMetadata = MessageMetadata.newBuilder().setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name").setSequenceId(0).build();
        ByteBuf data = Unpooled.buffer(1024);

        clientCommand = ByteBufPair.coalesce(Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data));
        channel.writeInbound(Unpooled.copiedBuffer(clientCommand));
        clientCommand.release();

        assertTrue(getResponse() instanceof CommandSendReceipt);
        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testUseSameProducerName() throws Exception {
        resetChannel();
        setChannelConnected();

        String producerName = "my-producer";

        ByteBuf clientCommand1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName, Collections.emptyMap());
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        ByteBuf clientCommand2 = Commands.newProducer(successTopicName, 2 /* producer id */, 2 /* request id */,
                producerName, Collections.emptyMap());
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
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer1);

        // Producer create succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 2 /* request id */,
                producerName, Collections.emptyMap());
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

    @Test(timeOut = 5000)
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
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0,
                "test" /* consumer name */, 0 /* avoid reseting cursor */);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandError, "Response is not CommandError but " + response);
        CommandError error = (CommandError) response;
        assertEquals(error.getError(), ServerError.ServiceNotReady);
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
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap());
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
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer1 = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer2);

        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 1 /* producer id */, 4 /* request id */,
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer3);

        ByteBuf createProducer4 = Commands.newProducer(successTopicName, 1 /* producer id */, 5 /* request id */,
                producerName, Collections.emptyMap());
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

    @Test(timeOut = 30000, invocationCount = 1, skipFailedInvocations = true)
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
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName, Collections.emptyMap());
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
                producerName, Collections.emptyMap());
        channel.writeInbound(createProducer3);

        // 3rd producer fails because 2nd is already connected
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 4);

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
            assertFalse(channel.outboundMessages().isEmpty());
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

        while (serverCnx.hasConsumer(1)) {
            Thread.sleep(10);
        }

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
        setConnectionVersion(ProtocolVersion.v3.getNumber());
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
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService).canConsumeAsync(Mockito.any(),
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
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService).canConsumeAsync(Mockito.any(),
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

        clientCommand = Commands.newAck(1 /* consumer id */, pos.getLedgerId(), pos.getEntryId(), AckType.Individual,
                                        null, Collections.emptyMap());
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
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache).get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache).getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        // test success case: encrypted producer can connect
        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "encrypted-producer", true, null);
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
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache).get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache).getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        // test failure case: unencrypted producer cannot connect
        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 2 /* producer id */, 2 /* request id */,
                "unencrypted-producer", false, null);
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
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache).get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache).getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", true, null);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test success case: encrypted messages can be published
        MessageMetadata messageMetadata = MessageMetadata.newBuilder()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0)
                .addEncryptionKeys(EncryptionKeys.newBuilder().setKey("testKey").setValue(ByteString.copyFrom("testVal".getBytes())))
                .build();
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
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        Policies policies = mock(Policies.class);
        policies.encryption_required = true;
        policies.topicDispatchRate = Maps.newHashMap();
        doReturn(Optional.of(policies)).when(zkDataCache).get(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(zkDataCache).getAsync(AdminResource.path(POLICIES, TopicName.get(encryptionRequiredTopicName).getNamespace()));
        doReturn(zkDataCache).when(configCacheService).policiesCache();

        ByteBuf clientCommand = Commands.newProducer(encryptionRequiredTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name", true, null);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test failure case: unencrypted messages cannot be published
        MessageMetadata messageMetadata = MessageMetadata.newBuilder()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name")
                .setSequenceId(0)
                .build();
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
        serverCnx.authRole = "";
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4), serverCnx);
    }

    protected void setChannelConnected() throws Exception {
        Field channelState = ServerCnx.class.getDeclaredField("state");
        channelState.setAccessible(true);
        channelState.set(serverCnx, State.Connected);
    }

    private void setConnectionVersion(int version) throws Exception {
        PulsarHandler cnx = (PulsarHandler) serverCnx;
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
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(-1, -1),
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class),
                any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[2])
                        .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[3])
                        .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(InitialPosition.class), any(Map.class),
                any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1])
                        .deleteCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*fail.*"), any(DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
                return null;
            }
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
                "prod-name", Collections.emptyMap());
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
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);

        ByteBuf closeProducerCmd = Commands.newCloseProducer(producerId, 2);
        channel.writeInbound(closeProducerCmd);

        Topic topic = mock(Topic.class);
        doReturn(CompletableFuture.completedFuture(topic)).when(brokerService).getOrCreateTopic(any(String.class));
        doReturn(CompletableFuture.completedFuture(false)).when(topic).hasSchema();

        clientCommand = Commands.newProducer(successTopicName, producerId /* producer id */, 1 /* request id */,
                "prod-name", Collections.emptyMap());
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandSuccess);
    }
}
