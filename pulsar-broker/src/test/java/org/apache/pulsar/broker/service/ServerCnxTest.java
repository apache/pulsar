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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.AuthenticationException;

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
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationManager;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.ServerCnx.State;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.utils.ClientChannelHelper;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.api.Commands.ChecksumType;
import org.apache.pulsar.common.api.proto.PulsarApi.AuthMethod;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnected;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 */
@Test
public class ServerCnxTest {
    private EmbeddedChannel channel;
    private ServiceConfiguration svcConfig;
    private ServerCnx serverCnx;
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ClientChannelHelper clientChannelHelper;
    private PulsarService pulsar;
    private ConfigurationCacheService configCacheService;
    private NamespaceService namespaceService;
    private final int currentProtocolVersion = ProtocolVersion.values()[ProtocolVersion.values().length - 1]
            .getNumber();

    private final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    private final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    private final String nonOwnedTopicName = "persistent://prop/use/ns-abc/success-not-owned-topic";
    private final String successSubName = "successSub";
    private final String nonExistentTopicName = "persistent://nonexistent-prop/nonexistent-cluster/nonexistent-namespace/successNonExistentTopic";
    private final String topicWithNonLocalCluster = "persistent://prop/usw/ns-abc/successTopic";

    private ManagedLedger ledgerMock = mock(ManagedLedger.class);
    private ManagedCursor cursorMock = mock(ManagedCursor.class);

    @BeforeMethod
    public void setup() throws Exception {
        svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        doReturn(svcConfig).when(pulsar).getConfiguration();

        doReturn("use").when(svcConfig).getClusterName();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(Optional.empty()).when(zkDataCache).get(anyObject());
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        namespaceService = mock(NamespaceService.class);
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(namespaceService).isServiceUnitActive(any(DestinationName.class));

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

    @Test(timeOut = 30000)
    public void testConnectCommandWithEnum() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        assertEquals(serverCnx.getState(), State.Start);

        // test server response to CONNECT
        ByteBuf clientCommand = Commands.newConnect(AuthMethod.AuthMethodNone, "");
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
        ByteBuf clientCommand = Commands.newConnect("none", "", ProtocolVersion.v0.getNumber(), null, null);
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
        doReturn(authenticationService).when(brokerService).getAuthenticationService();
        doReturn("appid1").when(authenticationService).authenticate(new AuthenticationDataCommand(Mockito.anyString()),
                Mockito.anyString());
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
        AuthenticationException e = new AuthenticationException();
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        doReturn(authenticationService).when(brokerService).getAuthenticationService();
        doThrow(e).when(authenticationService).authenticate(new AuthenticationDataCommand(Mockito.anyString()),
                Mockito.anyString());
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
                "prod-name");
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName);

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // test PRODUCER error case
        clientCommand = Commands.newProducer(failTopicName, 2, 2, "prod-name-2");
        channel.writeInbound(clientCommand);

        assertTrue(getResponse() instanceof CommandError);
        assertNull(brokerService.getTopicReference(failTopicName));

        channel.finish();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test(timeOut = 5000)
    public void testDuplicateConcurrentProducerCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        CompletableFuture<Topic> delayFuture = new CompletableFuture<>();
        doReturn(delayFuture).when(brokerService).getTopic(any(String.class));
        // Create producer first time
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);

        // Create producer second time
        clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */, "prod-name");
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
        doReturn(false).when(namespaceService).isServiceUnitActive(any(DestinationName.class));

        // test PRODUCER failure case
        ByteBuf clientCommand = Commands.newProducer(nonOwnedTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertEquals(response.getClass(), CommandError.class);

        CommandError errorResponse = (CommandError) response;
        assertEquals(errorResponse.getError(), ServerError.ServiceNotReady);

        assertNull(brokerService.getTopicReference(nonOwnedTopicName));

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testProducerCommandWithAuthorizationPositive() throws Exception {
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationManager).canProduceAsync(Mockito.any(), Mockito.any());
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        resetChannel();
        setChannelConnected();

        // test PRODUCER success case
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);
        assertEquals(getResponse().getClass(), CommandProducerSuccess.class);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName);

        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        channel.finish();
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @SuppressWarnings("unchecked")
    @Test(timeOut = 30000)
    public void testNonExistentTopic() throws Exception {
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        ConfigurationCacheService configCacheService = mock(ConfigurationCacheService.class);
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(matches(".*nonexistent.*"));

        AuthorizationManager authorizationManager = spy(new AuthorizationManager(svcConfig, configCacheService));
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(false).when(authorizationManager).isSuperUser(Mockito.anyString());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(newProducerCmd);
        assertTrue(getResponse() instanceof CommandError);
        channel.finish();

        // Test consumer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newSubscribeCmd = Commands.newSubscribe(nonExistentTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(newSubscribeCmd);
        assertTrue(getResponse() instanceof CommandError);
    }

    @Test(timeOut = 30000)
    public void testClusterAccess() throws Exception {
        AuthorizationManager authorizationManager = spy(new AuthorizationManager(svcConfig, configCacheService));
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(false).when(authorizationManager).isSuperUser(Mockito.anyString());
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationManager).checkPermission(any(DestinationName.class), Mockito.anyString(),
                any(AuthAction.class));

        resetChannel();
        setChannelConnected();
        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        resetChannel();
        setChannelConnected();
        clientCommand = Commands.newProducer(topicWithNonLocalCluster, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);
    }

    @Test(timeOut = 30000)
    public void testNonExistentTopicSuperUserAccess() throws Exception {
        AuthorizationManager authorizationManager = spy(new AuthorizationManager(svcConfig, configCacheService));
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(true).when(authorizationManager).isSuperUser(Mockito.anyString());

        // Test producer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newProducerCmd = Commands.newProducer(nonExistentTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(newProducerCmd);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName);
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        channel.finish();

        // Test consumer creation
        resetChannel();
        setChannelConnected();
        ByteBuf newSubscribeCmd = Commands.newSubscribe(nonExistentTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(newSubscribeCmd);
        topicRef = (PersistentTopic) brokerService.getTopicReference(nonExistentTopicName);
        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getPersistentSubscription(successSubName).getDispatcher().isConsumerConnected());
        assertTrue(getResponse() instanceof CommandSuccess);
    }

    public void testProducerCommandWithAuthorizationNegative() throws Exception {
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationManager).canProduceAsync(Mockito.any(), Mockito.any());
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn("prod1").when(brokerService).generateUniqueProducerName();
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */, null);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSendCommand() throws Exception {
        resetChannel();
        setChannelConnected();

        ByteBuf clientCommand = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                "prod-name");
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        // test SEND success
        MessageMetadata messageMetadata = MessageMetadata.newBuilder().setPublishTime(System.currentTimeMillis())
                .setProducerName("prod-name").setSequenceId(0).build();
        ByteBuf data = Unpooled.buffer(1024);

        clientCommand = Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data);
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
                producerName);
        channel.writeInbound(clientCommand1);
        assertTrue(getResponse() instanceof CommandProducerSuccess);

        ByteBuf clientCommand2 = Commands.newProducer(successTopicName, 2 /* producer id */, 2 /* request id */,
                producerName);
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
                producerName);
        channel.writeInbound(createProducer1);

        // Producer create succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandProducerSuccess.class);
        assertEquals(((CommandProducerSuccess) response).getRequestId(), 1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 2 /* request id */,
                producerName);
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
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe1);

        // 1st subscribe succeeds
        response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 1);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 2 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
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
        doReturn(delayFuture).when(brokerService).getTopic(any(String.class));
        // Create subscriber first time
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);

        // Create producer second time
        clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);

        Object response = getResponse();
        assertTrue(response instanceof CommandError);
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
                any(OpenLedgerCallback.class), anyObject());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName);
        channel.writeInbound(createProducer2);

        // Complete the topic opening
        openTopicFuture.get().run();

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // 2nd producer fails to create
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 3);

        // We should not receive response for 1st producer, since it was cancelled by the close
        assertTrue(channel.outboundMessages().isEmpty());
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
                any(OpenLedgerCallback.class), anyObject());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create producer
        // 2. close producer (when the timeout is triggered, which may be before the producer was created on the broker
        // 3. create producer (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(successTopicName, 1 /* producer id */, 1 /* request id */,
                producerName);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer1 = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer1);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName);
        channel.writeInbound(createProducer2);

        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 1 /* producer id */, 4 /* request id */,
                producerName);
        channel.writeInbound(createProducer3);

        ByteBuf createProducer4 = Commands.newProducer(successTopicName, 1 /* producer id */, 5 /* request id */,
                producerName);
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
                any(OpenLedgerCallback.class), anyObject());

        // In a create producer timeout from client side we expect to see this sequence of commands :
        // 1. create a failure producer which will timeout creation after 100msec
        // 2. close producer
        // 3. Recreate producer (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and create producer again.

        // These operations need to be serialized, to allow the last create producer to finally succeed
        // (There can be more create/close pairs in the sequence, depending on the client timeout

        String producerName = "my-producer";

        ByteBuf createProducer1 = Commands.newProducer(failTopicName, 1 /* producer id */, 1 /* request id */,
                producerName);
        channel.writeInbound(createProducer1);

        ByteBuf closeProducer = Commands.newCloseProducer(1 /* producer id */, 2 /* request id */ );
        channel.writeInbound(closeProducer);

        ByteBuf createProducer2 = Commands.newProducer(successTopicName, 1 /* producer id */, 3 /* request id */,
                producerName);
        channel.writeInbound(createProducer2);

        // Now the topic gets opened
        openFailedTopic.get().run();

        // Close succeeds
        Object response = getResponse();
        assertEquals(response.getClass(), CommandSuccess.class);
        assertEquals(((CommandSuccess) response).getRequestId(), 2);

        // 2nd producer fails
        response = getResponse();
        assertEquals(response.getClass(), CommandError.class);
        assertEquals(((CommandError) response).getRequestId(), 3);

        // Wait till the failtopic timeout interval
        Thread.sleep(500);
        ByteBuf createProducer3 = Commands.newProducer(successTopicName, 1 /* producer id */, 4 /* request id */,
                producerName);
        channel.writeInbound(createProducer3);

        // 3rd producer succeeds
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
                any(OpenLedgerCallback.class), anyObject());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe
        // 2. close consumer (when the timeout is triggered, which may be before the consumer was created on the broker)
        // 3. Subscribe (triggered by reconnection logic)

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout

        ByteBuf subscribe1 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe1);

        ByteBuf closeConsumer = Commands.newCloseConsumer(1 /* consumer id */, 2 /* request id */ );
        channel.writeInbound(closeConsumer);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 3 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe2);

        ByteBuf subscribe3 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 4 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe3);

        ByteBuf subscribe4 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 5 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe4);

        openTopicTask.get().run();

        Object response;

        synchronized (this) {
            // Close succeeds
            response = getResponse();
            assertEquals(response.getClass(), CommandSuccess.class);
            assertEquals(((CommandSuccess) response).getRequestId(), 2);

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

            // We should not receive response for 1st producer, since it was cancelled by the close
            assertTrue(channel.outboundMessages().isEmpty());
            assertTrue(channel.isActive());
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
                any(OpenLedgerCallback.class), anyObject());

        CompletableFuture<Runnable> openTopicFail = new CompletableFuture<>();
        doAnswer(invocationOnMock -> {
            openTopicFail.complete(() -> {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
            });
            return null;
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), anyObject());

        // In a subscribe timeout from client side we expect to see this sequence of commands :
        // 1. Subscribe against failtopic which will fail after 100msec
        // 2. close consumer
        // 3. Resubscribe (triggered by reconnection logic)
        // 4. Wait till the timeout of 1, and subscribe again.

        // These operations need to be serialized, to allow the last subscribe operation to finally succeed
        // (There can be more subscribe/close pairs in the sequence, depending on the client timeout
        ByteBuf subscribe1 = Commands.newSubscribe(failTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(subscribe1);

        ByteBuf closeConsumer = Commands.newCloseConsumer(1 /* consumer id */, 2 /* request id */ );
        channel.writeInbound(closeConsumer);

        ByteBuf subscribe2 = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 3 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
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
                successSubName, 1 /* consumer id */, 4 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
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
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName);

        assertNotNull(topicRef);
        assertTrue(topicRef.getSubscriptions().containsKey(successSubName));
        assertTrue(topicRef.getPersistentSubscription(successSubName).getDispatcher().isConsumerConnected());

        // test SUBSCRIBE on topic creation success and cursor failure
        clientCommand = Commands.newSubscribe(successTopicName, failSubName, 2, 2, SubType.Exclusive,
                0, "test" /*
                        * consumer name
                        */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandError);

        // test SUBSCRIBE on topic creation failure
        clientCommand = Commands.newSubscribe(failTopicName, successSubName, 3, 3, SubType.Exclusive,
                0, "test" /*
                        * consumer name
                        */);
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
                "test" /* consumer name */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PersistentTopic topicRef = (PersistentTopic) brokerService.getTopicReference(successTopicName);
        topicRef.markBatchMessagePublished();

        // test SUBSCRIBE on topic and cursor creation success
        clientCommand = Commands.newSubscribe(successTopicName, failSubName, 2, 2, SubType.Exclusive, 0 /* priority */,
                "test" /* consumer name */);
        channel.writeInbound(clientCommand);
        Object response = getResponse();
        assertTrue(response instanceof CommandError);
        assertTrue(((CommandError) response).getError().equals(ServerError.UnsupportedVersionError));

        // Server will not close the connection
        assertTrue(channel.isOpen());

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeCommandWithAuthorizationPositive() throws Exception {
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationManager).canConsumeAsync(Mockito.any(), Mockito.any());
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        resetChannel();
        setChannelConnected();

        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);

        assertTrue(getResponse() instanceof CommandSuccess);

        channel.finish();
    }

    @Test(timeOut = 30000)
    public void testSubscribeCommandWithAuthorizationNegative() throws Exception {
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationManager).canConsumeAsync(Mockito.any(), Mockito.any());
        doReturn(authorizationManager).when(brokerService).getAuthorizationManager();
        doReturn(true).when(brokerService).isAuthenticationEnabled();
        doReturn(true).when(brokerService).isAuthorizationEnabled();

        resetChannel();
        setChannelConnected();

        // test SUBSCRIBE on topic and cursor creation success
        ByteBuf clientCommand = Commands.newSubscribe(successTopicName, //
                successSubName, 1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
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
                   */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        PositionImpl pos = new PositionImpl(0, 0);

        clientCommand = Commands.newAck(1 /* consumer id */, pos.getLedgerId(), pos.getEntryId(), AckType.Individual,
                null);
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
                1 /* consumer id */, 1 /* request id */, SubType.Exclusive, 0, "test" /* consumer name */);
        channel.writeInbound(clientCommand);
        assertTrue(getResponse() instanceof CommandSuccess);

        clientCommand = Commands.newFlow(1 /* consumer id */, 1 /* message permits */);
        channel.writeInbound(clientCommand);

        // cursor is mocked
        // verify nothing is sent out on the wire after ack
        assertNull(channel.outboundMessages().peek());
        channel.finish();
    }

    private void resetChannel() throws Exception {
        int MaxMessageSize = 5 * 1024 * 1024;
        if (channel != null && channel.isActive()) {
            serverCnx.close();
            channel.close().get();
        }
        serverCnx = new ServerCnx(brokerService);
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4), serverCnx);
    }

    private void setChannelConnected() throws Exception {
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

    private Object getResponse() throws Exception {
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
                any(OpenLedgerCallback.class), anyObject());

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
                any(OpenLedgerCallback.class), anyObject());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(-1, -1),
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[1]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(OpenCursorCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((OpenCursorCallback) invocationOnMock.getArguments()[1])
                        .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(OpenCursorCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1])
                        .deleteCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*fail.*"), any(DeleteCursorCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
                return null;
            }
        }).when(cursorMock).asyncClose(any(CloseCallback.class), anyObject());

        doReturn(successSubName).when(cursorMock).getName();
    }

    private static final Logger log = LoggerFactory.getLogger(ServerCnxTest.class);
}
