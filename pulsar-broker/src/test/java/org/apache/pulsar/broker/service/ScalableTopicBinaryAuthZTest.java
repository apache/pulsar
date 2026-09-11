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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockAuthenticationProvider;
import org.apache.pulsar.broker.auth.MockAuthorizationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.broker.service.utils.ClientChannelHelper;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandScalableTopicSubscribeResponse;
import org.apache.pulsar.common.api.proto.CommandScalableTopicUpdate;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Authorization tests for the PIP-460/PIP-466 scalable-topic binary protocol commands.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>{@link Commands#newScalableTopicLookup} requires {@link TopicOperation#LOOKUP} on the
 *       parent topic.</li>
 *   <li>{@link Commands#newScalableTopicSubscribe} requires {@link TopicOperation#CONSUME} with
 *       the subscription name passed via {@link AuthenticationDataSubscription}.</li>
 *   <li>{@link Commands#newScalableTopicClose} performs no per-call authorization (the session is
 *       per-connection and the originating lookup was already authorized).</li>
 *   <li>The {@code scalableTopicsEnabled} feature flag short-circuits authorization so a disabled
 *       feature returns {@link ServerError#NotAllowedError} without consulting the authorization
 *       service.</li>
 * </ul>
 *
 * <p>Tests drive raw protocol bytes through an {@link EmbeddedChannel} so the assertions exercise
 * the wire-level handler paths in {@link ServerCnx} without depending on a client-side
 * implementation of the new commands.
 */
@Test(groups = "broker")
public class ScalableTopicBinaryAuthZTest {

    private static final String AUTHORIZED_ROLE = "pass.pass";
    private static final String UNAUTHORIZED_ROLE = "pass.fail";
    private static final String TOPIC = "persistent://public/default/test-scalable-topic";
    private static final String SUBSCRIPTION = "test-scalable-sub";

    private EmbeddedChannel channel;
    private ServiceConfiguration svcConfig;
    private ServerCnx serverCnx;
    private PulsarTestContext pulsarTestContext;
    private PulsarService pulsar;
    private BrokerService brokerService;
    private ClientChannelHelper clientChannelHelper;
    private AuthorizationService authorizationService;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setKeepAliveIntervalSeconds(60);
        svcConfig.setBacklogQuotaCheckEnabled(false);
        svcConfig.setClusterName("use");

        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .build();
        pulsar = pulsarTestContext.getPulsarService();
        brokerService = pulsarTestContext.getBrokerService();

        NamespaceService namespaceService = pulsar.getNamespaceService();
        when(namespaceService.getBundleAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

        clientChannelHelper = new ClientChannelHelper();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (serverCnx != null) {
            serverCnx.close();
        }
        if (channel != null) {
            channel.close();
        }
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    @Test
    public void testScalableTopicLookupRequiresLookupPermission() throws Exception {
        configureAuthAndConnect(UNAUTHORIZED_ROLE);

        long sessionId = 1L;
        channel.writeInbound(Commands.newScalableTopicLookup(sessionId, TOPIC));

        Object response = readResponse();
        assertTrue(response instanceof CommandScalableTopicUpdate,
                "Expected CommandScalableTopicUpdate, got " + response);
        CommandScalableTopicUpdate update = (CommandScalableTopicUpdate) response;
        assertEquals(update.getSessionId(), sessionId);
        assertTrue(update.hasError());
        assertEquals(update.getError(), ServerError.AuthorizationError);

        TopicName topicName = TopicName.get(TOPIC);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, UNAUTHORIZED_ROLE,
                        serverCnx.getAuthData());
    }

    @Test
    public void testScalableTopicLookupAllowsAuthorizedRole() throws Exception {
        configureAuthAndConnect(AUTHORIZED_ROLE);

        long sessionId = 2L;
        channel.writeInbound(Commands.newScalableTopicLookup(sessionId, TOPIC));

        // The session may eventually emit a TopicNotFound error from the metadata-store watch (the
        // topic does not exist in the test harness), but it must NOT emit AuthorizationError.
        Object response = readResponse();
        assertTrue(response instanceof CommandScalableTopicUpdate,
                "Expected CommandScalableTopicUpdate, got " + response);
        CommandScalableTopicUpdate update = (CommandScalableTopicUpdate) response;
        if (update.hasError()) {
            assertEquals(update.getError(), ServerError.TopicNotFound,
                    "Authorized role must not be rejected with AuthorizationError");
        }

        TopicName topicName = TopicName.get(TOPIC);
        verify(authorizationService, times(1))
                .allowTopicOperationAsync(topicName, TopicOperation.LOOKUP, AUTHORIZED_ROLE,
                        serverCnx.getAuthData());
    }

    @Test
    public void testScalableTopicSubscribeRequiresConsumePermission() throws Exception {
        configureAuthAndConnect(UNAUTHORIZED_ROLE);
        // Pre-stub the scalable-topic service so authorization (not service availability) is what
        // determines the response. The mock is never invoked because authorization fails first.
        when(brokerService.getScalableTopicService()).thenReturn(mock(ScalableTopicService.class));

        long requestId = 10L;
        channel.writeInbound(Commands.newScalableTopicSubscribe(requestId, TOPIC, SUBSCRIPTION,
                "test-consumer", 1L, ScalableConsumerType.STREAM));

        Object response = readResponse();
        assertTrue(response instanceof CommandScalableTopicSubscribeResponse,
                "Expected CommandScalableTopicSubscribeResponse, got " + response);
        CommandScalableTopicSubscribeResponse subResp = (CommandScalableTopicSubscribeResponse) response;
        assertEquals(subResp.getRequestId(), requestId);
        assertTrue(subResp.hasError());
        assertEquals(subResp.getError(), ServerError.AuthorizationError);

        TopicName topicName = TopicName.get(TOPIC);
        verify(authorizationService, times(1)).allowTopicOperationAsync(
                eq(topicName), eq(TopicOperation.CONSUME), eq(UNAUTHORIZED_ROLE),
                argThat(arg -> arg instanceof AuthenticationDataSubscription
                        && SUBSCRIPTION.equals(((AuthenticationDataSubscription) arg).getSubscription())));
    }

    @Test
    public void testScalableTopicCloseDoesNotCallAuthorization() throws Exception {
        configureAuthAndConnect(UNAUTHORIZED_ROLE);

        // Send a Close for an unknown sessionId. The handler must treat this as a no-op:
        // no response, no AuthorizationError, and the AuthorizationService must not be consulted.
        channel.writeInbound(Commands.newScalableTopicClose(99999L));
        channel.runPendingTasks();

        assertTrue(channel.outboundMessages().isEmpty(),
                "ScalableTopicClose for an unknown session must not emit any response");
        verify(authorizationService, never()).allowTopicOperationAsync(any(), any(), any(), any());
    }

    @Test
    public void testScalableTopicLookupSkipsAuthzWhenFeatureDisabled() throws Exception {
        svcConfig.setScalableTopicsEnabled(false);
        configureAuthAndConnect(AUTHORIZED_ROLE);

        long sessionId = 3L;
        channel.writeInbound(Commands.newScalableTopicLookup(sessionId, TOPIC));

        Object response = readResponse();
        assertTrue(response instanceof CommandScalableTopicUpdate);
        CommandScalableTopicUpdate update = (CommandScalableTopicUpdate) response;
        assertEquals(update.getSessionId(), sessionId);
        assertTrue(update.hasError());
        assertEquals(update.getError(), ServerError.NotAllowedError,
                "Disabled feature must short-circuit before authorization");

        verify(authorizationService, never()).allowTopicOperationAsync(any(), any(), any(), any());
    }

    @Test
    public void testScalableTopicSubscribeSkipsAuthzWhenFeatureDisabled() throws Exception {
        svcConfig.setScalableTopicsEnabled(false);
        configureAuthAndConnect(AUTHORIZED_ROLE);

        long requestId = 11L;
        channel.writeInbound(Commands.newScalableTopicSubscribe(requestId, TOPIC, SUBSCRIPTION,
                "test-consumer", 1L, ScalableConsumerType.STREAM));

        Object response = readResponse();
        assertTrue(response instanceof CommandScalableTopicSubscribeResponse);
        CommandScalableTopicSubscribeResponse subResp = (CommandScalableTopicSubscribeResponse) response;
        assertEquals(subResp.getRequestId(), requestId);
        assertTrue(subResp.hasError());
        assertEquals(subResp.getError(), ServerError.NotAllowedError);

        verify(authorizationService, never()).allowTopicOperationAsync(any(), any(), any(), any());
    }

    private void configureAuthAndConnect(String role) throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationProvider authenticationProvider = new MockAuthenticationProvider();
        String authMethodName = authenticationProvider.getAuthMethodName();
        when(brokerService.getAuthenticationService()).thenReturn(authenticationService);
        when(authenticationService.getAuthenticationProvider(authMethodName)).thenReturn(authenticationProvider);
        svcConfig.setAuthenticationEnabled(true);

        svcConfig.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        authorizationService = spyWithClassAndConstructorArgsRecordingInvocations(
                AuthorizationService.class, svcConfig, pulsarTestContext.getPulsarResources());
        when(brokerService.getAuthorizationService()).thenReturn(authorizationService);
        svcConfig.setAuthorizationEnabled(true);

        resetChannel();
        ByteBuf connect = Commands.newConnect(authMethodName, role, "test");
        channel.writeInbound(connect);

        Object response = readResponse();
        assertTrue(response instanceof CommandConnected, "Expected CommandConnected, got " + response);
    }

    private void resetChannel() throws Exception {
        if (channel != null && channel.isActive()) {
            serverCnx.close();
            channel.close().get();
        }
        serverCnx = new ServerCnx(pulsar);
        serverCnx.setAuthRole("");
        channel = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4),
                (ChannelHandler) serverCnx);
    }

    private Object readResponse() throws Exception {
        long sleepMs = 10;
        long iterations = TimeUnit.SECONDS.toMillis(10) / sleepMs;
        for (int i = 0; i < iterations; i++) {
            channel.runPendingTasks();
            if (!channel.outboundMessages().isEmpty()) {
                Object outbound = channel.outboundMessages().remove();
                Object cmd = clientChannelHelper.getCommand(outbound);
                if (cmd != null) {
                    return cmd;
                }
            }
            Thread.sleep(sleepMs);
        }
        throw new AssertionError("Timed out waiting for response");
    }
}
