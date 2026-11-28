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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Every response of a partitioned topic metadata request is reported to {@link PulsarStats} exactly once. The
 * metrics that the report feeds are covered by {@code BrokerOperabilityMetricsTest}, and end to end with a real
 * client by {@code OpenTelemetryBrokerOperabilityStatsTest}.
 */
@Test(groups = "broker", timeOut = 30000)
public class PartitionedMetadataRequestMetricsTest {
    private ServiceConfiguration config;
    private PulsarTestContext context;
    private PulsarService pulsar;
    private BrokerService brokerService;
    private NamespaceService namespaceService;
    private PulsarStats pulsarStats;
    private BrokerInterceptor interceptor;
    private EmbeddedChannel channel;
    private String topicName;
    private TopicName topic;
    private TopicExistsInfo lastExistingTopicInfo;
    // Mockito's inline mock maker keeps these until they are cleared, so they are released with the broker.
    private final List<Object> ownedMocks = new CopyOnWriteArrayList<>();

    @BeforeMethod
    public void setup() throws Exception {
        config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        interceptor = mock(BrokerInterceptor.class, CALLS_REAL_METHODS);
        ownedMocks.add(interceptor);
        context = PulsarTestContext.builderForNonStartableContext()
                .config(config).spyByDefault().build();
        pulsar = context.getPulsarService();
        brokerService = context.getBrokerService();
        // A broker that is never started does not get its interceptor from the Pulsar service.
        brokerService.setInterceptor(interceptor);
        namespaceService = pulsar.getNamespaceService();
        pulsarStats = mock(PulsarStats.class);
        ownedMocks.add(pulsarStats);
        doReturn(pulsarStats).when(brokerService).getPulsarStats();
        topicName = BrokerTestUtil.newUniqueName("persistent://public/default/partitioned-metadata-metrics");
        topic = TopicName.get(topicName);
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4),
                new ServerCnx(pulsar));
        channel.writeInbound(Commands.newConnect("none", "", null));
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.CONNECTED);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        if (context != null) {
            context.close();
        }
        ownedMocks.forEach(mock -> Mockito.framework().clearInlineMock(mock));
        ownedMocks.clear();
        // TestNG keeps this test instance until the run ends.
        context = null;
        pulsar = null;
        brokerService = null;
        namespaceService = null;
        pulsarStats = null;
        interceptor = null;
        channel = null;
        lastExistingTopicInfo = null;
    }

    @Test
    public void invalidTopicNameIsCounted() {
        request("xx://xx/xx/invalid", 1);
        assertError(1, ServerError.InvalidTopicName);
        assertFailureRecorded(ServerError.InvalidTopicName);
    }

    @Test
    public void serviceNotReadyIsCounted() {
        doReturn(false).when(pulsar).isRunning();
        request(1);
        assertError(1, ServerError.ServiceNotReady);
        assertFailureRecorded(ServerError.ServiceNotReady);
    }

    @Test
    public void tooManyRequestsIsCounted() {
        Semaphore lookupSemaphore = brokerService.getLookupRequestSemaphore();
        int permits = lookupSemaphore.availablePermits();
        lookupSemaphore.acquireUninterruptibly(permits);
        try {
            request(1);
            assertError(1, ServerError.TooManyRequests);
        } finally {
            lookupSemaphore.release(permits);
        }
        assertFailureRecorded(ServerError.TooManyRequests);
    }

    @DataProvider
    public Object[][] authorizationResults() {
        return new Object[][] {
                {CompletableFuture.completedFuture(false), ServerError.AuthorizationError},
                {CompletableFuture.failedFuture(new IllegalStateException("authorization unavailable")),
                        ServerError.AuthorizationError},
                {CompletableFuture.failedFuture(new RestException(Response.Status.NOT_FOUND, "Namespace not found")),
                        ServerError.TopicNotFound}
        };
    }

    @Test(dataProvider = "authorizationResults")
    public void authorizationFailureIsCounted(CompletableFuture<Boolean> authorization, ServerError error) {
        var authorizationService = enableAuthorization();
        doReturn(authorization).when(authorizationService)
                .allowTopicOperationAsync(any(), any(), any(), any(), any(), any());
        request(1);
        assertError(1, error);
        assertFailureRecorded(error);
    }

    @Test
    public void autoCreationPolicyFailureIsCounted() {
        doReturn(CompletableFuture.failedFuture(new MetadataStoreException("Namespace policy read failed")))
                .when(brokerService).isAllowAutoTopicCreationAsync(topic);
        request(1);
        assertError(1, ServerError.MetadataError);
        assertFailureRecorded(ServerError.MetadataError);
    }

    @DataProvider
    public Object[][] existingTopics() {
        return new Object[][] {
                {false, TopicType.NON_PARTITIONED, 0, ServerError.TopicNotFound},
                {true, TopicType.PARTITIONED, 3, null},
                {true, TopicType.NON_PARTITIONED, 0, null}
        };
    }

    @Test(dataProvider = "existingTopics")
    public void existingTopicResultIsCounted(boolean exists, TopicType type, int partitions, ServerError error) {
        disallowAutoCreation();
        stubExistingTopic(exists, type, partitions);
        request(1);
        if (error == null) {
            assertPartitions(1, partitions);
            assertSuccessRecorded();
        } else {
            assertError(1, error);
            assertFailureRecorded(error);
        }
    }

    @Test
    public void existingTopicLookupFailureIsCounted() {
        disallowAutoCreation();
        doReturn(CompletableFuture.failedFuture(new MetadataStoreException("Metadata read failed")))
                .when(namespaceService).checkTopicExistsAsync(topic);
        request(1);
        assertError(1, ServerError.MetadataError);
        assertFailureRecorded(ServerError.MetadataError);
    }

    @DataProvider
    public Object[][] autoCreationFailures() {
        return new Object[][] {
                {new RestException(Response.Status.NOT_FOUND, "Not found"), ServerError.TopicNotFound},
                {new RestException(Response.Status.BAD_REQUEST, "Invalid metadata"), ServerError.MetadataError},
                {new RestException(Response.Status.INTERNAL_SERVER_ERROR, "Unavailable"), ServerError.ServiceNotReady},
                {new MetadataStoreException("Metadata read failed"), ServerError.MetadataError},
                {new PulsarClientException.AuthorizationException("Denied"), ServerError.AuthorizationError},
                {new IllegalArgumentException("Too many partitions"), ServerError.NotAllowedError},
                {new IllegalStateException("Lookup failed"), ServerError.ServiceNotReady}
        };
    }

    @Test(dataProvider = "autoCreationFailures")
    public void autoCreationFailureIsCounted(Throwable failure, ServerError error) throws Exception {
        allowAutoCreation();
        doReturn(CompletableFuture.failedFuture(new CompletionException(failure)))
                .when(brokerService).fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(eq(topic), any());
        request(1);
        assertError(1, error);
        assertFailureRecorded(error);
    }

    @Test
    public void autoCreatedTopicIsCounted() throws Exception {
        allowAutoCreation();
        stubCreationCheckedLookup();
        request(1);
        assertPartitions(1, 3);
        assertSuccessRecorded();
    }

    @Test
    public void refusedAutoCreationIsCounted() throws Exception {
        allowAutoCreation();
        stubCreationCheckedLookup();
        var authorizationService = enableAuthorization();
        doReturn(CompletableFuture.completedFuture(false)).when(authorizationService)
                .allowTopicAutoCreationAsync(any(), any(), any(), any(), any());
        request(1);
        assertError(1, ServerError.TopicNotFound);
        assertFailureRecorded(ServerError.TopicNotFound);
    }

    @Test
    public void autoCreationCheckFailureIsCounted() throws Exception {
        allowAutoCreation();
        stubCreationCheckedLookup();
        var authorizationService = enableAuthorization();
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("authorization unavailable")))
                .when(authorizationService).allowTopicAutoCreationAsync(any(), any(), any(), any(), any());
        request(1);
        assertError(1, ServerError.AuthorizationError);
        assertFailureRecorded(ServerError.AuthorizationError);
    }

    @Test
    public void cleanupFailureAfterResponseIsNotCountedAgain() {
        disallowAutoCreation();
        stubExistingTopic(true, TopicType.PARTITIONED, 3);
        doThrow(new IllegalStateException("Recycle failed")).when(lastExistingTopicInfo).recycle();
        request(1);
        channel.runPendingTasks();
        assertPartitions(1, 3);
        // The response was already recorded when the cleanup failed.
        assertSuccessRecorded();
    }

    @DataProvider
    public Object[][] interceptorErrorCodes() {
        return new Object[][] {
                {ServerError.NotAllowedError.getValue(), ServerError.NotAllowedError},
                {Integer.MAX_VALUE, ServerError.UnknownError}
        };
    }

    /** A request that a broker interceptor rejects fails before it reaches the handler, which never times it. */
    @Test(dataProvider = "interceptorErrorCodes")
    public void interceptorRejectionIsCounted(int errorCode, ServerError error) throws Exception {
        doThrow(new InterceptException(errorCode, "rejected by interceptor")).when(interceptor)
                .onPulsarCommand(any(), any());
        request(1);
        assertError(1, error);
        verify(pulsarStats).recordPartitionedMetadataRequestFailure(eq(0L), eq(error));
        verify(pulsarStats, never()).recordPartitionedMetadataRequestDuration(anyLong());
    }

    private void request(long requestId) {
        request(topicName, requestId);
    }

    private void request(String topic, long requestId) {
        channel.writeInbound(Commands.newPartitionMetadataRequest(topic, requestId, true));
    }

    private CommandPartitionedTopicMetadataResponse response() {
        BaseCommand command = readResponse();
        assertThat(command.getType()).isEqualTo(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
        // Tasks queued behind the response, such as a second response to the same request, run before it is
        // counted.
        channel.runPendingTasks();
        assertThat(channel.outboundMessages()).as("a request gets exactly one response").isEmpty();
        return command.getPartitionMetadataResponse();
    }

    private void assertError(long requestId, ServerError error) {
        var response = response();
        assertThat(response.getRequestId()).isEqualTo(requestId);
        assertThat(response.getError()).isEqualTo(error);
    }

    private void assertPartitions(long requestId, int partitions) {
        var response = response();
        assertThat(response.getRequestId()).isEqualTo(requestId);
        assertThat(response.hasError()).isFalse();
        assertThat(response.getPartitions()).isEqualTo(partitions);
    }

    /** The request is reported as one failure with the error, and as nothing else. */
    private void assertFailureRecorded(ServerError error) {
        verify(pulsarStats).recordPartitionedMetadataRequestFailure(anyLong(), eq(error));
        verify(pulsarStats, never()).recordPartitionedMetadataRequestDuration(anyLong());
    }

    /** The request is reported as one duration sample, and as no failure. */
    private void assertSuccessRecorded() {
        verify(pulsarStats).recordPartitionedMetadataRequestDuration(anyLong());
        verify(pulsarStats, never()).recordPartitionedMetadataRequestFailure(anyLong(), any());
    }

    private BaseCommand readResponse() {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            channel.runPendingTasks();
            return !channel.outboundMessages().isEmpty();
        });
        ByteBuf frame = channel.readOutbound();
        try {
            frame.readInt();
            int commandSize = frame.readInt();
            var command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            return new BaseCommand().copyFrom(command);
        } finally {
            frame.release();
        }
    }

    private AuthorizationService enableAuthorization() {
        config.setAuthorizationEnabled(true);
        var authorizationService = mock(AuthorizationService.class);
        ownedMocks.add(authorizationService);
        doReturn(authorizationService).when(brokerService).getAuthorizationService();
        doReturn(CompletableFuture.completedFuture(true)).when(authorizationService)
                .allowTopicOperationAsync(any(), any(), any(), any(), any(), any());
        return authorizationService;
    }

    private void disallowAutoCreation() {
        doReturn(CompletableFuture.completedFuture(false)).when(brokerService).isAllowAutoTopicCreationAsync(topic);
    }

    private void allowAutoCreation() throws MetadataStoreException {
        Policies policies = new Policies();
        policies.replication_clusters.add(config.getClusterName());
        context.getPulsarResources().getNamespaceResources().createPolicies(topic.getNamespaceObject(), policies);
        doReturn(CompletableFuture.completedFuture(true)).when(brokerService).isAllowAutoTopicCreationAsync(topic);
    }

    private void stubExistingTopic(boolean exists, TopicType type, int partitions) {
        var info = mock(TopicExistsInfo.class);
        ownedMocks.add(info);
        when(info.isExists()).thenReturn(exists);
        when(info.getTopicType()).thenReturn(type);
        when(info.getPartitions()).thenReturn(partitions);
        lastExistingTopicInfo = info;
        doReturn(CompletableFuture.completedFuture(info)).when(namespaceService).checkTopicExistsAsync(topic);
    }

    /**
     * Stands in for the metadata lookup of the auto-creation path, as when the topic does not exist yet: the client
     * may create it only if its creation check allows it.
     */
    private void stubCreationCheckedLookup() {
        doAnswer(invocation -> {
            Supplier<CompletableFuture<Boolean>> creationCheck = invocation.getArgument(1);
            return creationCheck.get().thenApply(allowed -> new PartitionedTopicMetadata(allowed ? 3 : 0));
        }).when(brokerService).fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(eq(topic), any());
    }
}
