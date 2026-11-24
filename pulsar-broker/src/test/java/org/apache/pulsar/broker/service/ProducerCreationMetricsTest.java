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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class ProducerCreationMetricsTest {
    private static final String LEGACY_FAILURE_METRIC = "brk_producer_create_fail_count";

    private PulsarTestContext context;
    private BrokerService brokerService;
    private EmbeddedChannel channel;
    private String topicName;

    @BeforeMethod
    public void setup() throws Exception {
        var config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        context = PulsarTestContext.builderForNonStartableContext()
                .config(config).spyByDefault().build();
        brokerService = context.getBrokerService();
        topicName = BrokerTestUtil.newUniqueName("persistent://public/default/producer-metrics");
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4),
                new ServerCnx(context.getPulsarService()));
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
    }

    @DataProvider
    public Object[][] topicLoadFailures() {
        return new Object[][] {
                {new BrokerServiceException.ServerMetadataException("policy lookup failed"), ServerError.MetadataError},
                {new BrokerServiceException.PersistenceException("ledger unavailable"), ServerError.PersistenceError},
                {new BrokerServiceException.ServiceUnitNotReadyException("unloading"), ServerError.ServiceNotReady},
                {new BrokerServiceException.TopicNotFoundException("missing topic"), ServerError.TopicNotFound},
                {new IllegalStateException("unexpected load failure"), ServerError.UnknownError}
        };
    }

    @Test(dataProvider = "topicLoadFailures")
    public void topicLoadFailureIsCountedOnce(Throwable failure, ServerError error) {
        stubTopicLoad(CompletableFuture.failedFuture(failure));
        createProducer(1, 1);
        assertError(error, 1);
        channel.runPendingTasks();
        assertFailureCounts(Map.of(error, 1L));
    }

    @Test
    public void invalidTopicIsCounted() {
        channel.writeInbound(Commands.newProducer("invalid://topic", 1, 1, "producer", Map.of(), false));
        assertError(ServerError.InvalidTopicName, 1);
        assertFailureCounts(Map.of(ServerError.InvalidTopicName, 1L));
    }

    @Test
    public void missingTopicIsCounted() {
        stubTopicLoad(CompletableFuture.completedFuture(Optional.empty()));
        createProducer(1, 1);
        assertError(ServerError.TopicNotFound, 1);
        assertFailureCounts(Map.of(ServerError.TopicNotFound, 1L));
    }

    @DataProvider
    public Object[][] authorizationResults() {
        return new Object[][] {
                {CompletableFuture.completedFuture(false)},
                {CompletableFuture.failedFuture(new IllegalStateException("authorization unavailable"))}
        };
    }

    @Test(dataProvider = "authorizationResults")
    public void authorizationFailureIsCounted(CompletableFuture<Boolean> result) {
        var authorization = mock(AuthorizationService.class);
        doReturn(true).when(brokerService).isAuthorizationEnabled();
        doReturn(authorization).when(brokerService).getAuthorizationService();
        when(authorization.allowTopicOperationAsync(any(), any(), any(), any(), any(), any())).thenReturn(result);
        createProducer(1, 1);
        assertError(ServerError.AuthorizationError, 1);
        assertFailureCounts(Map.of(ServerError.AuthorizationError, 1L));
    }

    @Test
    public void schemaFailureIsCounted() {
        var topic = stubTopic();
        when(topic.hasSchema()).thenReturn(CompletableFuture.failedFuture(
                new IncompatibleSchemaException("schema mismatch")));
        createProducer(1, 1);
        assertError(ServerError.IncompatibleSchema, 1);
        assertFailureCounts(Map.of(ServerError.IncompatibleSchema, 1L));
    }

    @Test
    public void registrationFailureIsCounted() {
        var topic = stubTopic();
        when(topic.addProducer(any(), any())).thenReturn(CompletableFuture.failedFuture(
                new BrokerServiceException.ProducerBusyException("producer already exists")));
        createProducer(1, 1);
        assertError(ServerError.ProducerBusy, 1);
        assertFailureCounts(Map.of(ServerError.ProducerBusy, 1L));
    }

    @Test
    public void producerLimitFailureIsCounted() {
        var topic = stubTopic();
        when(topic.isProducersExceeded(anyString())).thenReturn(true);
        createProducer(1, 1);
        assertError(ServerError.ProducerBusy, 1);
        assertFailureCounts(Map.of(ServerError.ProducerBusy, 1L));
    }

    @Test
    public void encryptionRequirementFailureIsCounted() {
        var topic = stubTopic();
        when(topic.isEncryptionRequired()).thenReturn(true);
        createProducer(1, 1);
        assertError(ServerError.MetadataError, 1);
        assertFailureCounts(Map.of(ServerError.MetadataError, 1L));
    }

    @DataProvider
    public Object[][] backlogQuotaFailures() {
        return new Object[][] {
                {BacklogQuota.RetentionPolicy.producer_request_hold, ServerError.ProducerBlockedQuotaExceededError},
                {BacklogQuota.RetentionPolicy.producer_exception, ServerError.ProducerBlockedQuotaExceededException}
        };
    }

    @Test(dataProvider = "backlogQuotaFailures")
    public void backlogQuotaFailureIsCounted(BacklogQuota.RetentionPolicy policy, ServerError error) {
        var topic = stubTopic();
        when(topic.checkBacklogQuotaExceeded(anyString(), any())).thenReturn(CompletableFuture.failedFuture(
                new BrokerServiceException.TopicBacklogQuotaExceededException(policy)));
        createProducer(1, 1);
        assertError(error, 1);
        assertFailureCounts(Map.of(error, 1L));
    }

    @DataProvider
    public Object[][] initialSubscriptionFailures() {
        return new Object[][] {
                {new BrokerServiceException.NotAllowedException("subscription creation disabled"),
                        ServerError.NotAllowedError},
                {new BrokerServiceException.PersistenceException("cursor ledger unavailable"),
                        ServerError.PersistenceError}
        };
    }

    @Test(dataProvider = "initialSubscriptionFailures")
    public void initialSubscriptionFailureIsCounted(Throwable failure, ServerError error) {
        var topic = stubTopic();
        when(topic.isPersistent()).thenReturn(true);
        doReturn(CompletableFuture.failedFuture(failure))
                .when(brokerService).isAllowAutoSubscriptionCreationAsync(any());
        channel.writeInbound(Commands.newProducer(topicName, 1, 1, "producer", false, Map.of(), null,
                0, false, ProducerAccessMode.Shared, Optional.empty(), false, "initial-subscription"));
        assertError(error, 1);
        assertFailureCounts(Map.of(error, 1L));
    }

    @Test
    public void successfulCreationAndDuplicateSuccessAreNotFailures() {
        stubTopic();
        createProducer(1, 1);
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.PRODUCER_SUCCESS);
        createProducer(1, 2);
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.PRODUCER_SUCCESS);
        assertFailureCounts(Map.of());
    }

    @Test
    public void queuedProducerIsNotAFailure() {
        var topic = stubTopic();
        var registration = new CompletableFuture<Optional<Long>>();
        doAnswer(invocation -> {
            CompletableFuture<Void> queued = invocation.getArgument(1);
            queued.complete(null);
            return registration;
        }).when(topic).addProducer(any(), any());
        createProducer(1, 1);
        var queued = readResponse();
        assertThat(queued.getType()).isEqualTo(BaseCommand.Type.PRODUCER_SUCCESS);
        assertThat(queued.getProducerSuccess().isProducerReady()).isFalse();
        assertFailureCounts(Map.of());
        registration.complete(Optional.empty());
        var ready = readResponse();
        assertThat(ready.getType()).isEqualTo(BaseCommand.Type.PRODUCER_SUCCESS);
        assertThat(ready.getProducerSuccess().isProducerReady()).isTrue();
        assertFailureCounts(Map.of());
    }

    @Test
    public void pendingDuplicateCountsOnlyItsErrorResponse() {
        stubTopicLoad(new CompletableFuture<>());
        createProducer(1, 1);
        channel.runPendingTasks();
        assertFailureCounts(Map.of());
        createProducer(1, 2);
        assertError(ServerError.ServiceNotReady, 2);
        assertFailureCounts(Map.of(ServerError.ServiceNotReady, 1L));
        channel.writeInbound(Commands.newCloseProducer(1, 3));
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.SUCCESS);
        assertFailureCounts(Map.of(ServerError.ServiceNotReady, 1L));
    }

    @Test
    public void lateFailureAfterClientCloseIsNotCounted() {
        var load = new CompletableFuture<Optional<Topic>>();
        stubTopicLoad(load);
        createProducer(1, 1);
        channel.runPendingTasks();
        channel.writeInbound(Commands.newCloseProducer(1, 2));
        assertThat(readResponse().getType()).isEqualTo(BaseCommand.Type.SUCCESS);
        load.completeExceptionally(new BrokerServiceException.PersistenceException("late ledger failure"));
        channel.runPendingTasks();
        assertThat(channel.outboundMessages()).isEmpty();
        assertFailureCounts(Map.of());
    }

    @Test
    public void separateFailedRequestsAreCountedSeparately() {
        stubTopicLoad(CompletableFuture.failedFuture(
                new BrokerServiceException.PersistenceException("ledger unavailable")));
        createProducer(1, 1);
        assertError(ServerError.PersistenceError, 1);
        createProducer(2, 2);
        assertError(ServerError.PersistenceError, 2);
        assertFailureCounts(Map.of(ServerError.PersistenceError, 2L));
    }

    private PersistentTopic stubTopic() {
        var topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(topicName);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getShadowSourceTopic()).thenReturn(Optional.empty());
        when(topic.checkBacklogQuotaExceeded(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(topic.hasSchema()).thenReturn(CompletableFuture.completedFuture(false));
        when(topic.addProducer(any(), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        stubTopicLoad(CompletableFuture.completedFuture(Optional.of(topic)));
        return topic;
    }

    private void stubTopicLoad(CompletableFuture<Optional<Topic>> load) {
        doReturn(load).when(brokerService).getTopicCreatingIfAllowed(any(TopicName.class), any());
    }

    private void createProducer(long producerId, long requestId) {
        channel.writeInbound(Commands.newProducer(topicName, producerId, requestId,
                "producer-" + producerId, Map.of(), false));
    }

    private void assertError(ServerError error, long requestId) {
        var response = readResponse();
        assertThat(response.getType()).isEqualTo(BaseCommand.Type.ERROR);
        assertThat(response.getError().getError()).isEqualTo(error);
        assertThat(response.getError().getRequestId()).isEqualTo(requestId);
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

    private void assertFailureCounts(Map<ServerError, Long> expected) {
        var expectedByError = expected.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().name(), Map.Entry::getValue));
        var legacy = brokerService.getPulsarStats().getBrokerOperabilityMetrics().getMetrics().stream()
                .filter(metric -> metric.getMetrics().containsKey(LEGACY_FAILURE_METRIC))
                .filter(metric -> ((Number) metric.getMetrics().get(LEGACY_FAILURE_METRIC)).longValue() > 0)
                .collect(Collectors.toMap(metric -> metric.getDimensions().get("error"),
                        metric -> ((Number) metric.getMetrics().get(LEGACY_FAILURE_METRIC)).longValue()));
        assertThat(legacy).as("Prometheus producer creation failures").isEqualTo(expectedByError);
    }
}
