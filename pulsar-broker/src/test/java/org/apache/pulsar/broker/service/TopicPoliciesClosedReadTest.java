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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPoliciesClosedReadTest {
    private static final TopicName TOPIC = TopicName.get("persistent://tenant/ns/topic");

    @DataProvider(name = "types")
    public Object[][] types() {
        return new Object[][] {
                {TopicPoliciesService.GetType.LOCAL_ONLY},
                {TopicPoliciesService.GetType.GLOBAL_ONLY}
        };
    }

    @Test(dataProvider = "types")
    public void testClosedBeforeRead(TopicPoliciesService.GetType type) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.service.close();
            assertClosed(fixture.service.getTopicPoliciesAsync(TOPIC, type));
            assertThat(fixture.service.preparations).isZero();
        }
    }

    @Test(dataProvider = "types")
    public void testClosedWhileCachePreparationPending(TopicPoliciesService.GetType type) throws Exception {
        assertClosedDuringPreparation(type, true);
    }

    @Test(dataProvider = "types")
    public void testClosedWhenPreparationReturnsFalse(TopicPoliciesService.GetType type) throws Exception {
        assertClosedDuringPreparation(type, false);
    }

    private void assertClosedDuringPreparation(TopicPoliciesService.GetType type, boolean inserted) throws Exception {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<Optional<TopicPolicies>> result = fixture.service.getTopicPoliciesAsync(TOPIC, type);
            assertThat(result).isNotDone();
            assertThat(fixture.service.preparations).isEqualTo(1);
            fixture.service.close();
            fixture.service.prepared.complete(inserted);
            fixture.service.runContinuation();
            assertClosed(result);
        }
    }

    @Test(dataProvider = "types")
    public void testClosedAfterPreparationBeforeAsyncCacheRead(TopicPoliciesService.GetType type) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.service.prepared.complete(true);
            CompletableFuture<Optional<TopicPolicies>> result = fixture.service.getTopicPoliciesAsync(TOPIC, type);
            assertThat(fixture.service.prepared.continuations).hasSize(1);
            assertThat(result).isNotDone();
            fixture.service.close();
            fixture.service.runContinuation();
            assertClosed(result);
        }
    }

    @Test(dataProvider = "types")
    public void testActiveReadPreservesExistingPolicy(TopicPoliciesService.GetType type) throws Exception {
        try (Fixture fixture = new Fixture()) {
            TopicPolicies policy = new TopicPolicies();
            policy.setMaxConsumerPerTopic(17);
            if (type == TopicPoliciesService.GetType.LOCAL_ONLY) {
                fixture.service.policiesCache.put(TOPIC, policy);
            } else {
                fixture.service.globalPoliciesCache.put(TOPIC, policy);
            }
            fixture.service.prepared.complete(true);
            CompletableFuture<Optional<TopicPolicies>> result = fixture.service.getTopicPoliciesAsync(TOPIC, type);
            fixture.service.runContinuation();
            assertThat(result.get(5, TimeUnit.SECONDS)).containsSame(policy);
        }
    }

    @Test(dataProvider = "types")
    public void testActiveReadPreservesGenuineAbsence(TopicPoliciesService.GetType type) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.service.prepared.complete(true);
            CompletableFuture<Optional<TopicPolicies>> result = fixture.service.getTopicPoliciesAsync(TOPIC, type);
            fixture.service.runContinuation();
            assertThat(result.get(5, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test(dataProvider = "types")
    public void testStartupWithoutLoadManagerStillAvoidsPolicyInitialization(TopicPoliciesService.GetType type)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.loadManager.set(null);
            assertThat(fixture.service.getTopicPoliciesAsync(TOPIC, type).get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(fixture.service.preparations).isZero();
        }
    }

    @Test(dataProvider = "types")
    public void testStartupBeforeLoadManagerStartsStillAvoidsPolicyInitialization(TopicPoliciesService.GetType type)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            when(fixture.loadManager.get().started()).thenReturn(false);
            assertThat(fixture.service.getTopicPoliciesAsync(TOPIC, type).get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(fixture.service.preparations).isZero();
        }
    }

    @Test(dataProvider = "types")
    public void testClosedDuringStartupCheck(TopicPoliciesService.GetType type) throws Exception {
        try (Fixture fixture = new Fixture()) {
            TopicPolicies policy = new TopicPolicies();
            policy.setMaxConsumerPerTopic(17);
            fixture.service.policiesCache.put(TOPIC, policy);
            fixture.service.globalPoliciesCache.put(TOPIC, policy);
            when(fixture.loadManager.get().started()).thenAnswer(ignored -> {
                fixture.service.close();
                return false;
            });
            assertClosed(fixture.service.getTopicPoliciesAsync(TOPIC, type));
            assertThat(fixture.service.preparations).isZero();
        }
    }

    @Test(dataProvider = "types")
    public void testSelfTopicStillAvoidsPolicyInitializationAfterClose(TopicPoliciesService.GetType type)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.service.close();
            TopicName self = TopicName.get("persistent://tenant/ns/__change_events");
            assertThat(fixture.service.getTopicPoliciesAsync(self, type).get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(fixture.service.preparations).isZero();
        }
    }

    @Test(dataProvider = "types")
    public void testHeartbeatStillAvoidsPolicyInitializationAfterClose(TopicPoliciesService.GetType type)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.service.close();
            TopicName heartbeat = TopicName.get("persistent://pulsar/broker:8080/heartbeat");
            assertThat(fixture.service.getTopicPoliciesAsync(heartbeat, type).get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(fixture.service.preparations).isZero();
        }
    }

    private static void assertClosed(CompletableFuture<Optional<TopicPolicies>> result) {
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class);
    }

    private static final class PreparedFuture extends CompletableFuture<Boolean> {
        final Queue<Runnable> continuations = new ArrayDeque<>();

        @Override
        public <R> CompletableFuture<R> thenComposeAsync(
                Function<? super Boolean, ? extends CompletionStage<R>> action) {
            return super.thenComposeAsync(action, continuations::add);
        }
    }

    private static final class ControlledService extends SystemTopicBasedTopicPoliciesService {
        final PreparedFuture prepared = new PreparedFuture();
        int preparations;

        ControlledService(PulsarService pulsar) {
            super(pulsar);
            policyCacheInitMap.put(TOPIC.getNamespaceObject(), CompletableFuture.completedFuture(null));
        }

        @Override
        CompletableFuture<Boolean> prepareInitPoliciesCacheAsync(NamespaceName namespace) {
            preparations++;
            return prepared;
        }

        void runContinuation() {
            Runnable continuation = prepared.continuations.poll();
            assertThat(continuation).as("the actual asynchronous cache-read continuation").isNotNull();
            continuation.run();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final AtomicReference<LoadManager> loadManager = new AtomicReference<>(mock(LoadManager.class));
        final ControlledService service;

        Fixture() {
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration config = new ServiceConfiguration();
            config.setClusterName("test");
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getLoadManager()).thenReturn(loadManager);
            when(loadManager.get().started()).thenReturn(true);
            when(pulsar.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));
            PulsarBrokerOpenTelemetry telemetry = mock(PulsarBrokerOpenTelemetry.class);
            when(telemetry.getMeter()).thenReturn(OpenTelemetry.noop().getMeter("closed-policy-test"));
            when(pulsar.getOpenTelemetry()).thenReturn(telemetry);
            service = new ControlledService(pulsar);
        }

        @Override
        public void close() throws Exception {
            service.close();
        }
    }
}
