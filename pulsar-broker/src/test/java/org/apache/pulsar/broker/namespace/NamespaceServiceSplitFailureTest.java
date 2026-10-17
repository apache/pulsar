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
package org.apache.pulsar.broker.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceServiceSplitFailureTest {

    @DataProvider(name = "splitFailureTiming")
    public Object[][] splitFailureTiming() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "splitFailureTiming")
    public void testSplitFailsWhenBundleCacheLoadFails(boolean alreadyFailed) throws Exception {
        @Cleanup
        PulsarTestContext context = PulsarTestContext.builderForNonStartableContext().spyByDefault().build();
        PulsarService pulsar = context.getPulsarService();
        @Cleanup
        CoordinationServiceImpl coordinationService = new CoordinationServiceImpl(context.getLocalMetadataStore());
        @Cleanup("shutdown")
        OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1).name("split-failure-test").build();
        doReturn(coordinationService).when(pulsar).getCoordinationService();
        doReturn(executor).when(pulsar).getOrderedExecutor();
        assertThat(ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)).isFalse();

        @Cleanup
        NamespaceService namespaceService = new NamespaceService(pulsar);
        NamespaceName namespace = NamespaceName.get("prop/ns-split-cache-failure");
        NamespaceBundleFactory factory = namespaceService.getNamespaceBundleFactory();
        NamespaceBundle bundle = factory.getBundle(namespace.toString(), "0x00000000_0xffffffff");
        PulsarResources pulsarResources = pulsar.getPulsarResources();
        LocalPoliciesResources resources = spy(pulsarResources.getLocalPolicies());
        CompletableFuture<Optional<CacheGetResult<LocalPolicies>>> policiesFuture = new CompletableFuture<>();
        MetadataStoreException failure = new MetadataStoreException("Failed to read local policies for split");
        if (alreadyFailed) {
            policiesFuture.completeExceptionally(failure);
        }
        doReturn(policiesFuture).when(resources).getLocalPoliciesWithVersion(namespace);
        doReturn(resources).when(pulsarResources).getLocalPolicies();

        // The cache is empty: splitBundles must load policies through the real bundle factory.
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(bundle, false,
                NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO, null);
        if (!alreadyFailed) {
            assertThat(result).isNotDone();
            policiesFuture.completeExceptionally(failure);
        }
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class)
                .hasMessageContaining(failure.getMessage());
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    public void testSplitPreservesInvalidBoundaryFailure() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        // The real specified-positions algorithm accepts duplicates. Constructing the resulting
        // empty bundle throws inside NamespaceBundleFactory.splitBundles's thenApply stage.
        CompletableFuture<Void> result = context.namespaceService.splitAndOwnBundle(context.bundle, false,
                NamespaceBundleSplitAlgorithm.SPECIFIED_POSITIONS_DIVIDE_FORCE_ALGO,
                new ArrayList<>(List.of(1L, 1L)));
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty key range");
        assertThat(context.context.getPulsarService().getPulsarResources().getLocalPolicies()
                .getLocalPoliciesAsync(context.namespace).get(10, TimeUnit.SECONDS).orElseThrow().bundles
                .getNumBundles()).isEqualTo(1);
    }

    @Test
    public void testSplitCompletesWhenCompletionExecutorRejects() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        context.executor.shutdown();
        assertSplitRejected(context.split());
    }

    @Test
    public void testSplitCompletesWhenRetrySchedulerRejects() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        context.failPolicyUpdate(new MetadataStoreException.BadVersionException("split conflict"));
        context.scheduler.shutdown();
        assertSplitRejected(context.split());
    }

    @Test
    public void testSplitCompletesWhenRetryExecutorRejects() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        context.failPolicyUpdate(new MetadataStoreException.BadVersionException("split conflict"));
        // Shut down the real ordered executor only after the real scheduler accepts the retry.
        context.scheduler.beforeRetry = context.executor::shutdown;
        assertSplitRejected(context.split());
    }

    @Test
    public void testSplitRetriesVersionConflictsUntilLimit() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        MetadataStoreException.BadVersionException failure =
                new MetadataStoreException.BadVersionException("split conflict");
        AtomicInteger failedWrites = new AtomicInteger();
        for (int i = 0; i <= NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT; i++) {
            context.metadataStore.failConditional(failure, (operation, path) -> {
                if (operation == OperationType.PUT && path.equals(context.policiesPath())) {
                    failedWrites.incrementAndGet();
                    return true;
                }
                return false;
            });
        }
        CompletableFuture<Void> result = context.split();
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class)
                .hasRootCause(failure);
        assertThat(failedWrites).hasValue(NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT + 1);
    }

    @Test
    public void testSplitSucceedsAfterVersionConflict() throws Exception {
        @Cleanup
        SplitContext context = new SplitContext();
        context.failPolicyUpdate(new MetadataStoreException.BadVersionException("split conflict"));
        context.split().get(10, TimeUnit.SECONDS);
        PulsarResources resources = context.context.getPulsarService().getPulsarResources();
        assertThat(resources.getLocalPolicies().getLocalPoliciesAsync(context.namespace)
                .get(10, TimeUnit.SECONDS).orElseThrow().bundles.getNumBundles()).isEqualTo(2);
        assertThat(resources.getNamespaceResources().getPoliciesAsync(context.namespace)
                .get(10, TimeUnit.SECONDS).orElseThrow().bundles.getNumBundles()).isEqualTo(2);
    }

    private static void assertSplitRejected(CompletableFuture<Void> result) {
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);
    }

    private static class RetryScheduler extends ScheduledThreadPoolExecutor {
        private volatile Runnable beforeRetry;

        RetryScheduler() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return super.schedule(() -> {
                if (unit.toMillis(delay) == 100 && beforeRetry != null) {
                    beforeRetry.run();
                }
                command.run();
            }, delay, unit);
        }
    }

    private static class SplitContext implements AutoCloseable {
        private final FaultInjectionMetadataStore metadataStore;
        private final PulsarTestContext context;
        private final OrderedExecutor executor;
        private final RetryScheduler scheduler;
        private final NamespaceService namespaceService;
        private final NamespaceName namespace = NamespaceName.get("prop/ns-split-failure");
        private final NamespaceBundle bundle;

        SplitContext() throws Exception {
            metadataStore = new FaultInjectionMetadataStore(MetadataStoreFactoryImpl.createExtended("memory:local",
                    MetadataStoreConfig.builder().build()));
            ServiceConfiguration configuration = new ServiceConfiguration();
            configuration.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
            context = PulsarTestContext.builder().spyByDefault().config(configuration)
                    .localMetadataStore(metadataStore).configurationMetadataStore(metadataStore).build();
            PulsarService pulsar = context.getPulsarService();
            executor = OrderedExecutor.newBuilder().numThreads(1).name("split-failure-test").build();
            scheduler = new RetryScheduler();
            doReturn(executor).when(pulsar).getOrderedExecutor();
            doReturn(scheduler).when(pulsar).getExecutor();
            assertThat(ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)).isFalse();
            namespaceService = pulsar.getNamespaceService();
            bundle = namespaceService.getNamespaceBundleFactory()
                    .getBundle(namespace.toString(), "0x00000000_0xffffffff");
            pulsar.getPulsarResources().getLocalPolicies().createLocalPoliciesAsync(namespace,
                    new LocalPolicies(BundlesData.builder()
                            .boundaries(List.of("0x00000000", "0xffffffff")).numBundles(1).build(), null, null))
                    .get(10, TimeUnit.SECONDS);
        }

        CompletableFuture<Void> split() {
            return namespaceService.splitAndOwnBundle(bundle, false,
                    NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO, null);
        }

        String policiesPath() {
            return "/admin/local-policies/" + namespace;
        }

        void failPolicyUpdate(MetadataStoreException failure) {
            metadataStore.failConditional(failure,
                    (operation, path) -> operation == OperationType.PUT && path.equals(policiesPath()));
        }

        @Override
        public void close() throws Exception {
            PulsarService pulsar = context.getPulsarService();
            doCallRealMethod().when(pulsar).getOrderedExecutor();
            doCallRealMethod().when(pulsar).getExecutor();
            executor.shutdown();
            scheduler.shutdownNow();
            context.close();
            metadataStore.close();
        }
    }
}
