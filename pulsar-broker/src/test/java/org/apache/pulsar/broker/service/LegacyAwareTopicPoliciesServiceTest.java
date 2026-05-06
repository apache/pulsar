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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LegacyAwareTopicPoliciesServiceTest {

    @Test
    public void testRoutesOperationsByLegacyMarker() throws Exception {
        NamespaceName legacyNamespace = NamespaceName.get("tenant", "legacy");
        RecordingTopicPoliciesService systemTopicService = new RecordingTopicPoliciesService();
        RecordingTopicPoliciesService configuredService = new RecordingTopicPoliciesService();
        LegacyAwareTopicPoliciesService service = new LegacyAwareTopicPoliciesService(null, systemTopicService,
                configuredService, namespace -> CompletableFuture.completedFuture(namespace.equals(legacyNamespace)),
                __ -> { }, __ -> { });

        service.updateTopicPoliciesAsync(TopicName.get("persistent://tenant/legacy/topic"), false, false,
                policies -> { }).get();
        service.getTopicPoliciesAsync(TopicName.get("persistent://tenant/new/topic"),
                TopicPoliciesService.GetType.LOCAL_ONLY).get();
        service.deleteTopicPoliciesAsync(TopicName.get("persistent://tenant/legacy/topic")).get();

        assertEquals(systemTopicService.updateCount.get(), 1);
        assertEquals(systemTopicService.deleteCount.get(), 1);
        assertEquals(configuredService.getCount.get(), 1);
    }

    @Test
    public void testLegacyCheckFailureDoesNotRouteToConfiguredBackend() {
        RecordingTopicPoliciesService systemTopicService = new RecordingTopicPoliciesService();
        RecordingTopicPoliciesService configuredService = new RecordingTopicPoliciesService();
        LegacyAwareTopicPoliciesService service = new LegacyAwareTopicPoliciesService(null, systemTopicService,
                configuredService, __ -> CompletableFuture.failedFuture(new RuntimeException("failed marker check")),
                __ -> { }, __ -> { });

        assertThrows(ExecutionException.class, () -> service.getTopicPoliciesAsync(
                TopicName.get("persistent://tenant/ns/topic"), TopicPoliciesService.GetType.LOCAL_ONLY).get());
        assertEquals(systemTopicService.getCount.get(), 0);
        assertEquals(configuredService.getCount.get(), 0);
    }

    @Test
    public void testLoadsSystemTopicBackendOnlyForStillOwnedLegacyBundles() {
        NamespaceName namespace = NamespaceName.get("tenant", "ns");
        NamespaceBundle bundle = mock(NamespaceBundle.class);
        when(bundle.getNamespaceObject()).thenReturn(namespace);
        CompletableFuture<Boolean> markerCheck = new CompletableFuture<>();
        List<NamespaceBundle> loads = new ArrayList<>();
        List<NamespaceBundle> unloads = new ArrayList<>();
        LegacyAwareTopicPoliciesService service = new LegacyAwareTopicPoliciesService(null,
                new RecordingTopicPoliciesService(), new RecordingTopicPoliciesService(), __ -> markerCheck,
                loads::add, unloads::add);

        // Bundle loaded then unloaded before marker check completes
        service.onBundleLoaded(bundle);
        service.onBundleUnloaded(bundle);
        markerCheck.complete(true);
        assertTrue(loads.isEmpty());
        assertTrue(unloads.isEmpty());

        // Bundle loaded with synchronous marker check
        CompletableFuture<Boolean> secondMarkerCheck = CompletableFuture.completedFuture(true);
        service = new LegacyAwareTopicPoliciesService(null, new RecordingTopicPoliciesService(),
                new RecordingTopicPoliciesService(), __ -> secondMarkerCheck, loads::add, unloads::add);
        service.onBundleLoaded(bundle);
        assertEquals(loads, List.of(bundle));
        service.onBundleUnloaded(bundle);
        assertEquals(unloads, List.of(bundle));
    }

    @Test
    public void testResolveServiceReconcilesOwnedBundlesWhenLegacyStatusFlips() throws Exception {
        NamespaceName namespace = NamespaceName.get("tenant", "ns");
        NamespaceBundle bundle = mock(NamespaceBundle.class);
        when(bundle.getNamespaceObject()).thenReturn(namespace);
        AtomicBoolean isLegacy = new AtomicBoolean(false);
        List<NamespaceBundle> loads = new ArrayList<>();
        List<NamespaceBundle> unloads = new ArrayList<>();
        RecordingTopicPoliciesService systemTopicService = new RecordingTopicPoliciesService();
        RecordingTopicPoliciesService configuredService = new RecordingTopicPoliciesService();
        LegacyAwareTopicPoliciesService service = new LegacyAwareTopicPoliciesService(null, systemTopicService,
                configuredService, __ -> CompletableFuture.completedFuture(isLegacy.get()), loads::add, unloads::add);

        service.onBundleLoaded(bundle);
        assertEquals(service.resolveService(namespace).get(), configuredService);
        assertTrue(loads.isEmpty());

        isLegacy.set(true);
        assertEquals(service.resolveService(namespace).get(), systemTopicService);
        assertEquals(loads, List.of(bundle));

        isLegacy.set(false);
        assertEquals(service.resolveService(namespace).get(), configuredService);
        assertEquals(unloads, List.of(bundle));
    }

    @Test
    public void testRegisterAndUnregisterListenerOnBothBackends() {
        RecordingTopicPoliciesService systemTopicService = new RecordingTopicPoliciesService();
        RecordingTopicPoliciesService configuredService = new RecordingTopicPoliciesService();
        LegacyAwareTopicPoliciesService service = new LegacyAwareTopicPoliciesService(null, systemTopicService,
                configuredService, __ -> CompletableFuture.completedFuture(true),
                __ -> { }, __ -> { });

        TopicName topic = TopicName.get("persistent://tenant/ns/topic");
        TopicPolicyListener listener = policies -> { };

        assertTrue(service.registerListener(topic, listener));
        assertEquals(systemTopicService.registerListenerCount.get(), 1);
        assertEquals(configuredService.registerListenerCount.get(), 1);

        service.unregisterListener(topic, listener);
        assertEquals(systemTopicService.unregisterListenerCount.get(), 1);
        assertEquals(configuredService.unregisterListenerCount.get(), 1);
    }

    private static class RecordingTopicPoliciesService implements TopicPoliciesService {
        private final AtomicInteger getCount = new AtomicInteger();
        private final AtomicInteger updateCount = new AtomicInteger();
        private final AtomicInteger deleteCount = new AtomicInteger();
        private final AtomicInteger registerListenerCount = new AtomicInteger();
        private final AtomicInteger unregisterListenerCount = new AtomicInteger();

        @Override
        public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
            deleteCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                                boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                                Consumer<TopicPolicies> policyUpdater) {
            updateCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
            getCount.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
            registerListenerCount.incrementAndGet();
            return true;
        }

        @Override
        public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
            unregisterListenerCount.incrementAndGet();
        }
    }
}
