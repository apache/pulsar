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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.storage.BookKeeperClientContext;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarServiceBookKeeperClientContextTest {

    private static final TopicName TOPIC = TopicName.get("persistent://tenant/namespace/topic");

    @Test
    public void testNoPolicyUsesCallerClientWithNonBookKeeperDefaultStorage() {
        ManagedLedgerStorage managedLedgerStorage = mock(ManagedLedgerStorage.class);
        when(managedLedgerStorage.getDefaultStorageClass()).thenReturn(mock(ManagedLedgerStorageClass.class));
        PulsarService pulsar = mockPulsar(new ServiceConfiguration(), managedLedgerStorage);
        BookKeeper callerClient = mock(BookKeeper.class);

        BookKeeperClientContext context = pulsar.getBookKeeperClientContext(TOPIC, () -> callerClient).join();

        assertThat(context.getBookKeeper()).isSameAs(callerClient);
        assertThat(context.withPlacementMetadata(Map.of()))
                .doesNotContainKey(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        verify(managedLedgerStorage, never()).getDefaultStorageClass();
    }

    @Test
    public void testCustomPolicyUsesPolicyClientWithoutCallingFallback() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setStrictBookieAffinityEnabled(true);
        ManagedLedgerStorage managedLedgerStorage = mock(ManagedLedgerStorage.class);
        BookkeeperManagedLedgerStorageClass storageClass = mock(BookkeeperManagedLedgerStorageClass.class);
        BookKeeper policyClient = mock(BookKeeper.class);
        when(managedLedgerStorage.getDefaultStorageClass()).thenReturn(storageClass);
        when(storageClass.getBookKeeperClient(any(EnsemblePlacementPolicyConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(policyClient));
        PulsarService pulsar = mockPulsar(configuration, managedLedgerStorage);
        AtomicBoolean fallbackCalled = new AtomicBoolean();

        BookKeeperClientContext context = pulsar.getBookKeeperClientContext(TOPIC, () -> {
            fallbackCalled.set(true);
            return mock(BookKeeper.class);
        }).join();

        assertThat(context.getBookKeeper()).isSameAs(policyClient);
        assertThat(context.withPlacementMetadata(Map.of()))
                .containsKey(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        assertThat(fallbackCalled.get()).isFalse();
        verify(storageClass).getBookKeeperClient(any(EnsemblePlacementPolicyConfig.class));
    }

    @Test
    public void testCustomPolicyFailsWithNonBookKeeperDefaultStorage() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setStrictBookieAffinityEnabled(true);
        ManagedLedgerStorage managedLedgerStorage = mock(ManagedLedgerStorage.class);
        when(managedLedgerStorage.getDefaultStorageClass()).thenReturn(mock(ManagedLedgerStorageClass.class));
        PulsarService pulsar = mockPulsar(configuration, managedLedgerStorage);
        AtomicBoolean fallbackCalled = new AtomicBoolean();

        CompletableFuture<BookKeeperClientContext> future = pulsar.getBookKeeperClientContext(TOPIC, () -> {
            fallbackCalled.set(true);
            return mock(BookKeeper.class);
        });

        assertThatThrownBy(future::join).hasCauseInstanceOf(UnsupportedOperationException.class);
        assertThat(fallbackCalled.get()).isFalse();
    }

    private static PulsarService mockPulsar(ServiceConfiguration configuration,
                                            ManagedLedgerStorage managedLedgerStorage) {
        PulsarService pulsar = mock(PulsarService.class, CALLS_REAL_METHODS);
        PulsarResources resources = mock(PulsarResources.class);
        LocalPoliciesResources localPolicies = mock(LocalPoliciesResources.class);
        doReturn(configuration).when(pulsar).getConfig();
        doReturn(resources).when(pulsar).getPulsarResources();
        doReturn(managedLedgerStorage).when(pulsar).getManagedLedgerStorage();
        when(resources.getLocalPolicies()).thenReturn(localPolicies);
        when(localPolicies.getLocalPoliciesAsync(TOPIC.getNamespaceObject()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        return pulsar;
    }
}
