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
package org.apache.pulsar.broker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BookKeeperClientContextTest {

    @Test
    public void testDefaultClientDoesNotAddPlacementMetadata() throws Exception {
        BookKeeper bookKeeper = mock(BookKeeper.class);
        BookKeeperClientContext context = BookKeeperClientContext.create(bookKeeper, null);
        Map<String, byte[]> metadata = Map.of("component", "schema".getBytes(StandardCharsets.UTF_8));

        assertThat(context.getBookKeeper()).isSameAs(bookKeeper);
        assertThat(context.withPlacementMetadata(metadata)).isSameAs(metadata);
        assertThat(metadata).doesNotContainKey(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
    }

    @Test
    public void testPlacementMetadataMatchesClientPolicy() throws Exception {
        EnsemblePlacementPolicyConfig policy = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary",
                        IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "secondary"));
        BookKeeperClientContext context = BookKeeperClientContext.create(mock(BookKeeper.class), policy);
        byte[] stalePolicy = "stale".getBytes(StandardCharsets.UTF_8);
        Map<String, byte[]> metadata = Map.of(
                "component", "schema".getBytes(StandardCharsets.UTF_8),
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, stalePolicy);

        Map<String, byte[]> firstResult = context.withPlacementMetadata(metadata);
        byte[] firstEncodedPolicy = firstResult.get(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);

        assertThat(EnsemblePlacementPolicyConfig.decode(firstEncodedPolicy)).isEqualTo(policy);
        assertThat(metadata.get(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG))
                .isSameAs(stalePolicy);

        firstEncodedPolicy[0] = (byte) (firstEncodedPolicy[0] + 1);
        byte[] secondEncodedPolicy = context.withPlacementMetadata(metadata).get(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        assertThat(secondEncodedPolicy).isNotSameAs(firstEncodedPolicy);
        assertThat(EnsemblePlacementPolicyConfig.decode(secondEncodedPolicy)).isEqualTo(policy);
    }

    @Test
    public void testStorageClassDoesNotSilentlyIgnoreCustomPolicy() {
        BookKeeper defaultClient = mock(BookKeeper.class);
        BookkeeperManagedLedgerStorageClass storageClass = new BookkeeperManagedLedgerStorageClass() {
            @Override
            public BookKeeper getBookKeeperClient() {
                return defaultClient;
            }

            @Override
            public StatsProvider getStatsProvider() {
                return null;
            }

            @Override
            public String getName() {
                return "test";
            }

            @Override
            public ManagedLedgerFactory getManagedLedgerFactory() {
                return null;
            }
        };
        EnsemblePlacementPolicyConfig policy = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class, Map.of());

        assertThat(storageClass.getBookKeeperClient(null).join()).isSameAs(defaultClient);
        assertThatThrownBy(() -> storageClass.getBookKeeperClient(policy).join())
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
