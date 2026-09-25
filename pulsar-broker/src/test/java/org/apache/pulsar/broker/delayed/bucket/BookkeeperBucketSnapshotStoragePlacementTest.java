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
package org.apache.pulsar.broker.delayed.bucket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.BookKeeperClientContext;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BookkeeperBucketSnapshotStoragePlacementTest {

    private static final String TOPIC = "persistent://tenant/namespace/topic";

    @Test
    public void testCreateLedgerUsesTopicClientAndMatchingPlacementMetadata() throws Exception {
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfig()).thenReturn(new ServiceConfiguration());
        BookKeeper policyBookKeeper = mock(BookKeeper.class);
        EnsemblePlacementPolicyConfig placementPolicy = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary",
                        IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "secondary"));
        BookKeeperClientContext context = BookKeeperClientContext.create(policyBookKeeper, placementPolicy);
        when(pulsar.getBookKeeperClientContext(TopicName.get(TOPIC)))
                .thenReturn(CompletableFuture.completedFuture(context));

        CreateBuilder createBuilder = mock(CreateBuilder.class, RETURNS_SELF);
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        when(policyBookKeeper.newCreateLedgerOp()).thenReturn(createBuilder);
        doReturn(CompletableFuture.completedFuture(ledgerHandle)).when(createBuilder).execute();

        BookkeeperBucketSnapshotStorage storage = new BookkeeperBucketSnapshotStorage(pulsar);
        assertThat(storage.createLedger("bucket", TOPIC, "subscription").join()).isSameAs(ledgerHandle);

        verify(policyBookKeeper).newCreateLedgerOp();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, byte[]>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(createBuilder).withCustomMetadata(metadataCaptor.capture());
        Map<String, byte[]> metadata = metadataCaptor.getValue();
        assertThat(metadata.get(LedgerMetadataUtils.METADATA_PROPERTY_COMPONENT))
                .containsExactly(LedgerMetadataUtils.METADATA_PROPERTY_COMPONENT_DELAYED_INDEX_BUCKET);
        assertThat(new String(metadata.get(LedgerMetadataUtils.METADATA_PROPERTY_DELAYED_INDEX_TOPIC),
                StandardCharsets.UTF_8)).isEqualTo(TOPIC);
        assertThat(EnsemblePlacementPolicyConfig.decode(metadata.get(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG)))
                .isEqualTo(placementPolicy);
    }

    @Test
    public void testInvalidTopicCompletesFutureExceptionally() {
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfig()).thenReturn(new ServiceConfiguration());
        BookkeeperBucketSnapshotStorage storage = new BookkeeperBucketSnapshotStorage(pulsar);

        CompletableFuture<LedgerHandle> future = storage.createLedger("bucket", null, "subscription");

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).hasCauseInstanceOf(BucketSnapshotPersistenceException.class);
    }

    @Test
    public void testPlacementLookupFailureIsRetriablePersistenceFailure() {
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfig()).thenReturn(new ServiceConfiguration());
        RuntimeException lookupFailure = new RuntimeException("Failed to read local policies");
        when(pulsar.getBookKeeperClientContext(TopicName.get(TOPIC)))
                .thenReturn(FutureUtil.failedFuture(lookupFailure));
        BookkeeperBucketSnapshotStorage storage = new BookkeeperBucketSnapshotStorage(pulsar);

        CompletableFuture<LedgerHandle> future = storage.createLedger("bucket", TOPIC, "subscription");

        Throwable failure = future.handle((__, ex) -> FutureUtil.unwrapCompletionException(ex)).join();
        assertThat(failure).isInstanceOf(BucketSnapshotPersistenceException.class);
        assertThat(failure.getCause()).isSameAs(lookupFailure);
    }
}
