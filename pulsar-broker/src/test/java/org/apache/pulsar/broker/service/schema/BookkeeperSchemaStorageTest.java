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
package org.apache.pulsar.broker.service.schema;

import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.bkException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.storage.BookKeeperClientContext;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(groups = "broker")
public class BookkeeperSchemaStorageTest {

    @Test
    public void testBkException() {
        Exception ex = bkException("test", BKException.Code.ReadException, 1, -1, false);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test", ex.getMessage());
        ex = bkException("test", BKException.Code.ReadException, 1, 0, false);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, -1, false);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, 0, false);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
        SchemaException sc = (SchemaException) bkException("test",
                BKException.Code.BookieHandleNotAvailableException, 1, 0, false);
        assertTrue(sc.isRecoverable());
        sc = (SchemaException) bkException("test", BKException.Code.BookieHandleNotAvailableException, 1, 0, true);
        assertFalse(sc.isRecoverable());
    }

    @Test
    public void testVersionFromBytes() {
        long version = System.currentTimeMillis();

        ByteBuffer bbPre240 = ByteBuffer.allocate(Long.SIZE);
        bbPre240.putLong(version);
        byte[] versionBytesPre240 = bbPre240.array();

        ByteBuffer bbPost240 = ByteBuffer.allocate(Long.BYTES);
        bbPost240.putLong(version);
        byte[] versionBytesPost240 = bbPost240.array();

        PulsarService mockPulsarService = mock(PulsarService.class);
        when(mockPulsarService.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(mockPulsarService);
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPre240));
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPost240));
    }

    @DataProvider(name = "canonicalSchemaIds")
    public static Object[][] canonicalSchemaIds() {
        return new Object[][] {
                {"tenant/namespace/topic", TopicName.get("persistent://tenant/namespace/topic")},
                {"tenant/namespace/a%3Ab", TopicName.get("persistent://tenant/namespace/a:b")},
                {"tenant/namespace/a%2Fb", TopicName.get("topic://tenant/namespace/a/b")}
        };
    }

    @Test(dataProvider = "canonicalSchemaIds")
    public void testCreateLedgerUsesTopicPlacementClientAndMetadata(String schemaId, TopicName ownerTopic)
            throws Exception {
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());

        BookKeeper placementBookKeeper = mock(BookKeeper.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class, Mockito.RETURNS_SELF);
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        CompletableFuture<WriteHandle> createResult = CompletableFuture.completedFuture(ledgerHandle);
        doReturn(createBuilder).when(placementBookKeeper).newCreateLedgerOp();
        doReturn(createResult).when(createBuilder).execute();

        EnsemblePlacementPolicyConfig placementPolicy = new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary",
                        IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "secondary"));
        BookKeeperClientContext clientContext =
                BookKeeperClientContext.create(placementBookKeeper, placementPolicy);
        when(pulsar.getBookKeeperClientContext(ownerTopic))
                .thenReturn(CompletableFuture.completedFuture(clientContext));

        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(pulsar);
        assertThat(schemaStorage.createLedger(schemaId).join()).isSameAs(ledgerHandle);

        verify(pulsar).getBookKeeperClientContext(ownerTopic);
        verify(placementBookKeeper).newCreateLedgerOp();
        ArgumentCaptor<Map<String, byte[]>> metadataCaptor = ArgumentCaptor.captor();
        verify(createBuilder).withCustomMetadata(metadataCaptor.capture());
        Map<String, byte[]> metadata = metadataCaptor.getValue();
        LedgerMetadataUtils.buildMetadataForSchema(schemaId)
                .forEach((key, value) -> assertThat(metadata.get(key)).containsExactly(value));
        assertThat(EnsemblePlacementPolicyConfig.decode(metadata.get(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG)))
                .isEqualTo(placementPolicy);
    }

    @DataProvider(name = "nonCanonicalSchemaIds")
    public static Object[][] nonCanonicalSchemaIds() {
        return new Object[][] {
                {"tenant/cluster/namespace/topic"},
                {"id2"},
                {"tenant/namespace/a:b"}
        };
    }

    @Test(dataProvider = "nonCanonicalSchemaIds")
    public void testCreateLedgerUsesDefaultClientForNonCanonicalSchemaId(String schemaId) throws Exception {
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
        BookKeeper defaultBookKeeper = mock(BookKeeper.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class, Mockito.RETURNS_SELF);
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        CompletableFuture<WriteHandle> createResult = CompletableFuture.completedFuture(ledgerHandle);
        doReturn(createBuilder).when(defaultBookKeeper).newCreateLedgerOp();
        doReturn(createResult).when(createBuilder).execute();
        BookKeeperClientFactory bookKeeperClientFactory = mock(BookKeeperClientFactory.class);
        when(pulsar.getBookKeeperClientFactory()).thenReturn(bookKeeperClientFactory);
        doReturn(CompletableFuture.completedFuture(defaultBookKeeper))
                .when(bookKeeperClientFactory).create(any(), any(), any(), any(), any());
        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(pulsar);
        schemaStorage.start();

        CompletableFuture<LedgerHandle> createFuture = schemaStorage.createLedger(schemaId);

        assertThat(createFuture.join()).isSameAs(ledgerHandle);
        verify(pulsar, never()).getBookKeeperClientContext(any());
        ArgumentCaptor<Map<String, byte[]>> metadataCaptor = ArgumentCaptor.captor();
        verify(createBuilder).withCustomMetadata(metadataCaptor.capture());
        Map<String, byte[]> metadata = metadataCaptor.getValue();
        LedgerMetadataUtils.buildMetadataForSchema(schemaId)
                .forEach((key, value) -> assertThat(metadata.get(key)).containsExactly(value));
        assertThat(metadata).doesNotContainKey(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
    }

    @Test
    public void testCreateLedgerDoesNotFallbackWhenPlacementLookupFails() {
        String schemaId = "tenant/namespace/topic";
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
        RuntimeException failure = new RuntimeException("placement lookup failed");
        when(pulsar.getBookKeeperClientContext(TopicName.get(schemaId)))
                .thenReturn(CompletableFuture.failedFuture(failure));
        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(pulsar);

        CompletableFuture<LedgerHandle> createFuture = schemaStorage.createLedger(schemaId);

        assertThat(createFuture).isCompletedExceptionally();
        assertThatThrownBy(createFuture::join).hasCause(failure);
    }
}
