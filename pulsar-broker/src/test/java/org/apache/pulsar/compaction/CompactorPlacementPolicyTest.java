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
package org.apache.pulsar.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.BookKeeperClientContext;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CompactorPlacementPolicyTest {

    private static final String TOPIC = "persistent://tenant/namespace/topic";

    @DataProvider(name = "compactorTypes")
    public static Object[][] compactorTypes() {
        return new Object[][] {
                {"publishing"},
                {"event-time"},
                {"strategic"}
        };
    }

    @Test(dataProvider = "compactorTypes")
    public void testCreateLedgerUsesPlacementClientAndMatchingMetadata(String compactorType) throws Exception {
        BookKeeper defaultBookKeeper = mock(BookKeeper.class);
        BookKeeper placementBookKeeper = mock(BookKeeper.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class, RETURNS_SELF);
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        doReturn(createBuilder).when(placementBookKeeper).newCreateLedgerOp();
        doReturn(CompletableFuture.completedFuture(ledgerHandle)).when(createBuilder).execute();

        EnsemblePlacementPolicyConfig placementPolicy = placementPolicy();
        BookKeeperClientContext clientContext =
                BookKeeperClientContext.create(placementBookKeeper, placementPolicy);
        AtomicReference<TopicName> requestedTopic = new AtomicReference<>();
        Function<TopicName, CompletableFuture<BookKeeperClientContext>> provider = topicName -> {
            requestedTopic.set(topicName);
            return CompletableFuture.completedFuture(clientContext);
        };
        AbstractTwoPhaseCompactor<?> compactor = newCompactor(compactorType, defaultBookKeeper, provider);
        Map<String, byte[]> compactionMetadata = LedgerMetadataUtils.buildMetadataForCompactedLedger(
                TOPIC, new byte[] {1, 2, 3});

        LedgerHandle result = compactor.createLedger(defaultBookKeeper, compactionMetadata, TOPIC).join();

        assertThat(result).isSameAs(ledgerHandle);
        assertThat(requestedTopic.get()).isEqualTo(TopicName.get(TOPIC));
        verify(placementBookKeeper).newCreateLedgerOp();
        verify(defaultBookKeeper, never()).newCreateLedgerOp();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, byte[]>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(createBuilder).withCustomMetadata(metadataCaptor.capture());
        Map<String, byte[]> actualMetadata = metadataCaptor.getValue();
        compactionMetadata.forEach((key, value) -> assertThat(actualMetadata.get(key)).containsExactly(value));
        assertThat(EnsemblePlacementPolicyConfig.decode(actualMetadata.get(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG)))
                .isEqualTo(placementPolicy);
    }

    @Test
    public void testFailedProviderCompletesCreateFutureExceptionally() {
        RuntimeException failure = new RuntimeException("placement lookup failed");
        assertProviderFailureIsAsynchronous(topicName -> CompletableFuture.failedFuture(failure), failure);
    }

    @Test
    public void testThrowingProviderCompletesCreateFutureExceptionally() {
        RuntimeException failure = new RuntimeException("provider threw");
        assertProviderFailureIsAsynchronous(topicName -> {
            throw failure;
        }, failure);
    }

    @DataProvider(name = "brokerFactoryTypes")
    public static Object[][] brokerFactoryTypes() {
        return new Object[][] {
                {false, PublishingOrderCompactor.class},
                {true, EventTimeOrderCompactor.class}
        };
    }

    @Test(dataProvider = "brokerFactoryTypes")
    public void testBrokerFactoryWiresTopicClientProvider(
            boolean eventTime, Class<? extends Compactor> expectedType) throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        BookKeeper defaultBookKeeper = mock(BookKeeper.class);
        PulsarClient pulsarClient = mock(PulsarClient.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        BookKeeperClientContext expectedContext = BookKeeperClientContext.create(
                mock(BookKeeper.class), placementPolicy());
        TopicName topicName = TopicName.get(TOPIC);
        when(pulsarService.getConfiguration()).thenReturn(configuration);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        when(pulsarService.getBookKeeperClient()).thenReturn(defaultBookKeeper);
        when(pulsarService.getCompactorExecutor()).thenReturn(scheduler);
        when(pulsarService.getBookKeeperClientContext(topicName))
                .thenReturn(CompletableFuture.completedFuture(expectedContext));
        PulsarCompactionServiceFactory factory = eventTime
                ? new EventTimeCompactionServiceFactory() : new PulsarCompactionServiceFactory();
        factory.initialize(pulsarService).join();

        Compactor compactor = factory.getCompactor();

        assertThat(compactor).isInstanceOf(expectedType);
        assertThat(compactor.bookKeeperClientContextProvider.apply(topicName).join()).isSameAs(expectedContext);
        verify(pulsarService).getBookKeeperClientContext(topicName);
    }

    @Test
    public void testPulsarServiceWiresStrategicCompactorTopicClientProvider() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        TopicName topicName = TopicName.get(TOPIC);
        BookKeeperClientContext expectedContext = BookKeeperClientContext.create(
                mock(BookKeeper.class), placementPolicy());
        when(pulsarService.getConfiguration()).thenReturn(new ServiceConfiguration());
        when(pulsarService.getClient()).thenReturn(mock(PulsarClient.class));
        when(pulsarService.getBookKeeperClient()).thenReturn(mock(BookKeeper.class));
        when(pulsarService.getCompactorExecutor()).thenReturn(mock(ScheduledExecutorService.class));
        when(pulsarService.getBookKeeperClientContext(topicName))
                .thenReturn(CompletableFuture.completedFuture(expectedContext));
        doCallRealMethod().when(pulsarService).newStrategicCompactor();

        StrategicTwoPhaseCompactor compactor = pulsarService.newStrategicCompactor();

        assertThat(compactor.bookKeeperClientContextProvider.apply(topicName).join()).isSameAs(expectedContext);
        verify(pulsarService).getBookKeeperClientContext(topicName);
    }

    private static void assertProviderFailureIsAsynchronous(
            Function<TopicName, CompletableFuture<BookKeeperClientContext>> provider,
            RuntimeException failure) {
        BookKeeper defaultBookKeeper = mock(BookKeeper.class);
        AbstractTwoPhaseCompactor<?> compactor = newCompactor("publishing", defaultBookKeeper, provider);
        AtomicReference<CompletableFuture<LedgerHandle>> result = new AtomicReference<>();

        assertThatCode(() -> result.set(compactor.createLedger(defaultBookKeeper, Map.of(), TOPIC)))
                .doesNotThrowAnyException();

        assertThat(result.get()).isCompletedExceptionally();
        assertThatThrownBy(result.get()::join).hasCause(failure);
        verify(defaultBookKeeper, never()).newCreateLedgerOp();
    }

    private static AbstractTwoPhaseCompactor<?> newCompactor(
            String compactorType,
            BookKeeper defaultBookKeeper,
            Function<TopicName, CompletableFuture<BookKeeperClientContext>> provider) {
        ServiceConfiguration configuration = new ServiceConfiguration();
        PulsarClient pulsarClient = mock(PulsarClient.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        return switch (compactorType) {
            case "publishing" -> new PublishingOrderCompactor(
                    configuration, pulsarClient, defaultBookKeeper, scheduler, provider);
            case "event-time" -> new EventTimeOrderCompactor(
                    configuration, pulsarClient, defaultBookKeeper, scheduler, provider);
            case "strategic" -> new StrategicTwoPhaseCompactor(
                    configuration, pulsarClient, defaultBookKeeper, scheduler, provider);
            default -> throw new IllegalArgumentException("Unknown compactor type: " + compactorType);
        };
    }

    private static EnsemblePlacementPolicyConfig placementPolicy() {
        return new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class,
                Map.of(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary",
                        IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "secondary"));
    }
}
