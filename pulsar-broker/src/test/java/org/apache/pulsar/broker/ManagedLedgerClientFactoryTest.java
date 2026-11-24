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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerClientFactoryTest {
    @DataProvider
    public Object[][] cacheExtensionSettings() {
        return new Object[][] {{null, 5}, {false, 2}, {true, 0}, {true, 5}};
    }

    @Test(dataProvider = "cacheExtensionSettings")
    public void testCacheExtensionSettingsAtStartup(Boolean extendRecentlyAccessed, int maxExtensions)
            throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        if (extendRecentlyAccessed == null) {
            assertThat(conf.isManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed()).isFalse();
        } else {
            conf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(extendRecentlyAccessed);
        }
        conf.setManagedLedgerCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(maxExtensions);
        conf.setBookkeeperClientExposeStatsToPrometheus(false);
        BookKeeperClientFactory bookkeeperProvider = mock(BookKeeperClientFactory.class);
        when(bookkeeperProvider.create(any(), any(), any(), any(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(BookKeeper.class)));
        try (ManagedLedgerClientFactory factory = spy(new ManagedLedgerClientFactory())) {
            doReturn(mock(ManagedLedgerFactoryImpl.class)).when(factory)
                    .createManagedLedgerFactory(any(), any(), any(), any(), any());
            factory.initialize(conf, mock(MetadataStoreExtended.class), bookkeeperProvider,
                    mock(EventLoopGroup.class), OpenTelemetry.noop());
            ArgumentCaptor<ManagedLedgerFactoryConfig> config =
                    ArgumentCaptor.forClass(ManagedLedgerFactoryConfig.class);
            verify(factory).createManagedLedgerFactory(any(), any(), any(), config.capture(), any());
            assertThat(config.getValue().isCacheEvictionExtendTTLOfRecentlyAccessed())
                    .isEqualTo(Boolean.TRUE.equals(extendRecentlyAccessed));
            assertThat(config.getValue().getCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes())
                    .isEqualTo(maxExtensions);
        }
    }
}
