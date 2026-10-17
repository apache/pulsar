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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.testng.annotations.Test;

public class MetadataStoreCacheLoaderTest {

    private MetadataStoreCacheLoader newCacheLoader(LoadManagerReportResources loadReportResources) throws Exception {
        MetadataStore store = mock(MetadataStore.class);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getLoadReportResources()).thenReturn(loadReportResources);
        when(loadReportResources.getStore()).thenReturn(store);
        return new MetadataStoreCacheLoader(pulsarResources, 5000);
    }

    @Test
    public void testGetAvailableBrokersServesCacheWithoutBlocking() throws Exception {
        LoadManagerReportResources loadReportResources = mock(LoadManagerReportResources.class);
        LoadManagerReport report = mock(LoadManagerReport.class);
        when(loadReportResources.getChildrenAsync(LOADBALANCE_BROKERS_ROOT))
                .thenReturn(CompletableFuture.completedFuture(List.of("broker-1")));
        when(loadReportResources.getAsync(LOADBALANCE_BROKERS_ROOT + "/broker-1"))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(report)));

        @Cleanup
        MetadataStoreCacheLoader loader = newCacheLoader(loadReportResources);

        assertEquals(loader.getAvailableBrokers(), List.of(report));
        // The blocking, synchronous getChildren(...) must never be used: it could stall a Netty IO thread.
        verify(loadReportResources, never()).getChildren(anyString());
    }

    @Test
    public void testGetAvailableBrokersDoesNotBlockOnEmptyCache() throws Exception {
        LoadManagerReportResources loadReportResources = mock(LoadManagerReportResources.class);
        when(loadReportResources.getChildrenAsync(LOADBALANCE_BROKERS_ROOT))
                .thenReturn(CompletableFuture.completedFuture(List.of()));

        @Cleanup
        MetadataStoreCacheLoader loader = newCacheLoader(loadReportResources);

        assertTrue(loader.getAvailableBrokers().isEmpty());
        verify(loadReportResources, never()).getChildren(anyString());
    }
}
