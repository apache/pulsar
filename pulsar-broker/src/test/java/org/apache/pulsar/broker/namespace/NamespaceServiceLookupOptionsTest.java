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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.loadbalance.extensions.manager.RedirectManager;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceServiceLookupOptionsTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "differentLookupOptions")
    public Object[][] differentLookupOptions() {
        return new Object[][] {
            {
                LookupOptions.builder().advertisedListenerName("listener-a").build(),
                LookupOptions.builder().advertisedListenerName("listener-b").build()
            },
            {
                LookupOptions.builder().readOnly(true).build(),
                LookupOptions.builder().readOnly(false).build()
            },
            {
                LookupOptions.builder().loadTopicsInBundle(true).build(),
                LookupOptions.builder().loadTopicsInBundle(false).build()
            }
        };
    }

    @Test(dataProvider = "differentLookupOptions")
    public void testLookupRequestsWithDifferentOptionsAreNotCoalesced(
            LookupOptions firstOptions, LookupOptions secondOptions) throws Exception {
        TopicName topic = TopicName.get("persistent://public/default/lookup-options-" + UUID.randomUUID());
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(topic);
        NamespaceService namespaceService = pulsar.getNamespaceService();
        OwnershipCache ownershipCache = mock(OwnershipCache.class);
        CompletableFuture<Optional<NamespaceEphemeralData>> ownerFuture = new CompletableFuture<>();
        doReturn(CompletableFuture.completedFuture(bundle)).when(namespaceService).getBundleAsync(any());
        doReturn(ownershipCache).when(namespaceService).getOwnershipCache();
        when(ownershipCache.getOwnerAsync(bundle)).thenReturn(ownerFuture);
        stubRedirectManager(namespaceService);

        CompletableFuture<Optional<LookupResult>> firstLookup =
                namespaceService.getBrokerServiceUrlAsync(topic, firstOptions);
        CompletableFuture<Optional<LookupResult>> secondLookup =
                namespaceService.getBrokerServiceUrlAsync(topic, secondOptions);

        verify(ownershipCache, times(2)).getOwnerAsync(bundle);
        ownerFuture.complete(Optional.of(lookupOwnerWithAdvertisedListeners()));

        assertLookupResultUsesOptions(firstLookup.get(5, TimeUnit.SECONDS).orElseThrow(), firstOptions);
        assertLookupResultUsesOptions(secondLookup.get(5, TimeUnit.SECONDS).orElseThrow(), secondOptions);
    }

    @Test
    public void testEquivalentLookupRequestsAreCoalesced() throws Exception {
        TopicName topic = TopicName.get("persistent://public/default/lookup-options-" + UUID.randomUUID());
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(topic);
        NamespaceService namespaceService = pulsar.getNamespaceService();
        OwnershipCache ownershipCache = mock(OwnershipCache.class);
        CompletableFuture<Optional<NamespaceEphemeralData>> ownerFuture = new CompletableFuture<>();
        LookupOptions options = LookupOptions.builder().advertisedListenerName("listener-a").build();
        doReturn(CompletableFuture.completedFuture(bundle)).when(namespaceService).getBundleAsync(any());
        doReturn(ownershipCache).when(namespaceService).getOwnershipCache();
        when(ownershipCache.getOwnerAsync(bundle)).thenReturn(ownerFuture);
        stubRedirectManager(namespaceService);

        CompletableFuture<Optional<LookupResult>> firstLookup =
                namespaceService.getBrokerServiceUrlAsync(topic, options);
        CompletableFuture<Optional<LookupResult>> secondLookup = namespaceService.getBrokerServiceUrlAsync(topic,
                LookupOptions.builder().advertisedListenerName("listener-a").build());

        verify(ownershipCache).getOwnerAsync(bundle);
        ownerFuture.complete(Optional.of(lookupOwnerWithAdvertisedListeners()));

        assertLookupResultUsesOptions(firstLookup.get(5, TimeUnit.SECONDS).orElseThrow(), options);
        assertLookupResultUsesOptions(secondLookup.get(5, TimeUnit.SECONDS).orElseThrow(), options);
    }

    /**
     * Replaces the redirect manager with one that answers immediately, so that the lookup reaches the
     * request coalescing logic on the calling thread. Otherwise the redirect check performs a metadata
     * store read and the lookups would be handled asynchronously.
     */
    private static void stubRedirectManager(NamespaceService namespaceService) throws Exception {
        RedirectManager redirectManager = mock(RedirectManager.class);
        when(redirectManager.findRedirectLookupResultAsync())
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        FieldUtils.writeDeclaredField(namespaceService, "redirectManager", redirectManager, true);
    }

    private static NamespaceEphemeralData lookupOwnerWithAdvertisedListeners() {
        Map<String, AdvertisedListener> advertisedListeners = Map.of(
                "listener-a", AdvertisedListener.builder()
                        .brokerServiceUrl(URI.create("pulsar://listener-a:6650"))
                        .brokerHttpUrl(URI.create("http://listener-a:8080"))
                        .build(),
                "listener-b", AdvertisedListener.builder()
                        .brokerServiceUrl(URI.create("pulsar://listener-b:6650"))
                        .brokerHttpUrl(URI.create("http://listener-b:8080"))
                        .build());
        return new NamespaceEphemeralData("pulsar://broker-1:6650", null,
                "http://broker-1:8080", null, false, advertisedListeners);
    }

    private static void assertLookupResultUsesOptions(LookupResult lookupResult, LookupOptions options) {
        if (options.hasAdvertisedListenerName()) {
            assertEquals(lookupResult.getLookupData().getBrokerUrl(),
                    "pulsar://" + options.getAdvertisedListenerName() + ":6650");
        } else {
            assertEquals(lookupResult.getLookupData().getBrokerUrl(), "pulsar://broker-1:6650");
        }
    }
}
