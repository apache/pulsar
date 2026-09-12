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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link NamespaceService#resolveBrokerServiceLookupResult}.
 */
@Test(groups = "broker")
public class NamespaceServiceListenerResolutionTest {

    @Test
    public void testFailsOnBrokerServiceListenerMismatch() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData("broker-1:8080",
                "pulsar://broker-1:6650", null, "http://broker-1:8080", null, false,
                new HashMap<>());
        LookupOptions options = LookupOptions.builder().advertisedListenerName("missing").build();
        CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();
        NamespaceService.resolveBrokerServiceLookupResult(options, nsData, future);
        assertTrue(future.isCompletedExceptionally());
        Throwable cause = expectThrows(Exception.class, future::get).getCause();
        assertTrue(cause instanceof PulsarServerException, "unexpected cause: " + cause);
        assertTrue(cause.getMessage().contains("'missing' listener"), "unexpected message: " + cause.getMessage());
    }

    @Test
    public void testFallsBackOnWebServiceListenerMismatch() throws Exception {
        // During rolling upgrades, the target broker may not publish the listener yet. The lookup
        // must not fail; instead, the redirect target falls back to the broker's default URLs.
        NamespaceEphemeralData nsData = new NamespaceEphemeralData("broker-1:8080",
                "pulsar://broker-1:6650", null, "http://broker-1:8080", null, false,
                new HashMap<>());
        LookupOptions options = LookupOptions.builder().webServiceAdvertisedListenerName("missing").build();
        CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();
        NamespaceService.resolveBrokerServiceLookupResult(options, nsData, future);
        LookupResult result = future.get().orElseThrow();
        assertNull(result.getWebServiceListenerName());
        assertEquals(result.getLookupData().getHttpUrl(), "http://broker-1:8080");
    }

    @Test
    public void testSucceedsWhenListenersMatch() throws Exception {
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        advertisedListeners.put("listener-a", AdvertisedListener.builder()
                .brokerServiceUrl(new URI("pulsar://gateway:6650"))
                .brokerHttpUrl(new URI("http://gateway:8080"))
                .build());
        NamespaceEphemeralData nsData = new NamespaceEphemeralData("broker-1:8080",
                "pulsar://broker-1:6650", null, "http://broker-1:8080", null, false,
                advertisedListeners);
        LookupOptions options = LookupOptions.builder()
                .advertisedListenerName("listener-a")
                .webServiceAdvertisedListenerName("listener-a")
                .build();
        CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();
        NamespaceService.resolveBrokerServiceLookupResult(options, nsData, future);
        LookupResult result = future.get().orElseThrow();
        assertEquals(result.getBrokerServiceListenerName(), "listener-a");
        assertEquals(result.getWebServiceListenerName(), "listener-a");
    }
}
