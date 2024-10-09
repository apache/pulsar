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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * Unit test {@link RedirectManager}.
 */
public class RedirectManagerTest {

    @Test
    public void testFindRedirectLookupResultAsync() throws ExecutionException, InterruptedException {
        PulsarService pulsar = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        when(pulsar.getConfiguration()).thenReturn(configuration);
        RedirectManager redirectManager = spy(new RedirectManager(pulsar, null));

        configuration.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        configuration.setLoadBalancerDebugModeEnabled(true);

        // Test 1: No load manager class name found.
        doReturn(CompletableFuture.completedFuture(
                new HashMap<>(){{
                    put("broker-1", getLookupData("broker-1", null, 10));
                    put("broker-2", getLookupData("broker-2", ModularLoadManagerImpl.class.getName(), 1));
                }}
        )).when(redirectManager).getAvailableBrokerLookupDataAsync();

        // Should redirect to broker-1, since broker-1 has the latest load manager, even though the class name is null.
        Optional<LookupResult> lookupResult = redirectManager.findRedirectLookupResultAsync().get();
        assertTrue(lookupResult.isPresent());
        assertTrue(lookupResult.get().getLookupData().getBrokerUrl().contains("broker-1"));

        // Test 2: Should redirect to broker-1, since the latest broker are using ExtensibleLoadManagerImpl
        doReturn(CompletableFuture.completedFuture(
                new HashMap<>(){{
                    put("broker-1", getLookupData("broker-1", ExtensibleLoadManagerImpl.class.getName(), 10));
                    put("broker-2", getLookupData("broker-2", ModularLoadManagerImpl.class.getName(), 1));
                }}
        )).when(redirectManager).getAvailableBrokerLookupDataAsync();

        lookupResult = redirectManager.findRedirectLookupResultAsync().get();
        assertTrue(lookupResult.isPresent());
        assertTrue(lookupResult.get().getLookupData().getBrokerUrl().contains("broker-1"));


        // Test 3: Should not redirect, since current broker are using ModularLoadManagerImpl
        doReturn(CompletableFuture.completedFuture(
                new HashMap<>(){{
                    put("broker-1", getLookupData("broker-1", ExtensibleLoadManagerImpl.class.getName(), 10));
                    put("broker-2", getLookupData("broker-2", ModularLoadManagerImpl.class.getName(), 100));
                }}
        )).when(redirectManager).getAvailableBrokerLookupDataAsync();

        lookupResult = redirectManager.findRedirectLookupResultAsync().get();
        assertFalse(lookupResult.isPresent());
    }


    public BrokerLookupData getLookupData(String broker, String loadManagerClassName, long startTimeStamp) {
        String webServiceUrl = "http://" + broker + ":8080";
        String webServiceUrlTls = "https://" + broker + ":8081";
        String pulsarServiceUrl = "pulsar://" + broker + ":6650";
        String pulsarServiceUrlTls = "pulsar+ssl://" + broker + ":6651";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        return new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols, true, true,
                loadManagerClassName, startTimeStamp, "3.0.0", Collections.emptyMap());
    }
}
