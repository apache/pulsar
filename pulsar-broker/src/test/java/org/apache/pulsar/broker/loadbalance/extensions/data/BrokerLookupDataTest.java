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
package org.apache.pulsar.broker.loadbalance.extensions.data;

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerLookupDataTest {

    @Test
    public void testConstructors() throws PulsarServerException, URISyntaxException {
        String webServiceUrl = "http://localhost:8080";
        String webServiceUrlTls = "https://localhoss:8081";
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarServiceUrlTls = "pulsar+ssl://localhost:6651";
        final String listenerUrl = "pulsar://gateway:7000";
        final String listenerUrlTls = "pulsar://gateway:8000";
        final String listener = "internal";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>(){{
            put(listener, AdvertisedListener.builder()
                    .brokerServiceUrl(new URI(listenerUrl))
                    .brokerServiceUrlTls(new URI(listenerUrlTls))
                    .build());
        }};
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        BrokerLookupData lookupData = new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols, true, true,
                ExtensibleLoadManagerImpl.class.getName(), System.currentTimeMillis(),"3.0",
                Collections.emptyMap());
        assertEquals(webServiceUrl, lookupData.webServiceUrl());
        assertEquals(webServiceUrlTls, lookupData.webServiceUrlTls());
        assertEquals(pulsarServiceUrl, lookupData.pulsarServiceUrl());
        assertEquals(pulsarServiceUrlTls, lookupData.pulsarServiceUrlTls());
        assertEquals(Optional.of("9092"), lookupData.getProtocol("kafka"));
        assertEquals(Optional.empty(), lookupData.getProtocol("echo"));
        assertTrue(lookupData.persistentTopicsEnabled());
        assertTrue(lookupData.nonPersistentTopicsEnabled());
        assertEquals("3.0", lookupData.brokerVersion());


        LookupResult lookupResult = lookupData.toLookupResult(LookupOptions.builder().build());
        assertEquals(webServiceUrl, lookupResult.getLookupData().getHttpUrl());
        assertEquals(webServiceUrlTls, lookupResult.getLookupData().getHttpUrlTls());
        assertEquals(pulsarServiceUrl, lookupResult.getLookupData().getBrokerUrl());
        assertEquals(pulsarServiceUrlTls, lookupResult.getLookupData().getBrokerUrlTls());

        try {
            lookupData.toLookupResult(LookupOptions.builder().advertisedListenerName("others").build());
            fail();
        } catch (PulsarServerException ex) {
            assertTrue(ex.getMessage().contains("the broker do not have others listener"));
        }
        lookupResult = lookupData.toLookupResult(LookupOptions.builder().advertisedListenerName(listener).build());
        assertEquals(listenerUrl, lookupResult.getLookupData().getBrokerUrl());
        assertEquals(listenerUrlTls, lookupResult.getLookupData().getBrokerUrlTls());
        assertEquals(webServiceUrl, lookupResult.getLookupData().getHttpUrl());
        assertEquals(webServiceUrlTls, lookupResult.getLookupData().getHttpUrlTls());
    }
}
