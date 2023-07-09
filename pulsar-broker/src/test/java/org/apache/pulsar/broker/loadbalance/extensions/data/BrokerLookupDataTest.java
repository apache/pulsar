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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerLookupDataTest {

    @Test
    public void testConstructors() {
        String webServiceUrl = "http://localhost:8080";
        String webServiceUrlTls = "https://localhoss:8081";
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarServiceUrlTls = "pulsar+ssl://localhost:6651";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        BrokerLookupData lookupData = new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols, true, true,
                ExtensibleLoadManagerImpl.class.getName(), System.currentTimeMillis(),"3.0");
        assertEquals(webServiceUrl, lookupData.webServiceUrl());
        assertEquals(webServiceUrlTls, lookupData.webServiceUrlTls());
        assertEquals(pulsarServiceUrl, lookupData.pulsarServiceUrl());
        assertEquals(pulsarServiceUrlTls, lookupData.pulsarServiceUrlTls());
        assertEquals(Optional.of("9092"), lookupData.getProtocol("kafka"));
        assertEquals(Optional.empty(), lookupData.getProtocol("echo"));
        assertTrue(lookupData.persistentTopicsEnabled());
        assertTrue(lookupData.nonPersistentTopicsEnabled());
        assertEquals("3.0", lookupData.brokerVersion());


        LookupResult lookupResult = lookupData.toLookupResult();
        assertEquals(webServiceUrl, lookupResult.getLookupData().getHttpUrl());
        assertEquals(webServiceUrlTls, lookupResult.getLookupData().getHttpUrlTls());
        assertEquals(pulsarServiceUrl, lookupResult.getLookupData().getBrokerUrl());
        assertEquals(pulsarServiceUrlTls, lookupResult.getLookupData().getBrokerUrlTls());
    }
}
