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

import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.junit.Assert;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
                pulsarServiceUrlTls, advertisedListeners, protocols, true, true, "3.0");
        Assert.assertEquals(webServiceUrl, lookupData.webServiceUrl());
        Assert.assertEquals(webServiceUrlTls, lookupData.webServiceUrlTls());
        Assert.assertEquals(pulsarServiceUrl, lookupData.pulsarServiceUrl());
        Assert.assertEquals(pulsarServiceUrlTls, lookupData.pulsarServiceUrlTls());
        Assert.assertEquals(Optional.of("9092"), lookupData.getProtocol("kafka"));
        Assert.assertEquals(Optional.empty(), lookupData.getProtocol("echo"));
        Assert.assertTrue(lookupData.persistentTopicsEnabled());
        Assert.assertTrue(lookupData.nonPersistentTopicsEnabled());
        Assert.assertEquals("3.0", lookupData.brokerVersion());


        LookupResult lookupResult = lookupData.toLookupResult();
        Assert.assertEquals(webServiceUrl, lookupResult.getLookupData().getHttpUrl());
        Assert.assertEquals(webServiceUrlTls, lookupResult.getLookupData().getHttpUrlTls());
        Assert.assertEquals(pulsarServiceUrl, lookupResult.getLookupData().getBrokerUrl());
        Assert.assertEquals(pulsarServiceUrlTls, lookupResult.getLookupData().getBrokerUrlTls());
    }
}
