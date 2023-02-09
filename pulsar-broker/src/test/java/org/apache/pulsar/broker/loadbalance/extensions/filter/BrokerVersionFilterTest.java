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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterBadVersionException;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

/**
 * Unit test for {@link BrokerVersionFilter}.
 */
@Test(groups = "broker")
public class BrokerVersionFilterTest {


    @Test
    public void testFilterEmptyBrokerList() throws BrokerFilterException {
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        Map<String, BrokerLookupData> result = brokerVersionFilter.filter(new HashMap<>(), getContext());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDisabledFilter() throws BrokerFilterException {
        LoadManagerContext context = getContext();
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setPreferLaterVersions(false);
        doReturn(configuration).when(context).brokerConfiguration();

        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "localhost:6650", getLookupData("2.10.0"),
                "localhost:6651", getLookupData("2.10.1")
        );
        Map<String, BrokerLookupData> brokers = new HashMap<>(originalBrokers);
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        Map<String, BrokerLookupData> result = brokerVersionFilter.filter(brokers, context);
        assertEquals(result, originalBrokers);
    }

    @Test
    public void testFilter() throws BrokerFilterException {
        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "localhost:6650", getLookupData("2.10.0"),
                "localhost:6651", getLookupData("2.10.1"),
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.1")
        );
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        Map<String, BrokerLookupData> result = brokerVersionFilter.filter(new HashMap<>(originalBrokers), getContext());
        assertEquals(result, Map.of(
                "localhost:6651", getLookupData("2.10.1"),
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.1")
        ));

        originalBrokers = Map.of(
                "localhost:6650", getLookupData("2.10.0"),
                "localhost:6651", getLookupData("2.10.1-SNAPSHOT"),
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.1")
        );
        result = brokerVersionFilter.filter(new HashMap<>(originalBrokers), getContext());

        assertEquals(result, Map.of(
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.1")
        ));

        originalBrokers = Map.of(
                "localhost:6650", getLookupData("2.10.0"),
                "localhost:6651", getLookupData("2.10.1-SNAPSHOT"),
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.2-SNAPSHOT")
        );

        result = brokerVersionFilter.filter(new HashMap<>(originalBrokers), getContext());
        assertEquals(result, Map.of(
                "localhost:6653", getLookupData("2.10.2-SNAPSHOT")
        ));

    }

    @Test(expectedExceptions = BrokerFilterBadVersionException.class)
    public void testInvalidVersionString() throws BrokerFilterException {
        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "localhost:6650", getLookupData("xxx")
        );
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        brokerVersionFilter.filter(new HashMap<>(originalBrokers), getContext());
    }

    public LoadManagerContext getContext() {
        LoadManagerContext mockContext = mock(LoadManagerContext.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setPreferLaterVersions(true);
        doReturn(configuration).when(mockContext).brokerConfiguration();
        return mockContext;
    }

    public BrokerLookupData getLookupData(String version) {
        String webServiceUrl = "http://localhost:8080";
        String webServiceUrlTls = "https://localhoss:8081";
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarServiceUrlTls = "pulsar+ssl://localhost:6651";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        return new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols, true, true, version);
    }
}
