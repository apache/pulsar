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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterBadVersionException;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.testng.annotations.Test;

/**
 * Unit test for {@link BrokerVersionFilter}.
 */
@Test(groups = "broker")
public class BrokerVersionFilterTest extends BrokerFilterTestBase {


    @Test
    public void testFilterEmptyBrokerList() throws BrokerFilterException, ExecutionException, InterruptedException {
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        Map<String, BrokerLookupData> result = brokerVersionFilter.filterAsync(new HashMap<>(), null, getContext()).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDisabledFilter() throws BrokerFilterException, ExecutionException, InterruptedException {
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
        Map<String, BrokerLookupData> result = brokerVersionFilter.filterAsync(brokers, null, context).get();
        assertEquals(result, originalBrokers);
    }

    @Test
    public void testFilter() throws BrokerFilterException, ExecutionException, InterruptedException {
        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "localhost:6650", getLookupData("2.10.0"),
                "localhost:6651", getLookupData("2.10.1"),
                "localhost:6652", getLookupData("2.10.1"),
                "localhost:6653", getLookupData("2.10.1")
        );
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        Map<String, BrokerLookupData> result = brokerVersionFilter.filterAsync(
                new HashMap<>(originalBrokers), null, getContext()).get();
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
        result = brokerVersionFilter.filterAsync(new HashMap<>(originalBrokers), null, getContext()).get();

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

        result = brokerVersionFilter.filterAsync(new HashMap<>(originalBrokers), null, getContext()).get();
        assertEquals(result, Map.of(
                "localhost:6653", getLookupData("2.10.2-SNAPSHOT")
        ));

    }

    @Test
    public void testInvalidVersionString() {
        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "localhost:6650", getLookupData("xxx")
        );
        BrokerVersionFilter brokerVersionFilter = new BrokerVersionFilter();
        try {
            brokerVersionFilter.filterAsync(new HashMap<>(originalBrokers), null, getContext()).get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getClass(), BrokerFilterBadVersionException.class);
        }
    }
}
