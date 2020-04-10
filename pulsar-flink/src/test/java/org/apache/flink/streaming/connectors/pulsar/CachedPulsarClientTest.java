/**
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
package org.apache.flink.streaming.connectors.pulsar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.concurrent.ConcurrentMap;

import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unit test of {@link CachedPulsarClient}.
 */
public class CachedPulsarClientTest {

    private static final String SERVICE_URL = "pulsar://localhost:6650";

    @BeforeTest
    public void clearCache() {
        CachedPulsarClient.clear();
    }

    @Test
    public void testShouldReturnSameInstanceWithSameParam() throws Exception {
        PulsarClientImpl impl1 = Mockito.mock(PulsarClientImpl.class);
        PulsarClientImpl impl2 = Mockito.mock(PulsarClientImpl.class);

        ClientConfigurationData conf1 = new ClientConfigurationData();
        conf1.setServiceUrl(SERVICE_URL);

        ClientConfigurationData conf2 = new ClientConfigurationData();
        conf2.setServiceUrl(SERVICE_URL);

        PowerMockito.whenNew(PulsarClientImpl.class)
            .withArguments(conf1).thenReturn(impl1);
        PowerMockito.whenNew(PulsarClientImpl.class)
            .withArguments(conf2).thenReturn(impl2);

        PulsarClientImpl client1 = CachedPulsarClient.getOrCreate(conf1);
        PulsarClientImpl client2 = CachedPulsarClient.getOrCreate(conf2);
        PulsarClientImpl client3 = CachedPulsarClient.getOrCreate(conf1);

        assertEquals(client1, client2);
        assertEquals(client1, client3);

        assertEquals(CachedPulsarClient.getAsMap().size(), 1);
    }

    @Test
    public void testShouldCloseTheCorrectClient() throws Exception {
        PulsarClientImpl impl1 = Mockito.mock(PulsarClientImpl.class);
        PulsarClientImpl impl2 = Mockito.mock(PulsarClientImpl.class);

        ClientConfigurationData conf1 = new ClientConfigurationData();
        conf1.setServiceUrl(SERVICE_URL);

        ClientConfigurationData conf2 = new ClientConfigurationData();
        conf2.setServiceUrl(SERVICE_URL);
        conf2.setNumIoThreads(5);

        PowerMockito.whenNew(PulsarClientImpl.class)
            .withArguments(conf1).thenReturn(impl1);
        PowerMockito.whenNew(PulsarClientImpl.class)
            .withArguments(conf2).thenReturn(impl2);

        PulsarClientImpl client1 = CachedPulsarClient.getOrCreate(conf1);
        PulsarClientImpl client2 = CachedPulsarClient.getOrCreate(conf2);

        assertNotEquals(client1, client2);

        ConcurrentMap<ClientConfigurationData, PulsarClientImpl> map1 = CachedPulsarClient.getAsMap();
        assertEquals(map1.size(), 2);

        CachedPulsarClient.close(conf2);

        ConcurrentMap<ClientConfigurationData, PulsarClientImpl> map2 = CachedPulsarClient.getAsMap();
        assertEquals(map2.size(), 1);

        assertEquals(map2.values().iterator().next(), client1);
    }

    @Test
    public void getClientFromCacheShouldAlwaysReturnAnOpenedInstance() throws Exception {
        PulsarClientImpl impl1 = Mockito.mock(PulsarClientImpl.class);

        ClientConfigurationData conf1 = new ClientConfigurationData();
        conf1.setServiceUrl(SERVICE_URL);

        PowerMockito.whenNew(PulsarClientImpl.class)
                .withArguments(conf1).thenReturn(impl1);

        PulsarClientImpl client1 = CachedPulsarClient.getOrCreate(conf1);

        ConcurrentMap<ClientConfigurationData, PulsarClientImpl> map1 = CachedPulsarClient.getAsMap();
        assertEquals(map1.size(), 1);

        client1.getState().set(PulsarClientImpl.State.Closed);

        PulsarClientImpl client2 = CachedPulsarClient.getOrCreate(conf1);

        assertNotEquals(client1, client2);

        ConcurrentMap<ClientConfigurationData, PulsarClientImpl> map2 = CachedPulsarClient.getAsMap();
        assertEquals(map2.size(), 1);
    }
}
