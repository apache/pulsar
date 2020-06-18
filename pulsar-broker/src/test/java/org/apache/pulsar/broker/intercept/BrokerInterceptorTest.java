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
package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BrokerInterceptorTest extends ProducerConsumerBase {

    private static final String listenerName1 = "listener1";
    private BrokerInterceptor listener1;
    private NarClassLoader ncl1;
    private static final String listenerName2 = "listener2";
    private BrokerInterceptor listener2;
    private NarClassLoader ncl2;

    private Map<String, BrokerInterceptorWithClassLoader> listenerMap;
    private BrokerInterceptors listeners;

    @BeforeMethod
    public void setup() throws Exception {
        this.listener1 = mock(BrokerInterceptor.class);
        this.ncl1 = mock(NarClassLoader.class);
        this.listener2 = mock(BrokerInterceptor.class);
        this.ncl2 = mock(NarClassLoader.class);

        this.listenerMap = new HashMap<>();
        this.listenerMap.put(
                listenerName1,
                new BrokerInterceptorWithClassLoader(listener1, ncl1));
        this.listenerMap.put(
                listenerName2,
                new BrokerInterceptorWithClassLoader(listener2, ncl2));
        this.listeners = new BrokerInterceptors(this.listenerMap);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        teardown();
    }

    @AfterMethod
    public void teardown() throws Exception {
        this.listeners.close();

        verify(listener1, times(1)).close();
        verify(listener2, times(1)).close();
        verify(ncl1, times(1)).close();
        verify(ncl2, times(1)).close();
        super.internalCleanup();
    }

    @Test
    public void testInitialize() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        listeners.initialize(conf);
        verify(listener1, times(1)).initialize(same(conf));
        verify(listener2, times(1)).initialize(same(conf));
    }

    @Test
    public void testWebserviceRequest() throws PulsarAdminException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        admin.namespaces().createNamespace("public/test", 4);
        Assert.assertTrue(((CounterBrokerInterceptor)listener).getCount() >= 1);
    }

    @Test
    public void testPulsarCommand() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        pulsarClient.newProducer(Schema.BOOL).topic("test").create();
        // CONNECT and PRODUCER
        Assert.assertTrue(((CounterBrokerInterceptor)listener).getCount() >= 2);
    }
}
