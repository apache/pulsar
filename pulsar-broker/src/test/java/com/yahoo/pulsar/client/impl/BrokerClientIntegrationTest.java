/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.namespace.NamespaceOwnershipCache;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConsumerBase;
import com.yahoo.pulsar.client.impl.HandlerBase.State;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.util.collections.ConcurrentLongHashMap;

public class BrokerClientIntegrationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(BrokerClientIntegrationTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    
    /**
     * Verifies unload namespace-bundle doesn't close shared connection used by other namespace-bundle.
     * 
     * 1. after disabling broker fron loadbalancer
     * 2. unload namespace-bundle "my-ns1" which disconnects client (producer/consumer) connected on that namespacebundle
     * 3. but doesn't close the connection for namesapce-bundle "my-ns2" and clients are still connected
     * 4. verifies unloaded "my-ns1" should not connected again with the broker as broker is disabled
     * 5. unload "my-ns2" which closes the connection as broker doesn't have any more client connected on that connection
     * 6. all namespace-bundles are in "connecting" state and waiting for available broker
     * 
     * 
     * @throws Exception
     */
    @Test
    public void testDisconnectClientWithoutClosingConnection() throws Exception {

        final String ns1 = "my-property/use/con-ns1";
        final String ns2 = "my-property/use/con-ns2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);

        final String dn1 = "persistent://" + ns1 + "/my-topic";
        final String dn2 = "persistent://" + ns2 + "/my-topic";
        ConsumerImpl consumer1 = spy(
                (ConsumerImpl) pulsarClient.subscribe(dn1, "my-subscriber-name", new ConsumerConfiguration()));
        ProducerImpl producer1 = spy((ProducerImpl) pulsarClient.createProducer(dn1, new ProducerConfiguration()));
        ProducerImpl producer2 = spy((ProducerImpl) pulsarClient.createProducer(dn2, new ProducerConfiguration()));

        ClientCnx clientCnx = producer1.clientCnx.get();

        Field pfield = ClientCnx.class.getDeclaredField("producers");
        pfield.setAccessible(true);
        Field cfield = ClientCnx.class.getDeclaredField("consumers");
        cfield.setAccessible(true);

        ConcurrentLongHashMap<ProducerImpl> producers = (ConcurrentLongHashMap<ProducerImpl>) pfield.get(clientCnx);
        ConcurrentLongHashMap<ConsumerImpl> consumers = (ConcurrentLongHashMap<ConsumerImpl>) cfield.get(clientCnx);

        producers.put(0, producer1);
        producers.put(1, producer2);
        consumers.put(0, consumer1);

        // disable this broker to avoid any new requests
        pulsar.getLoadManager().disableBroker();

        NamespaceBundle bundle1 = pulsar.getNamespaceService().getBundle(DestinationName.get(dn1));
        NamespaceBundle bundle2 = pulsar.getNamespaceService().getBundle(DestinationName.get(dn2));

        // unload ns-bundle:1
        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) bundle1);
        // let server send signal to close-connection and client close the connection
        Thread.sleep(1000);
        // [1] Verify: producer1 must get connectionClosed signal
        verify(producer1, atLeastOnce()).connectionClosed(anyObject());
        // [2] Verify: consumer1 must get connectionClosed signal
        verify(consumer1, atLeastOnce()).connectionClosed(anyObject());
        // [3] Verify: producer2 should have not received connectionClosed signal
        verify(producer2, never()).connectionClosed(anyObject());

        // sleep for sometime to let other disconnected producer and consumer connect again: but they should not get
        // connected with same broker as that broker is already out from active-broker list
        Thread.sleep(200);

        // producer1 must not be able to connect again
        assertTrue(producer1.clientCnx.get() == null);
        assertTrue(producer1.state.get().equals(State.Connecting));
        // consumer1 must not be able to connect again
        assertTrue(consumer1.clientCnx.get() == null);
        assertTrue(consumer1.state.get().equals(State.Connecting));
        // producer2 must have live connection
        assertTrue(producer2.clientCnx.get() != null);
        assertTrue(producer2.state.get().equals(State.Ready));

        
        // unload ns-bundle2 as well
        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) bundle2);
        verify(producer2, atLeastOnce()).connectionClosed(anyObject());

        Thread.sleep(200);

        // producer1 must not be able to connect again
        assertTrue(producer1.clientCnx.get() == null);
        assertTrue(producer1.state.get().equals(State.Connecting));
        // consumer1 must not be able to connect again
        assertTrue(consumer1.clientCnx.get() == null);
        assertTrue(consumer1.state.get().equals(State.Connecting));
        // producer2 must not be able to connect again
        assertTrue(producer2.clientCnx.get() == null);
        assertTrue(producer2.state.get().equals(State.Connecting));

        producer1.close();
        producer2.close();
        consumer1.close();

    }

    
    /**
     * Verifies: 1. Closing of Broker service unloads all bundle gracefully and there must not be any connected bundles
     * after closing broker service 
     * 
     * @throws Exception
     */
    @Test
    public void testCloseBrokerService() throws Exception {

        final String ns1 = "my-property/use/brok-ns1";
        final String ns2 = "my-property/use/brok-ns2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);

        final String dn1 = "persistent://" + ns1 + "/my-topic";
        final String dn2 = "persistent://" + ns2 + "/my-topic";
        
        ConsumerImpl consumer1 = spy(
                (ConsumerImpl) pulsarClient.subscribe(dn1, "my-subscriber-name", new ConsumerConfiguration()));
        ProducerImpl producer1 = spy((ProducerImpl) pulsarClient.createProducer(dn1, new ProducerConfiguration()));
        ProducerImpl producer2 = spy((ProducerImpl) pulsarClient.createProducer(dn2, new ProducerConfiguration()));

        //unload all other namespace
        pulsar.getBrokerService().close();

        // [1] OwnershipCache should not contain any more namespaces
        NamespaceOwnershipCache ownershipCache = pulsar.getNamespaceService().getOwnershipCache();
        assertTrue(ownershipCache.getOwnedBundles().keySet().isEmpty());
        
        // [2] All clients must be disconnected and in connecting state
        // producer1 must not be able to connect again
        assertTrue(producer1.clientCnx.get() == null);
        assertTrue(producer1.state.get().equals(State.Connecting));
        // consumer1 must not be able to connect again
        assertTrue(consumer1.clientCnx.get() == null);
        assertTrue(consumer1.state.get().equals(State.Connecting));
        // producer2 must not be able to connect again
        assertTrue(producer2.clientCnx.get() == null);
        assertTrue(producer2.state.get().equals(State.Connecting));
        
        producer1.close();
        producer2.close();
        consumer1.close();
        
    }

    
}
