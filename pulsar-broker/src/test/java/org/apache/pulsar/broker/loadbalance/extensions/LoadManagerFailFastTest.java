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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LoadManagerFailFastTest {

    private static final String cluster = "test";
    private final int zkPort = PortManager.nextLockedFreePort();
    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(2, zkPort, PortManager::nextLockedFreePort);
    private final ServiceConfiguration config = new ServiceConfiguration();

    @BeforeClass
    protected void setup() throws Exception {
        bk.start();
        config.setClusterName(cluster);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:localhost:" + zkPort);
    }

    @AfterClass
    protected void cleanup() throws Exception {
        bk.stop();
    }

    @Test(timeOut = 30000)
    public void testBrokerRegistryFailure() throws Exception {
        config.setLoadManagerClassName(BrokerRegistryLoadManager.class.getName());
        @Cleanup final var pulsar = new PulsarService(config);
        try {
            pulsar.start();
            Assert.fail();
        } catch (PulsarServerException e) {
            Assert.assertNull(e.getCause());
            Assert.assertEquals(e.getMessage(), "Cannot start BrokerRegistry");
        }
        Assert.assertTrue(pulsar.getLocalMetadataStore().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).get()
                .isEmpty());
    }

    @Test(timeOut = 30000)
    public void testServiceUnitStateChannelFailure() throws Exception {
        config.setLoadManagerClassName(ChannelLoadManager.class.getName());
        @Cleanup final var pulsar = new PulsarService(config);
        try {
            pulsar.start();
            Assert.fail();
        } catch (PulsarServerException e) {
            Assert.assertNull(e.getCause());
            Assert.assertEquals(e.getMessage(), "Cannot start ServiceUnitStateChannel");
        }
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(pulsar.getLocalMetadataStore()
                .getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).get().isEmpty()));
    }

    private static class BrokerRegistryLoadManager extends ExtensibleLoadManagerImpl {

        @Override
        protected BrokerRegistry createBrokerRegistry(PulsarService pulsar) {
            final var mockBrokerRegistry = Mockito.mock(BrokerRegistryImpl.class);
            try {
                Mockito.doThrow(new PulsarServerException("Cannot start BrokerRegistry")).when(mockBrokerRegistry)
                        .start();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
            return mockBrokerRegistry;
        }
    }

    private static class ChannelLoadManager extends ExtensibleLoadManagerImpl {

        @Override
        protected ServiceUnitStateChannel createServiceUnitStateChannel(PulsarService pulsar) {
            final var channel = Mockito.mock(ServiceUnitStateChannelImpl.class);
            try {
                Mockito.doThrow(new PulsarServerException("Cannot start ServiceUnitStateChannel")).when(channel)
                        .start();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
            Mockito.doAnswer(__ -> null).when(channel).listen(Mockito.any());
            return channel;
        }
    }
}
