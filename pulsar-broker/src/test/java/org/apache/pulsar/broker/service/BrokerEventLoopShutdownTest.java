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
package org.apache.pulsar.broker.service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class BrokerEventLoopShutdownTest {

    private LocalBookkeeperEnsemble bk;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        bk = new LocalBookkeeperEnsemble(2, 0);
        bk.start();
    }

    @AfterClass(alwaysRun = true, timeOut = 30000)
    public void cleanup() throws Exception {
        bk.stop();
    }

    private ServiceConfiguration newConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        return config;
    }

    @Test(timeOut = 60000)
    public void testCloseOneBroker() throws Exception {
        @Cleanup final var broker0 = new PulsarService(newConfig());
        @Cleanup final var broker1 = new PulsarService(newConfig());
        broker0.start();
        broker1.start();

        final var startNs = System.nanoTime();
        broker0.close();
        final var closeTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        Assert.assertTrue(closeTimeMs < 1000, "close time: " + closeTimeMs + " ms");
    }

    @Test(timeOut = 60000)
    public void testCloseBrokerThatNeverStarted() throws Exception {
        final var broker = new PulsarService(newConfig());

        final var startNs = System.nanoTime();
        broker.close();
        final var closeTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        // Nothing is left on the IO event loops, so the close doesn't wait for Netty's default 2 s quiet period
        Assert.assertTrue(closeTimeMs < 1000, "close time: " + closeTimeMs + " ms");
        Assert.assertTrue(broker.getIoEventLoopGroup().isTerminated());
    }
}
