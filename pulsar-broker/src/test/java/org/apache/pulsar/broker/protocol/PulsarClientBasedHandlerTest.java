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
package org.apache.pulsar.broker.protocol;

import java.io.File;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class PulsarClientBasedHandlerTest {

    private final static String clusterName = "cluster";
    private final static int shutdownTimeoutMs = 100;
    private final int zkPort = PortManager.nextFreePort();
    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(2, zkPort, PortManager::nextFreePort);
    private File tempDirectory;
    private PulsarService pulsar;

    @BeforeClass
    public void setup() throws Exception {
        bk.start();
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + zkPort);

        tempDirectory = SimpleProtocolHandlerTestsBase.configureProtocolHandler(config,
                PulsarClientBasedHandler.class.getName(), true);

        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(shutdownTimeoutMs);

        pulsar = new PulsarService(config);
        pulsar.start();
    }

    @Test(timeOut = 30000)
    public void testStopBroker() throws PulsarServerException {
        final var beforeStop = System.currentTimeMillis();
        final var handler = (PulsarClientBasedHandler) pulsar.getProtocolHandlers()
                .protocol(PulsarClientBasedHandler.PROTOCOL);
        pulsar.close();
        final var elapsedMs = System.currentTimeMillis() - beforeStop;
        log.info("It spends {} ms to stop the broker ({} for protocol handler)", elapsedMs, handler.closeTimeMs);
        Assert.assertTrue(elapsedMs <
                + handler.closeTimeMs + shutdownTimeoutMs + 1000); // tolerate 1 more second for other processes
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        bk.stop();
        if (tempDirectory != null) {
            FileUtils.deleteDirectory(tempDirectory);
        }
    }
}
