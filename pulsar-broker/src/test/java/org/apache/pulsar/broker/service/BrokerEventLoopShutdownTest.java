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

import io.netty.buffer.ByteBuf;
import java.io.InputStream;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.Commands;
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
    public void testBrokerServiceCloseClosesConnections() throws Exception {
        final var config = newConfig();
        // PulsarService.close() closes the broker service a second time, which shuts its BookKeeper OrderedExecutor
        // down again: that executor then reports that it isn't terminated, and the graceful shutdown would wait for
        // its whole timeout
        config.setBrokerShutdownTimeoutMs(0);
        @Cleanup final var broker = new PulsarService(config);
        broker.start();

        @Cleanup final var socket = new Socket("localhost", broker.getBrokerListenPort().orElseThrow());
        socket.setSoTimeout(30_000);
        final ByteBuf connect = Commands.newConnect("none", "", null);
        try {
            final var bytes = new byte[connect.readableBytes()];
            connect.readBytes(bytes);
            socket.getOutputStream().write(bytes);
        } finally {
            connect.release();
        }
        final InputStream in = socket.getInputStream();
        // the broker answers the CONNECT command, so it has accepted the connection
        Assert.assertNotEquals(in.read(), -1);

        broker.getBrokerService().closeAsync().get();

        // The broker service closes its connections itself: the IO event loops, which belong to the PulsarService,
        // are still running. A connection left open would make this read time out.
        Assert.assertFalse(broker.getIoEventLoopGroup().isShuttingDown());
        while (in.read() != -1) {
            // skip the rest of the CONNECTED response
        }
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
