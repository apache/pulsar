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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZookeeperBkClientFactoryImplTest {

    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;
    private final long ZOOKEEPER_SESSION_TIMEOUT_MILLIS = 1000;
    private OrderedScheduler executor;

    @BeforeMethod
    void setup() throws Exception {
        executor = OrderedScheduler.newSchedulerBuilder().build();
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        localZkS.close();
        executor.shutdownNow();
    }

    @Test
    public void testZKCreationRW() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(), SessionType.ReadWrite,
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(localZkc.getState().isConnected());
        assertNotEquals(localZkc.getState(), States.CONNECTEDREADONLY);
        localZkc.close();
    }

    @Test
    public void testZKCreationRO() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                SessionType.AllowReadOnly, (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(localZkc.getState().isConnected());
        localZkc.close();
    }

    @Test
    public void testZKCreationFailure() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("invalid", SessionType.ReadWrite,
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);

        try {
            zkFuture.get();
            fail("Creation should have failed");
        } catch (ExecutionException e) {
            // Expected
        }
    }
}
