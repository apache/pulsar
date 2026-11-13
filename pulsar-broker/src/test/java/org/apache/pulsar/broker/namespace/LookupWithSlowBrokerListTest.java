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
package org.apache.pulsar.broker.namespace;

import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A slow read of the available brokers must not block the broker's executor while lookups select a broker for
 * unowned bundles.
 */
@Test(groups = "broker")
public class LookupWithSlowBrokerListTest extends BrokerTestBase {

    private static final long BROKER_LIST_READ_DELAY_MS = 5000;
    private static final int MAX_DELAYED_BROKER_LIST_READS = 64;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setNumExecutorThreadPoolSize(2);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60000)
    public void testSlowBrokerListReadDoesNotBlockExecutor() throws Exception {
        String namespace = "prop/ns-slow-broker-list";
        admin.namespaces().createNamespace(namespace, 16);

        AtomicBoolean slowBrokerListReads = new AtomicBoolean(true);
        CompletableFuture<Void> slowReadObserved = new CompletableFuture<>();
        try {
            // Delay every broker-list read while enabled instead of relying on how many reads selection makes.
            for (int i = 0; i < MAX_DELAYED_BROKER_LIST_READS; i++) {
                mockZooKeeper.delayMulti(BROKER_LIST_READ_DELAY_MS, (op, path) -> {
                    if (slowBrokerListReads.get() && op == MockZooKeeper.Op.GET_CHILDREN
                            && path.equals(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
                        slowReadObserved.complete(null);
                        return true;
                    }
                    return false;
                });
            }

            // A broker joining and leaving invalidates the cached broker list, so the next lookup reads it again.
            String otherBroker = LoadManager.LOADBALANCE_BROKERS_ROOT + "/other-broker:8080";
            CompletableFuture<Void> brokerDeleted = new CompletableFuture<>();
            Runnable unregisterListener = pulsar.getLocalMetadataStore().registerCancellableListener(notification -> {
                if (notification.getType() == NotificationType.Deleted && notification.getPath().equals(otherBroker)) {
                    brokerDeleted.complete(null);
                }
            });
            try {
                mockZooKeeper.create(otherBroker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                mockZooKeeper.delete(otherBroker, -1);
                brokerDeleted.get(10, TimeUnit.SECONDS);
            } finally {
                unregisterListener.run();
            }

            LookupOptions options = LookupOptions.builder().authoritative(false).loadTopicsInBundle(false).build();
            List<CompletableFuture<Optional<LookupResult>>> lookups = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                lookups.add(pulsar.getNamespaceService()
                        .getBrokerServiceUrlAsync(TopicName.get("persistent://" + namespace + "/t-" + i), options));
            }
            Thread.sleep(1000);

            assertTrue(slowReadObserved.isDone(), "no broker-list read was delayed");
            assertTrue(lookups.stream().anyMatch(f -> !f.isDone()),
                    "the lookups finished before the probe, so the slow read wasn't exercised");

            long start = System.nanoTime();
            pulsar.getExecutor().submit(() -> { }).get(30, TimeUnit.SECONDS);
            long executorDelayMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(executorDelayMs < 1000, "the executor was blocked for " + executorDelayMs + " ms");
            slowBrokerListReads.set(false);

            for (CompletableFuture<Optional<LookupResult>> lookup : lookups) {
                assertTrue(lookup.get(30, TimeUnit.SECONDS).isPresent());
            }
        } finally {
            slowBrokerListReads.set(false);
        }
    }
}
