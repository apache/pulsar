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

import static org.apache.bookkeeper.util.SafeRunnable.safeRun;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperBkClientFactoryImpl implements ZooKeeperClientFactory {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperBkClientFactoryImpl.class);

    private final OrderedExecutor executor;

    public ZookeeperBkClientFactoryImpl(OrderedExecutor executor) {
        this.executor = executor;
    }

    @Override
    public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType, int zkSessionTimeoutMillis) {
        CompletableFuture<ZooKeeper> future = new CompletableFuture<>();

        executor.execute(safeRun(() -> {
            try {
                ZooKeeper zk = ZooKeeperClient.newBuilder().connectString(serverList)
                        .sessionTimeoutMs(zkSessionTimeoutMillis)
                        .connectRetryPolicy(new BoundExponentialBackoffRetryPolicy(zkSessionTimeoutMillis,
                                zkSessionTimeoutMillis, 0))
                        .build();

                if (zk.getState() == States.CONNECTEDREADONLY && sessionType != SessionType.AllowReadOnly) {
                    zk.close();
                    future.completeExceptionally(new IllegalStateException("Cannot use a read-only session"));
                }

                log.info("ZooKeeper session established: {}", zk);
                future.complete(zk);
            } catch (IOException | KeeperException | InterruptedException exception) {
                log.error("Failed to establish ZooKeeper session: {}", exception.getMessage());
                future.completeExceptionally(exception);
            }
        }, throwable -> {
            future.completeExceptionally(throwable);
        }));

        return future;
    }
}
