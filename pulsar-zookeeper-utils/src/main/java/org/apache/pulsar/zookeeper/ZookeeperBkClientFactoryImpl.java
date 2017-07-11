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

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperBkClientFactoryImpl implements ZooKeeperClientFactory {

    @Override
    public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType, int zkSessionTimeoutMillis) {
        boolean canBeReadOnly = sessionType == SessionType.AllowReadOnly;

        CompletableFuture<ZooKeeper> future = new CompletableFuture<>();
        try {
            CompletableFuture<Void> internalFuture = new CompletableFuture<>();

            try {
                Constructor<ZooKeeperClient> cn = (Constructor<ZooKeeperClient>) ZooKeeperClient.class
                        .getDeclaredConstructor(String.class, int.class, ZooKeeperWatcherBase.class, RetryPolicy.class);
                cn.setAccessible(true);
                ZooKeeperWatcherBase zkWatch = new ZooKeeperWatcherBase(zkSessionTimeoutMillis);
                zkWatch.addChildWatcher((event) -> {

                    if (event.getType() == Event.EventType.None) {
                        switch (event.getState()) {

                        case ConnectedReadOnly:
                            checkArgument(canBeReadOnly);
                            // Fall through
                        case SyncConnected:
                            // ZK session is ready to use
                            internalFuture.complete(null);
                            break;

                        case Expired:
                            internalFuture
                                    .completeExceptionally(KeeperException.create(KeeperException.Code.SESSIONEXPIRED));
                            break;

                        default:
                            log.warn("Unexpected ZK event received: {}", event);
                            break;
                        }
                    }
                });
                final ZooKeeperClient zk = cn.newInstance(serverList, zkSessionTimeoutMillis, zkWatch,
                        new BoundExponentialBackoffRetryPolicy(zkSessionTimeoutMillis, zkSessionTimeoutMillis, 0));

                internalFuture.thenRun(() -> {
                    log.info("ZooKeeper session established: {}", zk);
                    // Sometimes: ZooKeeperWatcherBase triggers childwatch before setting zk.getState() = CONNECTED and
                    // state is still in CONNECTING state which takes few more msec to update state CONNECTED
                    completeFutureOnConnect(future, zk, System.currentTimeMillis() + zkSessionTimeoutMillis, Executors.newSingleThreadScheduledExecutor());
                }).exceptionally((exception) -> {
                    log.error("Failed to establish ZooKeeper session: {}", exception.getMessage());
                    future.completeExceptionally(exception);
                    return null;
                });

                internalFuture.complete(null);
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }

        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    private void completeFutureOnConnect(CompletableFuture<ZooKeeper> future, ZooKeeperClient zk, long timeOutTime,
            ScheduledExecutorService executor) {
        if (zk.getState().isConnected()) {
            future.complete(zk);
            executor.shutdown();
        } else {
            if (System.currentTimeMillis() - timeOutTime < 0) {
                executor.schedule(() -> completeFutureOnConnect(future, zk, timeOutTime, executor), 100,
                        TimeUnit.MILLISECONDS);
            } else {
                future.completeExceptionally(
                        new IllegalStateException("zookeeper couldn't connect with in given zkSessionTimeout"));
                executor.shutdown();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperBkClientFactoryImpl.class);
}
