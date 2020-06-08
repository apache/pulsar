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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClientFactoryImpl implements ZooKeeperClientFactory {
    public static final Charset ENCODING_SCHEME = StandardCharsets.UTF_8;

    @Override
    public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType, int zkSessionTimeoutMillis) {
        // Create a normal ZK client
        boolean canBeReadOnly = sessionType == SessionType.AllowReadOnly;

        CompletableFuture<ZooKeeper> future = new CompletableFuture<>();
        try {
            CompletableFuture<Void> internalFuture = new CompletableFuture<>();

            ZooKeeper zk = new ZooKeeper(serverList, zkSessionTimeoutMillis, event -> {
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
            }, canBeReadOnly);

            internalFuture.thenRun(() -> {
                log.info("ZooKeeper session established: {}", zk);
                future.complete(zk);
            }).exceptionally((exception) -> {
                log.error("Failed to establish ZooKeeper session: {}", exception.getMessage());
                future.completeExceptionally(exception);
                return null;
            });

        } catch (IllegalArgumentException | IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperClientFactoryImpl.class);
}
