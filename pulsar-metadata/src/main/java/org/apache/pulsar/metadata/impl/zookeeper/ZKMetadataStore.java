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
package org.apache.pulsar.metadata.impl.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.DefaultThreadFactory;

public class ZKMetadataStore implements MetadataStore {

    private final boolean isZkManaged;
    private final ZooKeeper zkc;
    private final ExecutorService executor;

    public ZKMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) throws IOException {
        try {
            isZkManaged = true;
            zkc = ZooKeeperClient.newBuilder().connectString(metadataURL)
                    .connectRetryPolicy(new BoundExponentialBackoffRetryPolicy(100, 60_000, Integer.MAX_VALUE))
                    .allowReadOnlyMode(metadataStoreConfig.isAllowReadOnlyOperations())
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis()).build();
        } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
        }

        this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("zk-metadata-store-callback"));
    }

    @VisibleForTesting
    public ZKMetadataStore(ZooKeeper zkc) {
        this.isZkManaged = false;
        this.zkc = zkc;
        this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("zk-metadata-store-callback"));
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        CompletableFuture<Optional<GetResult>> future = new CompletableFuture<>();

        try {
            zkc.getData(path, null, (rc, path1, ctx, data, stat) -> {
                executor.execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(Optional.of(new GetResult(data, getStat(stat))));
                    } else if (code == Code.NONODE) {
                        future.complete(Optional.empty());
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                });
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }

        return future;
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();

        try {
            zkc.getChildren(path, null, (rc, path1, ctx, children) -> {
                executor.execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        Collections.sort(children);
                        future.complete(children);
                    } else if (code == Code.NONODE) {
                        // The node we want may not exist yet, so put a watcher on its existence
                        // before throwing up the exception. Its possible that the node could have
                        // been created after the call to getChildren, but before the call to exists().
                        // If this is the case, exists will return true, and we just call getChildren
                        // again.
                        exists(path).thenAccept(exists -> {
                            if (exists) {
                                getChildren(path).thenAccept(c -> future.complete(c)).exceptionally(ex -> {
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                            } else {
                                // Z-node does not exist
                                future.complete(Collections.emptyList());
                            }
                        }).exceptionally(ex -> {
                            future.completeExceptionally(ex);
                            return null;
                        });

                        future.complete(Collections.emptyList());
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                });
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        try {
            zkc.exists(path, null, (rc, path1, ctx, stat) -> {
                executor.execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(true);
                    } else if (code == Code.NONODE) {
                        future.complete(false);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                });
            }, future);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }

        return future;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> optExpectedVersion) {
        boolean hasVersion = optExpectedVersion.isPresent();
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        CompletableFuture<Stat> future = new CompletableFuture<>();

        try {
            if (hasVersion && expectedVersion == -1) {
                ZkUtils.asyncCreateFullPathOptimistic(zkc, path, value, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, (rc, path1, ctx, name) -> {
                            executor.execute(() -> {
                                Code code = Code.get(rc);
                                if (code == Code.OK) {
                                    future.complete(new Stat(0, 0, 0));
                                } else if (code == Code.NODEEXISTS) {
                                    // We're emulating a request to create node, so the version is invalid
                                    future.completeExceptionally(getException(Code.BADVERSION, path));
                                } else {
                                    future.completeExceptionally(getException(code, path));
                                }
                            });
                        }, null);
            } else {
                zkc.setData(path, value, expectedVersion, (rc, path1, ctx, stat) -> {
                    executor.execute(() -> {
                        Code code = Code.get(rc);
                        if (code == Code.OK) {
                            future.complete(getStat(stat));
                        } else if (code == Code.NONODE) {
                            if (hasVersion) {
                                // We're emulating here a request to update or create the znode, depending on
                                // the version
                                future.completeExceptionally(getException(Code.BADVERSION, path));
                            } else {
                                // The z-node does not exist, let's create it first
                                put(path, value, Optional.of(-1L)).thenAccept(s -> future.complete(s))
                                        .exceptionally(ex -> {
                                            future.completeExceptionally(ex.getCause());
                                            return null;
                                        });
                            }
                        } else {
                            future.completeExceptionally(getException(code, path));
                        }
                    });
                }, null);
            }
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> optExpectedVersion) {
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            zkc.delete(path, expectedVersion, (rc, path1, ctx) -> {
                executor.execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                });
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        if (isZkManaged) {
            zkc.close();
        }
        executor.shutdownNow();
    }

    private static Stat getStat(org.apache.zookeeper.data.Stat zkStat) {
        return new Stat(zkStat.getVersion(), zkStat.getCtime(), zkStat.getMtime());
    }

    private static MetadataStoreException getException(Code code, String path) {
        KeeperException ex = KeeperException.create(code, path);

        switch (code) {
        case BADVERSION:
            return new BadVersionException(ex);
        case NONODE:
            return new NotFoundException(ex);
        default:
            return new MetadataStoreException(ex);
        }
    }
}
