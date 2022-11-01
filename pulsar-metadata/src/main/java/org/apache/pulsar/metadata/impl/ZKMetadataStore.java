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
package org.apache.pulsar.metadata.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreLifecycle;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class ZKMetadataStore extends AbstractMetadataStore implements MetadataStoreExtended, Watcher, MetadataStoreLifecycle {

    private final String metadataURL;
    private final MetadataStoreConfig metadataStoreConfig;
    private final boolean isZkManaged;
    private final ZooKeeper zkc;
    private ZKSessionWatcher sessionWatcher;

    public ZKMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) throws MetadataStoreException {
        try {
            this.metadataURL = metadataURL;
            this.metadataStoreConfig = metadataStoreConfig;
            isZkManaged = true;
            zkc = ZooKeeperClient.newBuilder().connectString(metadataURL)
                    .connectRetryPolicy(new BoundExponentialBackoffRetryPolicy(100, 60_000, Integer.MAX_VALUE))
                    .allowReadOnlyMode(metadataStoreConfig.isAllowReadOnlyOperations())
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis())
                    .watchers(Collections.singleton(event -> {
                        if (sessionWatcher != null) {
                            sessionWatcher.process(event);
                        }
                    }))
                    .build();
            sessionWatcher = new ZKSessionWatcher(zkc, this::receivedSessionEvent);
        } catch (Throwable t) {
            throw new MetadataStoreException(t);
        }
    }

    @VisibleForTesting
    public ZKMetadataStore(ZooKeeper zkc) {
        this.metadataURL = null;
        this.metadataStoreConfig = null;
        this.isZkManaged = false;
        this.zkc = zkc;
        this.sessionWatcher = new ZKSessionWatcher(zkc, this::receivedSessionEvent);
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        CompletableFuture<Optional<GetResult>> future = new CompletableFuture<>();
        getInternal(path, future);
        return future;
    }

    private void getInternal(String path, CompletableFuture<Optional<GetResult>> future) {
        try {
            zkc.getData(path, this, (rc, path1, ctx, data, stat) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(Optional.of(new GetResult(data, getStat(path1, stat))));
                    } else if (code == Code.NONODE) {
                        future.complete(Optional.empty());
                        // if z-node does not exist then discards cache,
                        // because we can't be received NodeCreated event
                        metadataCaches.forEach(c -> c.invalidate(path));
                        receivedNotification(new Notification(NotificationType.Invalidate, path));
                    } else if (code == Code.CONNECTIONLOSS) {
                        // There is the chance that we caused a connection reset by sending or requesting a batch
                        // that passed the max ZK limit. Retry with the individual operations
                        log.warn("Zookeeper connection loss, get {}, retry after 100ms", path);
                        executor.schedule(() -> getInternal(path, future),
                            100, TimeUnit.MILLISECONDS);
                    }  else {
                        future.completeExceptionally(getException(code, path));
                    }
                }, future);
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        getChildrenFromStoreInternal(path, future);
        return future;
    }

    private void getChildrenFromStoreInternal(String path, CompletableFuture<List<String>> future) {
        try {
            zkc.getChildren(path, this, (rc, path1, ctx, children) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        Collections.sort(children);
                        future.complete(children);
                    } else if (code == Code.NONODE) {
                        // Z-node does not exist
                        future.complete(Collections.emptyList());
                        // if z-node does not exist then discards cache,
                        // because we can't be received NodeCreated event
                        childrenCache.synchronous().invalidate(path);
                        receivedNotification(new Notification(NotificationType.Invalidate, path));
                    } else if (code == Code.CONNECTIONLOSS) {
                        // There is the chance that we caused a connection reset by sending or requesting a batch
                        // that passed the max ZK limit. Retry with the individual operations
                        executor.schedule(() -> getChildrenFromStoreInternal(path, future),
                            100, TimeUnit.MILLISECONDS);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                }, future);
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public CompletableFuture<Boolean> existsFromStore(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        existsFromStoreInternal(path, future);
        return future;
    }

    private void existsFromStoreInternal(String path, CompletableFuture<Boolean> future) {
        try {
            zkc.exists(path, this, (rc, path1, ctx, stat) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(true);
                    } else if (code == Code.NONODE) {
                        future.complete(false);
                        // remove watcher if node does not exist
                        zkc.removeAllWatches(path, Watcher.WatcherType.Any, true,
                                (rc0, path0, ctx0) -> {
                                    Code code0 = Code.get(rc0);
                                    if (code0 != Code.OK && code0 != Code.NOWATCHER) {
                                        log.warn("Remove watcher for non-existing znode failed. rc: {}, path: {}", code0, path);
                                    }
                                }, null);
                    } else if (code == Code.CONNECTIONLOSS) {
                        // There is the chance that we caused a connection reset by sending or requesting a batch
                        // that passed the max ZK limit. Retry with the individual operations
                        log.warn("Zookeeper connection loss, existsFromStore {}, retry after 100ms", path);
                        executor.schedule(() -> existsFromStoreInternal(path, future),
                            100, TimeUnit.MILLISECONDS);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                }, future);
            }, future);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> optExpectedVersion) {
        return put(path, value, optExpectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public CompletableFuture<Stat> storePut(String path, byte[] value, Optional<Long> optExpectedVersion,
                                            EnumSet<CreateOption> options) {
        CompletableFuture<Stat> future = new CompletableFuture<>();
        storePutInternal(path, value, optExpectedVersion, options, future);
        return future;
    }

    private void storePutInternal(String path, byte[] value, Optional<Long> optExpectedVersion,
            EnumSet<CreateOption> options, CompletableFuture<Stat> future) {
        boolean hasVersion = optExpectedVersion.isPresent();
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        try {
            if (hasVersion && expectedVersion == -1) {
                CreateMode createMode = getCreateMode(options);
                ZkUtils.asyncCreateFullPathOptimistic(zkc, path, value, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        createMode, (rc, path1, ctx, name) -> {
                            execute(() -> {
                                Code code = Code.get(rc);
                                if (code == Code.OK) {
                                    future.complete(new Stat(name, 0, 0, 0, createMode.isEphemeral(), true));
                                } else if (code == Code.NODEEXISTS) {
                                    // We're emulating a request to create node, so the version is invalid
                                    future.completeExceptionally(getException(Code.BADVERSION, path));
                                } else if (code == Code.CONNECTIONLOSS) {
                                    // There is the chance that we caused a connection reset by sending or requesting a batch
                                    // that passed the max ZK limit. Retry with the individual operations
                                    log.warn("Zookeeper connection loss, storePut {}, retry after 100ms", path);
                                    executor.schedule(() ->
                                            storePutInternal(path, value, optExpectedVersion, options, future),
                                        100, TimeUnit.MILLISECONDS);
                                } else {
                                    future.completeExceptionally(getException(code, path));
                                }
                            }, future);
                        }, null);
            } else {
                zkc.setData(path, value, expectedVersion, (rc, path1, ctx, stat) -> {
                    execute(() -> {
                        Code code = Code.get(rc);
                        if (code == Code.OK) {
                            future.complete(getStat(path1, stat));
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
                        } else if (code == Code.CONNECTIONLOSS) {
                            // There is the chance that we caused a connection reset by sending or requesting a batch
                            // that passed the max ZK limit. Retry with the individual operations
                            log.warn("Zookeeper connection loss, storePut {}, retry after 100ms", path);
                            executor.schedule(() -> storePutInternal(path, value, optExpectedVersion, options, future),
                                100, TimeUnit.MILLISECONDS);
                        } else {
                            future.completeExceptionally(getException(code, path));
                        }
                    }, future);
                }, null);
            }
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public CompletableFuture<Void> storeDelete(String path, Optional<Long> optExpectedVersion) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        storeDeleteInternal(path, optExpectedVersion, future);
        return future;
    }

    private void storeDeleteInternal(String path, Optional<Long> optExpectedVersion,
                                                       CompletableFuture<Void> future) {
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        try {
            zkc.delete(path, expectedVersion, (rc, path1, ctx) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(null);
                    } else if (code == Code.CONNECTIONLOSS) {
                        // There is the chance that we caused a connection reset by sending or requesting a batch
                        // that passed the max ZK limit. Retry with the individual operations
                        log.warn("Zookeeper connection loss, storeDelete {}, retry after 100ms", path);
                        executor.schedule(() -> storeDeleteInternal(path, optExpectedVersion, future),
                            100, TimeUnit.MILLISECONDS);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                }, future);
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public void close() throws Exception {
        if (isZkManaged) {
            zkc.close();
        }
        sessionWatcher.close();
        super.close();
    }

    private Stat getStat(String path, org.apache.zookeeper.data.Stat zkStat) {
        return new Stat(path, zkStat.getVersion(), zkStat.getCtime(), zkStat.getMtime(),
                zkStat.getEphemeralOwner() != -1,
                zkStat.getEphemeralOwner() == zkc.getSessionId());
    }

    private static MetadataStoreException getException(Code code, String path) {
        KeeperException ex = KeeperException.create(code, path);

        switch (code) {
        case BADVERSION:
            return new BadVersionException(ex);
        case NONODE:
            return new NotFoundException(ex);
        case NODEEXISTS:
            return new AlreadyExistsException(ex);
        default:
            return new MetadataStoreException(ex);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (log.isDebugEnabled()) {
            log.debug("Received ZK watch : {}", event);
        }
        String path = event.getPath();
        if (path == null) {
            // Ignore Session events
            return;
        }

        NotificationType type;
        switch (event.getType()) {
        case NodeCreated:
            type = NotificationType.Created;
            break;

        case NodeDataChanged:
            type = NotificationType.Modified;
            break;

        case NodeChildrenChanged:
            type = NotificationType.ChildrenChanged;
            break;

        case NodeDeleted:
            type = NotificationType.Deleted;
            break;

        default:
            return;
        }

        receivedNotification(new Notification(type, event.getPath()));
    }

    private static CreateMode getCreateMode(EnumSet<CreateOption> options) {
        if (options.contains(CreateOption.Ephemeral)) {
            if (options.contains(CreateOption.Sequential)) {
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            } else {
                return CreateMode.EPHEMERAL;
            }
        } else if (options.contains(CreateOption.Sequential)) {
            return CreateMode.PERSISTENT_SEQUENTIAL;
        } else {
            return CreateMode.PERSISTENT;
        }
    }

    public long getZkSessionId() {
        return zkc.getSessionId();
    }

    @Override
    public CompletableFuture<Void> initializeCluster() {
        if (this.metadataURL == null) {
            return FutureUtil.failedFuture(new MetadataStoreException("metadataURL is not set"));
        }
        if (this.metadataStoreConfig == null) {
            return FutureUtil.failedFuture(new MetadataStoreException("metadataStoreConfig is not set"));
        }
        int chrootIndex = metadataURL.indexOf("/");
        if (chrootIndex > 0) {
            String chrootPath = metadataURL.substring(chrootIndex);
            String zkConnectForChrootCreation = metadataURL.substring(0, chrootIndex);
            try (ZooKeeper chrootZk = ZooKeeperClient.newBuilder()
                    .connectString(zkConnectForChrootCreation)
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis())
                    .connectRetryPolicy(
                            new BoundExponentialBackoffRetryPolicy(metadataStoreConfig.getSessionTimeoutMillis(),
                                    metadataStoreConfig.getSessionTimeoutMillis(), 0))
                    .build()) {
                if (chrootZk.exists(chrootPath, false) == null) {
                    ZkUtils.createFullPathOptimistic(chrootZk, chrootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                    log.info("Created zookeeper chroot path {} successfully", chrootPath);
                }
            } catch (Exception e) {
                return FutureUtil.failedFuture(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
