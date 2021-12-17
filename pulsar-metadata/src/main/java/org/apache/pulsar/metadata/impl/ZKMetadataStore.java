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
import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
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
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.batching.AbstractBatchedMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpDelete;
import org.apache.pulsar.metadata.impl.batching.OpGet;
import org.apache.pulsar.metadata.impl.batching.OpGetChildren;
import org.apache.pulsar.metadata.impl.batching.OpPut;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class ZKMetadataStore extends AbstractBatchedMetadataStore
        implements MetadataStoreExtended, MetadataStoreLifecycle {

    private final String metadataURL;
    private final MetadataStoreConfig metadataStoreConfig;
    private final boolean isZkManaged;
    private final ZooKeeper zkc;
    private Optional<ZKSessionWatcher> sessionWatcher;

    public ZKMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig, boolean enableSessionWatcher)
            throws MetadataStoreException {
        super(metadataStoreConfig);

        try {
            this.metadataURL = metadataURL;
            this.metadataStoreConfig = metadataStoreConfig;
            isZkManaged = true;
            zkc = PulsarZooKeeperClient.newBuilder().connectString(metadataURL)
                    .connectRetryPolicy(new BoundExponentialBackoffRetryPolicy(100, 60_000, Integer.MAX_VALUE))
                    .allowReadOnlyMode(metadataStoreConfig.isAllowReadOnlyOperations())
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis())
                    .watchers(Collections.singleton(event -> {
                        if (sessionWatcher != null) {
                            sessionWatcher.ifPresent(sw -> sw.process(event));
                        }
                    }))
                    .build();
            zkc.addWatch("/", this::handleWatchEvent, AddWatchMode.PERSISTENT_RECURSIVE);
            if (enableSessionWatcher) {
                sessionWatcher = Optional.of(new ZKSessionWatcher(zkc, this::receivedSessionEvent));
            } else {
                sessionWatcher = Optional.empty();
            }
        } catch (Throwable t) {
            throw new MetadataStoreException(t);
        }
    }

    @VisibleForTesting
    @SneakyThrows
    public ZKMetadataStore(ZooKeeper zkc) {
        super(MetadataStoreConfig.builder().build());

        this.metadataURL = null;
        this.metadataStoreConfig = null;
        this.isZkManaged = false;
        this.zkc = zkc;
        this.sessionWatcher = Optional.of(new ZKSessionWatcher(zkc, this::receivedSessionEvent));
        zkc.addWatch("/", this::handleWatchEvent, AddWatchMode.PERSISTENT_RECURSIVE);
    }

    @Override
    protected void receivedSessionEvent(SessionEvent event) {
        if (event == SessionEvent.SessionReestablished) {
            // Recreate the persistent watch on the new session
            zkc.addWatch("/", this::handleWatchEvent, AddWatchMode.PERSISTENT_RECURSIVE,
                    (rc, path, ctx) -> {
                        if (rc == Code.OK.intValue()) {
                            super.receivedSessionEvent(event);
                        } else {
                            log.error("Failed to recreate persistent watch on ZooKeeper: {}", Code.get(rc));
                            sessionWatcher.ifPresent(ZKSessionWatcher::setSessionInvalid);
                            // On the reconnectable client, mark the session as expired to trigger a new reconnect and
                            // we will have the chance to set the watch again.
                            if (zkc instanceof PulsarZooKeeperClient) {
                                ((PulsarZooKeeperClient) zkc).process(
                                        new WatchedEvent(Watcher.Event.EventType.None,
                                                Watcher.Event.KeeperState.Expired,
                                                null));
                             }
                        }
                    }, null);
        } else {
            super.receivedSessionEvent(event);
        }
    }

    @Override
    protected void batchOperation(List<MetadataOp> ops) {
        try {
            zkc.multi(ops.stream().map(this::convertOp).collect(Collectors.toList()), (rc, path, ctx, results) -> {
                if (results == null) {
                    Code code = Code.get(rc);
                    if (code == Code.CONNECTIONLOSS) {
                        // There is the chance that we caused a connection reset by sending or requesting a batch
                        // that passed the max ZK limit. Retry with the individual operations
                        executor.schedule(() -> {
                            ops.forEach(o -> batchOperation(Collections.singletonList(o)));
                        }, 100, TimeUnit.MILLISECONDS);
                    } else {
                        MetadataStoreException e = getException(code, path);
                        ops.forEach(o -> o.getFuture().completeExceptionally(e));
                    }
                    return;
                }

                // Trigger all the futures in the batch
                for (int i = 0; i < ops.size(); i++) {
                    OpResult opr = results.get(i);
                    MetadataOp op = ops.get(i);

                    switch (op.getType()) {
                        case PUT:
                            handlePutResult(op.asPut(), opr);
                            break;
                        case DELETE:
                            handleDeleteResult(op.asDelete(), opr);
                            break;
                        case GET:
                            handleGetResult(op.asGet(), opr);
                            break;
                        case GET_CHILDREN:
                            handleGetChildrenResult(op.asGetChildren(), opr);
                            break;

                        default:
                            op.getFuture().completeExceptionally(new MetadataStoreException(
                                    "Operation type not supported in multi: " + op.getType()));
                    }
                }
            }, null);
        } catch (Throwable t) {
            ops.forEach(o -> o.getFuture().completeExceptionally(new MetadataStoreException(t)));
        }
    }

    private void handlePutResult(OpPut op, OpResult opr) {
        if (opr instanceof OpResult.ErrorResult) {
            OpResult.ErrorResult er = (OpResult.ErrorResult) opr;
            Code code = Code.get(er.getErr());
            if (code == Code.NONODE) {
                // Receiving no-node for a put operation, means that parent node are not
                // existing: let's make sure to create them
                internalStorePut(op);
            } else if (code == Code.NODEEXISTS) {
                // We're emulating a request to create node, so the version is invalid
                op.getFuture().completeExceptionally(getException(Code.BADVERSION, op.getPath()));
            } else if (code == Code.RUNTIMEINCONSISTENCY || code == Code.OK) {
                // This error will happen when other items in the batch did already fail. In this case, we're
                // retrying the operation individually
                internalStorePut(op);
            } else {
                op.getFuture().completeExceptionally(getException(code, op.getPath()));
            }
        } else if (opr instanceof OpResult.CreateResult) {
            OpResult.CreateResult cr = (OpResult.CreateResult) opr;
            op.getFuture().complete(new Stat(cr.getPath(), 0, 0, 0, op.isEphemeral(), true));
        } else {
            OpResult.SetDataResult sdr = (OpResult.SetDataResult) opr;
            op.getFuture().complete(getStat(op.getPath(), sdr.getStat()));
        }
    }

    private void handleGetResult(OpGet op, OpResult opr) {
        if (opr instanceof OpResult.ErrorResult) {
            OpResult.ErrorResult er = (OpResult.ErrorResult) opr;
            Code code = Code.get(er.getErr());
            if (code == Code.NONODE) {
                // For get operations, we return an empty optional
                op.getFuture().complete(Optional.empty());
            } else {
                op.getFuture().completeExceptionally(getException(code, op.getPath()));
            }
        } else {
            OpResult.GetDataResult gdr = (OpResult.GetDataResult) opr;
            op.getFuture().complete(Optional.of(new GetResult(gdr.getData(), getStat(op.getPath(), gdr.getStat()))));
        }
    }

    private void handleGetChildrenResult(OpGetChildren op, OpResult opr) {
        if (opr instanceof OpResult.ErrorResult) {
            OpResult.ErrorResult er = (OpResult.ErrorResult) opr;
            Code code = Code.get(er.getErr());
            if (code == Code.NONODE) {
                op.asGetChildren().getFuture().complete(Collections.emptyList());
            } else {
                op.getFuture().completeExceptionally(getException(code, op.getPath()));
            }
        } else {
            OpResult.GetChildrenResult gdr = (OpResult.GetChildrenResult) opr;
            Collections.sort(gdr.getChildren());
            op.getFuture().complete(gdr.getChildren());
        }
    }

    private void handleDeleteResult(OpDelete op, OpResult opr) {
        if (opr instanceof OpResult.ErrorResult) {
            OpResult.ErrorResult er = (OpResult.ErrorResult) opr;
            Code code = Code.get(er.getErr());
            if (code == Code.RUNTIMEINCONSISTENCY || code == Code.OK) {
                // This error will happen when other items in the batch did already fail. In this case, we're
                // retrying the operation individually
                internalStoreDelete(op);
            } else {
                op.getFuture().completeExceptionally(getException(code, op.getPath()));
            }
        } else {
            op.getFuture().complete(null);
        }
    }

    private Op convertOp(MetadataOp op) {
        switch (op.getType()) {
            case GET: {
                return Op.getData(op.asGet().getPath());
            }
            case PUT: {
                OpPut p = op.asPut();
                CreateMode createMode = getCreateMode(p.getOptions());
                if (p.getOptExpectedVersion().isPresent() && p.getOptExpectedVersion().get() == -1L) {
                    // We are assuming a create operation
                    return Op.create(p.getPath(), p.getData(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                } else {
                    // Assuming a set-data
                    return Op.setData(p.getPath(), p.getData(), p.getOptExpectedVersion().orElse(-1L).intValue());
                }
            }
            case DELETE: {
                OpDelete d = op.asDelete();
                return Op.delete(d.getPath(), d.getOptExpectedVersion().orElse(-1L).intValue());
            }
            case GET_CHILDREN: {
                return Op.getChildren(op.asGetChildren().getPath());
            }

            default:
                return null;
        }
    }

    @Override
    public CompletableFuture<Boolean> existsFromStore(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        try {
            zkc.exists(path, null, (rc, path1, ctx, stat) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(true);
                    } else if (code == Code.NONODE) {
                        future.complete(false);
                    } else {
                        future.completeExceptionally(getException(code, path));
                    }
                }, future);
            }, future);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));

        }

        return future;
    }

    private void internalStoreDelete(OpDelete op) {
        int expectedVersion = op.getOptExpectedVersion().orElse(-1L).intValue();

        CompletableFuture<Void> future = op.getFuture();

        try {
            zkc.delete(op.getPath(), expectedVersion, (rc, path1, ctx) -> {
                execute(() -> {
                    Code code = Code.get(rc);
                    if (code == Code.OK) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(getException(code, op.getPath()));
                    }
                }, future);
            }, null);
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    private void internalStorePut(OpPut opPut) {
        boolean hasVersion = opPut.getOptExpectedVersion().isPresent();
        int expectedVersion = opPut.getOptExpectedVersion().orElse(-1L).intValue();

        CompletableFuture<Stat> future = opPut.getFuture();

        try {
            if (hasVersion && expectedVersion == -1) {
                CreateMode createMode = getCreateMode(opPut.getOptions());
                asyncCreateFullPathOptimistic(zkc, opPut.getPath(), opPut.getData(), createMode,
                        (rc, path1, ctx, name) -> {
                            execute(() -> {
                                Code code = Code.get(rc);
                                if (code == Code.OK) {
                                    future.complete(new Stat(name, 0, 0, 0, createMode.isEphemeral(), true));
                                } else if (code == Code.NODEEXISTS) {
                                    // We're emulating a request to create node, so the version is invalid
                                    future.completeExceptionally(getException(Code.BADVERSION, opPut.getPath()));
                                } else {
                                    future.completeExceptionally(getException(code, opPut.getPath()));
                                }
                            }, future);
                        });
            } else {
                zkc.setData(opPut.getPath(), opPut.getData(), expectedVersion, (rc, path1, ctx, stat) -> {
                    execute(() -> {
                        Code code = Code.get(rc);
                        if (code == Code.OK) {
                            future.complete(getStat(path1, stat));
                        } else if (code == Code.NONODE) {
                            if (hasVersion) {
                                // We're emulating here a request to update or create the znode, depending on
                                // the version
                                future.completeExceptionally(getException(Code.BADVERSION, opPut.getPath()));
                            } else {
                                // The z-node does not exist, let's create it first
                                put(opPut.getPath(), opPut.getData(), Optional.of(-1L)).thenAccept(
                                                s -> future.complete(s))
                                        .exceptionally(ex -> {
                                            future.completeExceptionally(new MetadataStoreException(ex.getCause()));
                                            return null;
                                        });
                            }
                        } else {
                            future.completeExceptionally(getException(code, opPut.getPath()));
                        }
                    }, future);
                }, null);
            }
        } catch (Throwable t) {
            future.completeExceptionally(new MetadataStoreException(t));
        }
    }

    @Override
    public void close() throws Exception {
        if (isZkManaged) {
            zkc.close();
        }
        if (sessionWatcher.isPresent()) {
            sessionWatcher.get().close();
        }
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

    private void handleWatchEvent(WatchedEvent event) {
        if (log.isDebugEnabled()) {
            log.debug("Received ZK watch : {}", event);
        }
        String path = event.getPath();
        if (path == null) {
            // Ignore Session events
            return;
        }

        String parent = parent(path);
        Notification childrenChangedNotification = null;

        NotificationType type;
        switch (event.getType()) {
        case NodeCreated:
            type = NotificationType.Created;
            if (parent != null) {
                childrenChangedNotification = new Notification(NotificationType.ChildrenChanged, parent);
            }
            break;

        case NodeDataChanged:
            type = NotificationType.Modified;
            break;

        case NodeChildrenChanged:
            type = NotificationType.ChildrenChanged;
            break;

        case NodeDeleted:
            type = NotificationType.Deleted;
            if (parent != null) {
                childrenChangedNotification = new Notification(NotificationType.ChildrenChanged, parent);
            }
            break;

        default:
            return;
        }

        receivedNotification(new Notification(type, event.getPath()));
        if (childrenChangedNotification != null) {
            receivedNotification(childrenChangedNotification);
        }
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

    public ZooKeeper getZkClient() {
        return zkc;
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
            try (ZooKeeper chrootZk = PulsarZooKeeperClient.newBuilder()
                    .connectString(zkConnectForChrootCreation)
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis())
                    .connectRetryPolicy(
                            new BoundExponentialBackoffRetryPolicy(metadataStoreConfig.getSessionTimeoutMillis(),
                                    metadataStoreConfig.getSessionTimeoutMillis(), 0))
                    .build()) {
                if (chrootZk.exists(chrootPath, false) == null) {
                    createFullPathOptimistic(chrootZk, chrootPath, new byte[0], CreateMode.PERSISTENT);
                    log.info("Created zookeeper chroot path {} successfully", chrootPath);
                }
            } catch (Exception e) {
                return FutureUtil.failedFuture(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private static void asyncCreateFullPathOptimistic(final ZooKeeper zk, final String originalPath, final byte[] data,
                                                      final CreateMode createMode,
                                                      final AsyncCallback.StringCallback callback) {
        zk.create(originalPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, (rc, path, ctx, name) -> {
            if (rc != Code.NONODE.intValue()) {
                callback.processResult(rc, path, ctx, name);
            } else {
                String parent = (new File(originalPath)).getParent().replace("\\", "/");

                // Create parent nodes as "CONTAINER" so that ZK will automatically delete them when they're empty
                asyncCreateFullPathOptimistic(zk, parent, new byte[0], CreateMode.CONTAINER,
                        (rc1, path1, ctx1, name1) -> {
                            if (rc1 != Code.OK.intValue() && rc1 != Code.NODEEXISTS.intValue()) {
                                callback.processResult(rc1, path1, ctx1, name1);
                            } else {
                                asyncCreateFullPathOptimistic(zk, originalPath, data,
                                        createMode, callback);
                            }
                        });
            }
        }, null);
    }

    private static void createFullPathOptimistic(ZooKeeper zkc, String path, byte[] data, CreateMode createMode)
            throws KeeperException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(Code.OK.intValue());
        asyncCreateFullPathOptimistic(zkc, path, data, createMode, (rc2, path1, ctx, name) -> {
            rc.set(rc2);
            latch.countDown();
        });
        latch.await();
        if (rc.get() != Code.OK.intValue()) {
            throw KeeperException.create(Code.get(rc.get()));
        }
    }
}
