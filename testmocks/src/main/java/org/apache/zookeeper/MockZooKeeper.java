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
package org.apache.zookeeper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import lombok.AllArgsConstructor;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockZooKeeper extends ZooKeeper {
    // ephemeralOwner value for persistent nodes
    private static final long NOT_EPHEMERAL = 0L;
    private static final String ROOT_PATH = "/";

    @AllArgsConstructor
    private static class MockZNode {
        byte[] content;
        int version;
        long ephemeralOwner;
        long creationTimestamp;
        long modificationTimestamp;
        List<String> children;

        static MockZNode of(byte[] content, int version, long ephemeralOwner) {
            return new MockZNode(content, version, ephemeralOwner, System.currentTimeMillis(),
                    System.currentTimeMillis(), new ArrayList<>());
        }

        public void updateVersion() {
            version++;
            modificationTimestamp = System.currentTimeMillis();
        }

        public void updateData(byte[] data) {
            content = data;
            updateVersion();
        }

        public Stat getStat() {
            return applyToStat(new Stat());
        }

        public Stat applyToStat(Stat stat) {
            stat.setCtime(creationTimestamp);
            stat.setMtime(modificationTimestamp);
            stat.setVersion(version);
            stat.setEphemeralOwner(ephemeralOwner);
            return stat;
        }

        public int getVersion() {
            return version;
        }

        public byte[] getContent() {
            return content;
        }

        public long getEphemeralOwner() {
            return ephemeralOwner;
        }

        public List<String> getChildren() {
            return children;
        }
    }

    private TreeMap<String, MockZNode> tree;
    private SetMultimap<String, NodeWatcher> watchers;
    private AtomicBoolean stopped;
    private AtomicReference<KeeperException.Code> alwaysFail;
    private CopyOnWriteArrayList<Failure> failures;
    private ExecutorService executor;

    private volatile Watcher sessionWatcher;
    private long sessionId = 1L;
    private int readOpDelayMs;

    private AtomicLong sequentialIdGenerator;
    private ThreadLocal<Long> overriddenSessionIdThreadLocal;
    private ThreadLocal<Boolean> inExecutorThreadLocal;
    private int referenceCount;
    private List<AutoCloseable> closeables;

    //see details of Objenesis caching - http://objenesis.org/details.html
    //see supported jvms - https://github.com/easymock/objenesis/blob/master/SupportedJVMs.md
    private static final Objenesis objenesis = new ObjenesisStd();

    public enum Op {
        CREATE, GET, SET, GET_CHILDREN, DELETE, EXISTS, SYNC,
    }

    private static class Failure {
        final KeeperException.Code failReturnCode;
        final BiPredicate<Op, String> predicate;

        Failure(KeeperException.Code failReturnCode, BiPredicate<Op, String> predicate) {
            this.failReturnCode = failReturnCode;
            this.predicate = predicate;
        }
    }

    private record PersistentWatcher(String path, Watcher watcher, AddWatchMode mode, long sessionId) {
    }

    private record NodeWatcher(Watcher watcher, long sessionId) {
    }

    private List<PersistentWatcher> persistentWatchers;

    public static MockZooKeeper newInstance() {
        return newInstance(-1);
    }

    public static MockZooKeeper newInstance(int readOpDelayMs) {
        try {
            return createMockZooKeeperInstance(readOpDelayMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    private static MockZooKeeper createMockZooKeeperInstance(int readOpDelayMs) {
        ObjectInstantiator<MockZooKeeper> mockZooKeeperInstantiator =
                objenesis.getInstantiatorOf(MockZooKeeper.class);
        MockZooKeeper zk = mockZooKeeperInstantiator.newInstance();
        zk.overriddenSessionIdThreadLocal = new ThreadLocal<>();
        zk.inExecutorThreadLocal = ThreadLocal.withInitial(() -> false);
        zk.init();
        zk.readOpDelayMs = readOpDelayMs;
        zk.sequentialIdGenerator = new AtomicLong();
        zk.closeables = new ArrayList<>();
        return zk;
    }

    private void init() {
        tree = Maps.newTreeMap();
        tree.put(ROOT_PATH, MockZNode.of(new byte[0], 0, NOT_EPHEMERAL));
        this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("mock-zookeeper"));
        watchers = HashMultimap.create();
        stopped = new AtomicBoolean(false);
        alwaysFail = new AtomicReference<>(KeeperException.Code.OK);
        failures = new CopyOnWriteArrayList<>();
        persistentWatchers = new ArrayList<>();
    }

    @Override
    public int getSessionTimeout() {
        return 30_000;
    }

    private MockZooKeeper(String quorum) throws Exception {
        // This constructor is never called
        super(quorum, 1, event -> {
        });
        assert false;
    }

    @Override
    public States getState() {
        return States.CONNECTED;
    }

    @Override
    public void register(Watcher watcher) {
        sessionWatcher = watcher;
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() -> internalCreate(path, data, createMode));
    }

    private <T> T runInExecutorReturningValue(Callable<T> task)
            throws InterruptedException, KeeperException {
        if (isStopped()) {
            throw new KeeperException.ConnectionLossException();
        }
        if (inExecutorThreadLocal.get()) {
            try {
                return task.call();
            } catch (Exception e) {
                if (e instanceof KeeperException ke) {
                    throw ke;
                }
                if (e instanceof InterruptedException ie) {
                    throw ie;
                }
                log.error("Unexpected exception", e);
                throw new KeeperException.SystemErrorException();
            }
        }
        try {
            long currentSessionId = getSessionId();
            return executor.submit(() -> {
                inExecutorThreadLocal.set(true);
                overrideSessionId(currentSessionId);
                try {
                    return task.call();
                } finally {
                    removeSessionIdOverride();
                    inExecutorThreadLocal.set(false);
                }
            }).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof KeeperException ke) {
                throw ke;
            }
            if (cause instanceof InterruptedException ie) {
                throw ie;
            }
            log.error("Unexpected exception", e);
            throw new KeeperException.SystemErrorException();
        }
    }

    private void runInExecutorAsync(Runnable runnable) {
        if (isStopped()) {
            throw new RejectedExecutionException("MockZooKeeper is stopped");
        }
        if (inExecutorThreadLocal.get()) {
            try {
                runnable.run();
            } catch (Throwable t) {
                log.error("Unexpected exception", t);
            }
            return;
        }
        long currentSessionId = getSessionId();
        executor.submit(() -> {
            try {
                inExecutorThreadLocal.set(true);
                overrideSessionId(currentSessionId);
                try {
                    runnable.run();
                } finally {
                    removeSessionIdOverride();
                    inExecutorThreadLocal.set(false);
                }
            } catch (Throwable t) {
                log.error("Unexpected exception", t);
            }
        });
    }

    private void runInExecutorSync(Runnable runnable) {
        try {
            runInExecutorReturningValue(() -> {
                runnable.run();
                return null;
            });
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }

    private String internalCreate(String path, byte[] data, CreateMode createMode) throws KeeperException {
        final Set<Watcher> toNotifyCreate = Sets.newHashSet();
        final Set<Watcher> toNotifyParent = Sets.newHashSet();
        final String parent = getParentName(path);

        maybeThrowProgrammedFailure(Op.CREATE, path);

        if (isStopped()) {
            throw new KeeperException.ConnectionLossException();
        }

        if (tree.containsKey(path)) {
            throw new KeeperException.NodeExistsException(path);
        }

        MockZNode parentNode = tree.get(parent);

        if (parentNode == null) {
            throw new KeeperException.NoNodeException(parent);
        }

        if (createMode.isSequential()) {
            int parentVersion = parentNode.getVersion();
            path = path + parentVersion;
            parentNode.updateVersion();
        }

        parentNode.getChildren().add(getNodeName(path));
        tree.put(path, createMockZNode(data, createMode));

        toNotifyCreate.addAll(getWatchers(path));
        if (!ROOT_PATH.equals(parent)) {
            toNotifyParent.addAll(getWatchers(parent));
        }
        watchers.removeAll(path);

        final String finalPath = path;
        executor.execute(() -> {
            if (isStopped()) {
                return;
            }

            triggerPersistentWatches(finalPath, parent, EventType.NodeCreated);

            toNotifyCreate.forEach(
                    watcher -> watcher.process(
                            new WatchedEvent(EventType.NodeCreated,
                                    KeeperState.SyncConnected,
                                    finalPath)));
            toNotifyParent.forEach(
                    watcher -> watcher.process(
                            new WatchedEvent(EventType.NodeChildrenChanged,
                                    KeeperState.SyncConnected,
                                    parent)));
        });

        return path;
    }

    private static String getParentName(String path) {
        String parentName = path.substring(0, path.lastIndexOf('/'));
        return parentName.length() > 0 ? parentName : "/";
    }

    private static String getNodeName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private Collection<Watcher> getWatchers(String path) {
        Set<NodeWatcher> nodeWatchers = watchers.get(path);
        if (nodeWatchers != null) {
            return nodeWatchers.stream().map(NodeWatcher::watcher).toList();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public long getSessionId() {
        Long overriddenSessionId = overriddenSessionIdThreadLocal.get();
        if (overriddenSessionId != null) {
            return overriddenSessionId;
        }
        return sessionId;
    }

    public void overrideSessionId(long sessionId) {
        overriddenSessionIdThreadLocal.set(sessionId);
    }

    public void removeSessionIdOverride() {
        overriddenSessionIdThreadLocal.remove();
    }

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl, CreateMode createMode,
                       final StringCallback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                final Set<Watcher> toNotifyCreate = Sets.newHashSet();
                toNotifyCreate.addAll(getWatchers(path));

                final Set<Watcher> toNotifyParent = Sets.newHashSet();
                final String parent = getParentName(path);
                if (!ROOT_PATH.equals(parent)) {
                    toNotifyParent.addAll(getWatchers(parent));
                }

                final String name;
                if (createMode != null && createMode.isSequential()) {
                    name = path + sequentialIdGenerator.getAndIncrement();
                } else {
                    name = path;
                }

                Optional<KeeperException.Code> failure = programmedFailure(Op.CREATE, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                } else if (tree.containsKey(path)) {
                    cb.processResult(KeeperException.Code.NODEEXISTS.intValue(), path, ctx, null);
                } else if (!tree.containsKey(parent)) {
                    runNotifications(() -> {
                        toNotifyParent.forEach(watcher -> watcher
                                .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected,
                                        parent)));
                    });
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                } else {
                    tree.get(parent).getChildren().add(getNodeName(name));
                    tree.put(name, createMockZNode(data, createMode));
                    watchers.removeAll(name);
                    cb.processResult(0, path, ctx, name);
                    runNotifications(() -> {
                        triggerPersistentWatches(path, parent, EventType.NodeCreated);

                        toNotifyCreate.forEach(
                                watcher -> watcher.process(
                                        new WatchedEvent(EventType.NodeCreated,
                                                KeeperState.SyncConnected,
                                                name)));
                        toNotifyParent.forEach(
                                watcher -> watcher.process(
                                        new WatchedEvent(EventType.NodeChildrenChanged,
                                                KeeperState.SyncConnected,
                                                parent)));
                    });
                }
            } catch (Throwable ex) {
                log.error("create path : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            }
        });
    }

    public void runNotifications(Runnable runnable) {
        executor.execute(() -> {
            if (isStopped()) {
                return;
            }
            runnable.run();
        });
    }

    private boolean isStopped() {
        return stopped.get();
    }

    private MockZNode createMockZNode(byte[] data, CreateMode createMode) {
        return MockZNode.of(data, 0,
                createMode != null && createMode.isEphemeral() ? getSessionId() : NOT_EPHEMERAL);
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() -> internalGetData(path, watcher, stat));
    }

    private byte[] internalGetData(String path, Watcher watcher, Stat stat) throws KeeperException {
        maybeThrowProgrammedFailure(Op.GET, path);
        MockZNode value = tree.get(path);
        if (value == null) {
            throw new KeeperException.NoNodeException(path);
        } else {
            if (watcher != null) {
                watchers.put(path, new NodeWatcher(watcher, getSessionId()));
            }
            if (stat != null) {
                value.applyToStat(stat);
            }
            return value.getContent();
        }
    }

    @Override
    public void getData(final String path, boolean watch, final DataCallback cb, final Object ctx) {
        getData(path, null, cb, ctx);
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
            return;
        }
        runInExecutorAsync(() -> {
            checkReadOpDelay();
            try {
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null, null);
                    return;
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                    return;
                }

                MockZNode value = tree.get(path);
                if (value == null) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                } else {
                    if (watcher != null) {
                        watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                    }
                    Stat stat = value.getStat();
                    cb.processResult(0, path, ctx, value.getContent(), stat);
                }
            } catch (Throwable ex) {
                log.error("get data : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            }
        });
    }

    @Override
    public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                    return;
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                if (!tree.containsKey(path)) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                    return;
                }

                List<String> children = findFirstLevelChildren(path);
                if (watcher != null) {
                    watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                }
                cb.processResult(0, path, ctx, children);
            } catch (Throwable ex) {
                log.error("get children : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            }
        });
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() -> internalGetChildren(path, watcher));
    }

    private List<String> internalGetChildren(String path, Watcher watcher) throws KeeperException {
        maybeThrowProgrammedFailure(Op.GET_CHILDREN, path);

        if (!tree.containsKey(path)) {
            throw new KeeperException.NoNodeException(path);
        }

        if (watcher != null) {
            watchers.put(path, new NodeWatcher(watcher, getSessionId()));
        }

        return findFirstLevelChildren(path);
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return getChildren(path, null);
    }

    @Override
    public void getChildren(final String path, boolean watcher, final Children2Callback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                MockZNode mockZNode = tree.get(path);
                Stat stat = mockZNode != null ? mockZNode.getStat() : null;
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null, null);
                    return;
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                    return;
                } else if (mockZNode == null) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                    return;
                }

                List<String> children = findFirstLevelChildren(path);
                cb.processResult(0, path, ctx, children, stat);
            } catch (Throwable ex) {
                log.error("get children : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            }
        });
    }

    private List<String> findFirstLevelChildren(String path) {
        return new ArrayList<>(tree.get(path).getChildren());
    }

    private boolean hasChildren(String path) {
        return !tree.get(path).getChildren().isEmpty();
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() -> internalGetStat(path, null));
    }

    private Stat internalGetStat(String path, Watcher watcher) throws KeeperException {
        maybeThrowProgrammedFailure(Op.EXISTS, path);

        if (isStopped()) {
            throw new KeeperException.ConnectionLossException();
        }

        if (watcher != null) {
            watchers.put(path, new NodeWatcher(watcher, getSessionId()));
        }

        if (tree.containsKey(path)) {
            return tree.get(path).getStat();
        } else {
            return null;
        }
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() -> internalGetStat(path, watcher));
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path, null, cb, ctx);
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                Optional<KeeperException.Code> failure = programmedFailure(Op.EXISTS, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                    return;
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                if (watcher != null) {
                    watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                }

                MockZNode mockZNode = tree.get(path);
                if (mockZNode != null) {
                    Stat stat = mockZNode.getStat();
                    cb.processResult(0, path, ctx, stat);
                } else {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                }
            } catch (Throwable ex) {
                log.error("exist : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            }
        });
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
            return;
        }
        runInExecutorAsync(() -> {
            Optional<KeeperException.Code> failure = programmedFailure(Op.SYNC, path);
            if (failure.isPresent()) {
                cb.processResult(failure.get().intValue(), path, ctx);
                return;
            } else if (isStopped()) {
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                return;
            }
            cb.processResult(0, path, ctx);
        });
    }

    @Override
    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return runInExecutorReturningValue(() ->  internalSetData(path, data, version));
    }

    private Stat internalSetData(String path, byte[] data, int version) throws KeeperException {
        final Set<Watcher> toNotify = Sets.newHashSet();
        maybeThrowProgrammedFailure(Op.SET, path);

        if (isStopped()) {
            throw new KeeperException.ConnectionLossException();
        }

        if (!tree.containsKey(path)) {
            throw new KeeperException.NoNodeException(path);
        }

        MockZNode mockZNode = tree.get(path);
        int currentVersion = mockZNode.getVersion();

        // Check version
        if (version != -1 && version != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }

        log.debug("[{}] Updating -- current version: {}", path, currentVersion);
        mockZNode.updateData(data);
        Stat stat = mockZNode.getStat();
        toNotify.addAll(getWatchers(path));
        watchers.removeAll(path);

        runNotifications(() -> {
            triggerPersistentWatches(path, null, EventType.NodeDataChanged);

            toNotify.forEach(watcher -> watcher
                    .process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path)));
        });

        return stat;
    }

    @Override
    public void setData(final String path, final byte[] data, int version, final StatCallback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                final Set<Watcher> toNotify = Sets.newHashSet();
                Stat stat;
                Optional<KeeperException.Code> failure = programmedFailure(Op.SET, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                    return;
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                if (!tree.containsKey(path)) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                    return;
                }

                MockZNode mockZNode = tree.get(path);
                int currentVersion = mockZNode.getVersion();

                // Check version
                if (version != -1 && version != currentVersion) {
                    log.debug("[{}] Current version: {} -- Expected: {}", path, currentVersion, version);
                    Stat currentStat = mockZNode.getStat();
                    cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx, currentStat);
                    return;
                }

                log.debug("[{}] Updating -- current version: {}", path, currentVersion);
                mockZNode.updateData(data);
                stat = mockZNode.getStat();
                cb.processResult(0, path, ctx, stat);

                toNotify.addAll(getWatchers(path));
                watchers.removeAll(path);

                runNotifications(() -> {
                    triggerPersistentWatches(path, null, EventType.NodeDataChanged);

                    for (Watcher watcher : toNotify) {
                        watcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path));
                    }
                });
            } catch (Throwable ex) {
                log.error("Update data : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            }
        });
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        runInExecutorReturningValue(() -> {
            internalDelete(path, version);
            return null;
        });
    }

    private void internalDelete(String path, int version) throws KeeperException {
        maybeThrowProgrammedFailure(Op.DELETE, path);

        final Set<Watcher> toNotifyDelete;
        final Set<Watcher> toNotifyParent;
        final String parent;

        if (isStopped()) {
            throw new KeeperException.ConnectionLossException();
        } else if (!tree.containsKey(path)) {
            throw new KeeperException.NoNodeException(path);
        } else if (hasChildren(path)) {
            throw new KeeperException.NotEmptyException(path);
        }

        if (version != -1) {
            int currentVersion = tree.get(path).getVersion();
            if (version != currentVersion) {
                throw new KeeperException.BadVersionException(path);
            }
        }

        parent = getParentName(path);
        tree.remove(path);
        tree.get(parent).getChildren().remove(getNodeName(path));

        toNotifyDelete = Sets.newHashSet();
        toNotifyDelete.addAll(getWatchers(path));

        toNotifyParent = Sets.newHashSet();
        if (!ROOT_PATH.equals(parent)) {
            toNotifyParent.addAll(getWatchers(parent));
        }

        watchers.removeAll(path);

        runNotifications(() -> {
            for (Watcher watcher1 : toNotifyDelete) {
                watcher1.process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path));
            }
            for (Watcher watcher2 : toNotifyParent) {
                watcher2.process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent));
            }

            triggerPersistentWatches(path, parent, EventType.NodeDeleted);
        });
    }

    @Override
    public void delete(final String path, int version, final VoidCallback cb, final Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                final Set<Watcher> toNotifyDelete = Sets.newHashSet();
                toNotifyDelete.addAll(getWatchers(path));

                final Set<Watcher> toNotifyParent = Sets.newHashSet();
                final String parent = getParentName(path);
                if (!ROOT_PATH.equals(parent)) {
                    toNotifyParent.addAll(getWatchers(parent));
                }
                watchers.removeAll(path);

                Optional<KeeperException.Code> failure = programmedFailure(Op.DELETE, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx);
                } else if (isStopped()) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                } else if (!tree.containsKey(path)) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx);
                } else if (hasChildren(path)) {
                    cb.processResult(KeeperException.Code.NOTEMPTY.intValue(), path, ctx);
                } else {
                    if (version != -1) {
                        int currentVersion = tree.get(path).getVersion();
                        if (version != currentVersion) {
                            cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx);
                            return;
                        }
                    }

                    tree.remove(path);
                    tree.get(parent).getChildren().remove(getNodeName(path));
                    cb.processResult(0, path, ctx);

                    runNotifications(() -> {
                        triggerPersistentWatches(path, parent, EventType.NodeDeleted);
                        toNotifyDelete.forEach(watcher -> watcher
                                .process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path)));
                        toNotifyParent.forEach(watcher -> watcher
                                .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected,
                                        parent)));
                    });
                }
            } catch (Throwable ex) {
                log.error("delete path : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx);
            }
        });
    }

    @Override
    public void multi(Iterable<org.apache.zookeeper.Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), null, ctx, null);
            return;
        }
        runInExecutorAsync(() -> {
            try {
                List<OpResult> res = multi(ops);
                cb.processResult(KeeperException.Code.OK.intValue(), null, ctx, res);
            } catch (Exception e) {
                cb.processResult(KeeperException.Code.APIERROR.intValue(), null, ctx, null);
            }
        });
    }

    @Override
    public List<OpResult> multi(Iterable<org.apache.zookeeper.Op> ops) throws InterruptedException, KeeperException {
        return runInExecutorReturningValue(() -> internalMulti(ops));
    }

    private List<OpResult> internalMulti(Iterable<org.apache.zookeeper.Op> ops) {
        List<OpResult> res = new ArrayList<>();
        for (org.apache.zookeeper.Op op : ops) {
            switch (op.getType()) {
                case ZooDefs.OpCode.create -> {
                    handleOperation("create", op, () -> {
                        org.apache.zookeeper.Op.Create opc = ((org.apache.zookeeper.Op.Create) op);
                        CreateMode cm = CreateMode.fromFlag(opc.flags);
                        String path = create(op.getPath(), opc.data, null, cm);
                        res.add(new OpResult.CreateResult(path));
                    }, res);
                }
                case ZooDefs.OpCode.delete -> {
                    handleOperation("delete", op, () -> {
                        DeleteRequest deleteRequest = (DeleteRequest) op.toRequestRecord();
                        delete(op.getPath(), deleteRequest.getVersion());
                        res.add(new OpResult.DeleteResult());
                    }, res);
                }
                case ZooDefs.OpCode.setData -> {
                    handleOperation("setData", op, () -> {
                        SetDataRequest setDataRequest = (SetDataRequest) op.toRequestRecord();
                        Stat stat = setData(op.getPath(), setDataRequest.getData(), setDataRequest.getVersion());
                        res.add(new OpResult.SetDataResult(stat));
                    }, res);
                }
                case ZooDefs.OpCode.getChildren -> {
                    handleOperation("getChildren", op, () -> {
                        List<String> children = getChildren(op.getPath(), null);
                        res.add(new OpResult.GetChildrenResult(children));
                    }, res);
                }
                case ZooDefs.OpCode.getData -> {
                    Stat stat = new Stat();
                    handleOperation("getData", op, () -> {
                        byte[] payload = getData(op.getPath(), null, stat);
                        res.add(new OpResult.GetDataResult(payload, stat));
                    }, res);
                }
                default -> {
                    log.error("Unsupported operation for path {} type {} kind {} request {}", op.getPath(),
                            op.getType(), op.getKind(), op.toRequestRecord());
                    res.add(new OpResult.ErrorResult(KeeperException.Code.APIERROR.intValue()));
                }
            }
        }
        return res;
    }

    interface ZkOpHandler {
        void handle() throws KeeperException, InterruptedException;
    }

    private void handleOperation(String opName, org.apache.zookeeper.Op op, ZkOpHandler handler, List<OpResult> res) {
        try {
            handler.handle();
        } catch (Exception e) {
            if (e instanceof KeeperException keeperException) {
                res.add(new OpResult.ErrorResult(keeperException.code().intValue()));
            } else {
                log.error("Error handling {} operation for path {} type {} kind {} request {}", opName, op.getPath(),
                        op.getType(), op.getKind(), op.toRequestRecord(), e);
                res.add(new OpResult.ErrorResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue()));
            }
        }
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode) {
        runInExecutorSync(() -> {
            persistentWatchers.add(new PersistentWatcher(basePath, watcher, mode, getSessionId()));
        });
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        if (isStopped()) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), basePath, ctx);
            return;
        }
        runInExecutorAsync(() -> {
            addWatch(basePath, watcher, mode);
            cb.processResult(KeeperException.Code.OK.intValue(), basePath, ctx);
        });
    }

    public synchronized void increaseRefCount() {
        referenceCount++;
    }

    public synchronized MockZooKeeper registerCloseable(AutoCloseable closeable) {
        closeables.add(closeable);
        return this;
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (--referenceCount <= 0) {
            shutdown();
            closeables.forEach(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    log.error("Error closing closeable", e);
                }
            });
            closeables.clear();
        }
    }

    public void shutdown() throws InterruptedException {
        if (stopped.compareAndSet(false, true)) {
            Future<?> shutdownTask = executor.submit(() -> {
                tree.clear();
                watchers.clear();
                persistentWatchers.clear();
            });
            try {
                shutdownTask.get();
            } catch (ExecutionException e) {
                log.error("Error shutting down", e);
            }
            MoreExecutors.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }
    }

    Optional<KeeperException.Code> programmedFailure(Op op, String path) {
        KeeperException.Code error = alwaysFail.get();
        if (error != KeeperException.Code.OK) {
            return Optional.of(error);
        }
        Optional<Failure> failure = failures.stream().filter(f -> f.predicate.test(op, path)).findFirst();
        if (failure.isPresent()) {
            failures.remove(failure.get());
            return Optional.ofNullable(failure.get().failReturnCode);
        } else {
            return Optional.empty();
        }
    }

    void maybeThrowProgrammedFailure(Op op, String path) throws KeeperException {
        Optional<KeeperException.Code> failure = programmedFailure(op, path);
        if (failure.isPresent()) {
            throw KeeperException.create(failure.get());
        }
    }

    public void failConditional(KeeperException.Code rc, BiPredicate<Op, String> predicate) {
        failures.add(new Failure(rc, predicate));
    }

    public void delay(long millis, BiPredicate<Op, String> predicate) {
        failures.add(new Failure(null, (op, s) -> {
            if (predicate.test(op, s)) {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {}
                return true;
            }
            return false;
        }));
    }

    public void setAlwaysFail(KeeperException.Code rc) {
        alwaysFail.set(rc);
    }

    public void unsetAlwaysFail() {
        alwaysFail.set(KeeperException.Code.OK);
    }

    public void setSessionId(long id) {
        sessionId = id;
    }

    @Override
    public String toString() {
        return "MockZookeeper";
    }

    private void checkReadOpDelay() {
        if (readOpDelayMs > 0) {
            try {
                Thread.sleep(readOpDelayMs);
            } catch (InterruptedException e) {
                // Ok
            }
        }
    }

    private void triggerPersistentWatches(String path, String parent, EventType eventType) {
        persistentWatchers.forEach(w -> {
            if (w.mode == AddWatchMode.PERSISTENT_RECURSIVE) {
                if (path.startsWith(w.path())) {
                    w.watcher.process(new WatchedEvent(eventType, KeeperState.SyncConnected, path));
                }
            } else if (w.mode == AddWatchMode.PERSISTENT) {
                if (w.path().equals(path)) {
                    w.watcher.process(new WatchedEvent(eventType, KeeperState.SyncConnected, path));
                }

                if (eventType == EventType.NodeCreated || eventType == EventType.NodeDeleted) {
                    // Also notify parent
                    w.watcher.process(
                            new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent));
                }
            }
        });
    }

    public void deleteEphemeralNodes(long sessionId) {
        if (sessionId != NOT_EPHEMERAL) {
            runInExecutorSync(() -> {
                tree.values().removeIf(zNode -> zNode.getEphemeralOwner() == sessionId);
            });
        }
    }


    public void deleteWatchers(long sessionId) {
        runInExecutorSync(() -> {
            // remove all persistent watchers for the session
            persistentWatchers.removeIf(w -> w.sessionId == sessionId);
            // remove all watchers for the session
            List<Map.Entry<String, NodeWatcher>> watchersForSession =
                    watchers.entries().stream().filter(e -> e.getValue().sessionId == sessionId).toList();
            watchersForSession
                    .forEach(e -> watchers.remove(e.getKey(), e.getValue()));
        });
    }

    private static final Logger log = LoggerFactory.getLogger(MockZooKeeper.class);
}
