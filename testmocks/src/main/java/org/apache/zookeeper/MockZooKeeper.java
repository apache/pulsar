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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
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
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockZooKeeper extends ZooKeeper {
    // ephemeralOwner value for persistent nodes
    private static final long NOT_EPHEMERAL = 0L;

    @AllArgsConstructor
    private static class MockZNode {
        byte[] content;
        int version;
        long ephemeralOwner;
        long creationTimestamp;
        long modificationTimestamp;

        static MockZNode of(byte[] content, int version, long ephemeralOwner) {
            return new MockZNode(content, version, ephemeralOwner, System.currentTimeMillis(),
                    System.currentTimeMillis());
        }

        public synchronized void updateVersion() {
            version++;
            modificationTimestamp = System.currentTimeMillis();
        }

        public synchronized void updateData(byte[] data) {
            content = data;
            updateVersion();
        }

        public Stat getStat() {
            return applyToStat(new Stat());
        }

        public synchronized Stat applyToStat(Stat stat) {
            stat.setCtime(creationTimestamp);
            stat.setMtime(modificationTimestamp);
            stat.setVersion(version);
            stat.setEphemeralOwner(ephemeralOwner);
            return stat;
        }

        public synchronized int getVersion() {
            return version;
        }

        public synchronized byte[] getContent() {
            return content;
        }

        public long getEphemeralOwner() {
            return ephemeralOwner;
        }
    }

    private TreeMap<String, MockZNode> tree;
    private SetMultimap<String, NodeWatcher> watchers;
    private volatile boolean stopped;
    private AtomicReference<KeeperException.Code> alwaysFail;
    private CopyOnWriteArrayList<Failure> failures;
    private ExecutorService executor;

    private Watcher sessionWatcher;
    private long sessionId = 1L;
    private int readOpDelayMs;

    private ReentrantLock mutex;

    private AtomicLong sequentialIdGenerator;
    private ThreadLocal<Long> overriddenSessionIdThreadLocal;
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
        return newInstance(null);
    }

    public static MockZooKeeper newInstance(ExecutorService executor) {
        return newInstance(executor, -1);
    }

    public static MockZooKeeper newInstanceForGlobalZK(ExecutorService executor) {
        return newInstanceForGlobalZK(executor, -1);
    }

    public static MockZooKeeper newInstanceForGlobalZK(ExecutorService executor, int readOpDelayMs) {
        try {
            return createMockZooKeeperInstance(executor, readOpDelayMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    public static MockZooKeeper newInstance(ExecutorService executor, int readOpDelayMs) {
        try {
            return createMockZooKeeperInstance(executor, readOpDelayMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    private static MockZooKeeper createMockZooKeeperInstance(ExecutorService executor, int readOpDelayMs) {
        ObjectInstantiator<MockZooKeeper> mockZooKeeperInstantiator =
                objenesis.getInstantiatorOf(MockZooKeeper.class);
        MockZooKeeper zk = mockZooKeeperInstantiator.newInstance();
        zk.overriddenSessionIdThreadLocal = new ThreadLocal<>();
        zk.init(executor);
        zk.readOpDelayMs = readOpDelayMs;
        zk.mutex = new ReentrantLock();
        zk.lockInstance = ThreadLocal.withInitial(zk::createLock);
        zk.sequentialIdGenerator = new AtomicLong();
        zk.closeables = new ArrayList<>();
        return zk;
    }

    private void init(ExecutorService executor) {
        tree = Maps.newTreeMap();
        if (executor != null) {
            this.executor = executor;
        } else {
            this.executor = Executors.newFixedThreadPool(1, new DefaultThreadFactory("mock-zookeeper"));
        }
        SetMultimap<String, NodeWatcher> w = HashMultimap.create();
        watchers = Multimaps.synchronizedSetMultimap(w);
        stopped = false;
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


    @Slf4j
    private static class SingleAcquireAndReleaseLock {
        private final AtomicBoolean acquired = new AtomicBoolean(false);
        private final Lock lock;

        SingleAcquireAndReleaseLock(Lock lock) {
            this.lock = lock;
        }

        public void lock() {
            if (acquired.compareAndSet(false, true)) {
                lock.lock();
            } else {
                throw new IllegalStateException("Lock was already acquired!");
            }
        }

        public void unlockIfNeeded() {
            if (acquired.compareAndSet(true, false)) {
                lock.unlock();
            }
        }
    }

    private ThreadLocal<SingleAcquireAndReleaseLock> lockInstance;

    private SingleAcquireAndReleaseLock createLock() {
        return new SingleAcquireAndReleaseLock(mutex);
    }

    private void lock() {
        lockInstance.get().lock();
    }

    private void unlockIfLocked() {
        lockInstance.get().unlockIfNeeded();
    }

    @Override
    public void register(Watcher watcher) {
        lock();
        sessionWatcher = watcher;
        unlockIfLocked();
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        final Set<Watcher> toNotifyCreate = Sets.newHashSet();
        final Set<Watcher> toNotifyParent = Sets.newHashSet();
        final String parent = path.substring(0, path.lastIndexOf("/"));

        lock();
        try {
            maybeThrowProgrammedFailure(Op.CREATE, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            }

            if (tree.containsKey(path)) {
                throw new KeeperException.NodeExistsException(path);
            }

            if (!parent.isEmpty() && !tree.containsKey(parent)) {
                throw new KeeperException.NoNodeException();
            }

            if (createMode.isSequential()) {
                MockZNode parentNode = tree.get(parent);
                int parentVersion = tree.get(parent).getVersion();
                path = path + parentVersion;
                parentNode.updateVersion();
            }

            tree.put(path, createMockZNode(data, createMode));

            toNotifyCreate.addAll(getWatchers(path));
            if (!parent.isEmpty()) {
                toNotifyParent.addAll(getWatchers(parent));
            }
            watchers.removeAll(path);
        } finally {
            unlockIfLocked();
        }

        final String finalPath = path;
        executor.execute(() -> {
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


        executor.execute(() -> {
            try {
                lock();

                if (stopped) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                final Set<Watcher> toNotifyCreate = Sets.newHashSet();
                toNotifyCreate.addAll(getWatchers(path));

                final Set<Watcher> toNotifyParent = Sets.newHashSet();
                final String parent = path.substring(0, path.lastIndexOf("/"));
                if (!parent.isEmpty()) {
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
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                } else if (tree.containsKey(path)) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NODEEXISTS.intValue(), path, ctx, null);
                } else if (!parent.isEmpty() && !tree.containsKey(parent)) {
                    unlockIfLocked();
                    toNotifyParent.forEach(watcher -> watcher
                            .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected,
                                    parent)));
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                } else {
                    tree.put(name, createMockZNode(data, createMode));
                    watchers.removeAll(name);
                    unlockIfLocked();
                    cb.processResult(0, path, ctx, name);

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
                }
            } catch (Throwable ex) {
                log.error("create path : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            } finally {
                unlockIfLocked();
            }
        });

    }

    private MockZNode createMockZNode(byte[] data, CreateMode createMode) {
        return MockZNode.of(data, 0,
                createMode != null && createMode.isEphemeral() ? getSessionId() : NOT_EPHEMERAL);
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException {
        lock();
        try {
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
        } finally {
            unlockIfLocked();
        }
    }

    @Override
    public void getData(final String path, boolean watch, final DataCallback cb, final Object ctx) {
        executor.execute(() -> {
            try {
                checkReadOpDelay();
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET, path);
                if (failure.isPresent()) {
                    cb.processResult(failure.get().intValue(), path, ctx, null, null);
                    return;
                } else if (stopped) {
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                    return;
                }

                MockZNode value;
                Stat stat;
                lock();
                try {
                    value = tree.get(path);
                    stat = value.getStat();
                } finally {
                    unlockIfLocked();
                }

                if (value == null) {
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                } else {
                    cb.processResult(0, path, ctx, value.getContent(), stat);
                }
            } catch (Throwable ex) {
                log.error("get data : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            }
        });
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
        executor.execute(() -> {
            checkReadOpDelay();
            try {
                lock();
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET, path);
                if (failure.isPresent()) {
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx, null, null);
                    return;
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                    return;
                }

                MockZNode value = tree.get(path);
                if (value == null) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                } else {
                    if (watcher != null) {
                        watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                    }
                    Stat stat = value.getStat();
                    unlockIfLocked();
                    cb.processResult(0, path, ctx, value.getContent(), stat);
                }
            } catch (Throwable ex) {
                log.error("get data : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            } finally {
                unlockIfLocked();
            }
        });
    }

    @Override
    public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
        executor.execute(() -> {
            List<String> children = Lists.newArrayList();
            try {
                lock();
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
                if (failure.isPresent()) {
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                    return;
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                if (!tree.containsKey(path)) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                    return;
                }

                for (String item : tree.tailMap(path).keySet()) {
                    if (!item.startsWith(path)) {
                        break;
                    } else {
                        if (path.length() >= item.length()) {
                            continue;
                        }

                        String child = item.substring(path.length() + 1);
                        if (item.charAt(path.length()) == '/' && !child.contains("/")) {
                            children.add(child);
                        }
                    }
                }

                if (watcher != null) {
                    watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                }
                cb.processResult(0, path, ctx, children);
            } catch (Throwable ex) {
                log.error("get children : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            } finally {
                unlockIfLocked();
            }

        });
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
        lock();
        try {
            maybeThrowProgrammedFailure(Op.GET_CHILDREN, path);

            if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            String firstKey = path.equals("/") ? path : path + "/";
            String lastKey = path.equals("/") ? "0" : path + "0"; // '0' is lexicographically just after '/'

            Set<String> children = new TreeSet<>();
            tree.subMap(firstKey, false, lastKey, false).forEach((key, value) -> {
                String relativePath = key.replace(firstKey, "");

                // Only return first-level children
                String child = relativePath.split("/", 2)[0];
                children.add(child);
            });

            if (watcher != null) {
                watchers.put(path, new NodeWatcher(watcher, getSessionId()));
            }

            return new ArrayList<>(children);
        } finally {
            unlockIfLocked();
        }
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        lock();
        try {
            maybeThrowProgrammedFailure(Op.GET_CHILDREN, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            } else if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            String firstKey = path.equals("/") ? path : path + "/";
            String lastKey = path.equals("/") ? "0" : path + "0"; // '0' is lexicographically just after '/'

            Set<String> children = new TreeSet<>();
            tree.subMap(firstKey, false, lastKey, false).forEach((key, value) -> {
                String relativePath = key.replace(firstKey, "");

                // Only return first-level children
                String child = relativePath.split("/", 2)[0];
                children.add(child);
            });

            return new ArrayList<>(children);
        } finally {
            unlockIfLocked();
        }
    }

    @Override
    public void getChildren(final String path, boolean watcher, final Children2Callback cb, final Object ctx) {
        executor.execute(() -> {
            Set<String> children = new TreeSet<>();
            try {
                lock();
                MockZNode mockZNode = tree.get(path);
                Stat stat = mockZNode != null ? mockZNode.getStat() : null;
                Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
                if (failure.isPresent()) {
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx, null, null);
                    return;
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                    return;
                } else if (mockZNode == null) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                    return;
                }

                String firstKey = path.equals("/") ? path : path + "/";
                String lastKey = path.equals("/") ? "0" : path + "0"; // '0' is lexicographically just after '/'

                tree.subMap(firstKey, false, lastKey, false).forEach((key, value) -> {
                    String relativePath = key.replace(firstKey, "");
                    // Only return first-level children
                    String child = relativePath.split("/", 2)[0];
                    children.add(child);
                });
                unlockIfLocked();
                cb.processResult(0, path, ctx, new ArrayList<>(children), stat);
            } catch (Throwable ex) {
                log.error("get children : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            }
        });

    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        lock();
        try {
            maybeThrowProgrammedFailure(Op.EXISTS, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            }

            if (tree.containsKey(path)) {
                return tree.get(path).getStat();
            } else {
                return null;
            }
        } finally {
            unlockIfLocked();
        }
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        lock();
        try {
            maybeThrowProgrammedFailure(Op.EXISTS, path);

            if (stopped) {
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
        } finally {
            unlockIfLocked();
        }
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path, null, cb, ctx);
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        executor.execute(() -> {
            try {
                lock();
                Optional<KeeperException.Code> failure = programmedFailure(Op.EXISTS, path);
                if (failure.isPresent()) {
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx, null);
                    return;
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                    return;
                }

                if (watcher != null) {
                    watchers.put(path, new NodeWatcher(watcher, getSessionId()));
                }

                MockZNode mockZNode = tree.get(path);
                if (mockZNode != null) {
                    Stat stat = mockZNode.getStat();
                    unlockIfLocked();
                    cb.processResult(0, path, ctx, stat);
                } else {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                }
            } catch (Throwable ex) {
                log.error("exist : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            } finally {
                unlockIfLocked();
            }
        });
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        executor.execute(() -> {
            Optional<KeeperException.Code> failure = programmedFailure(Op.SYNC, path);
            if (failure.isPresent()) {
                cb.processResult(failure.get().intValue(), path, ctx);
                return;
            } else if (stopped) {
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                return;
            }

            cb.processResult(0, path, ctx);
        });

    }

    @Override
    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        final Set<Watcher> toNotify = Sets.newHashSet();
        lock();
        Stat stat;
        try {
            maybeThrowProgrammedFailure(Op.SET, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            }

            if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            MockZNode mockZNode = tree.get(path);
            int currentVersion = mockZNode.getVersion();

            // Check version
            if (version != -1 && version != currentVersion) {
                throw new KeeperException.BadVersionException(path);
            }

            log.debug("[{}] Updating -- current version: {}", path, currentVersion);
            mockZNode.updateData(data);
            stat = mockZNode.getStat();
            toNotify.addAll(getWatchers(path));
            watchers.removeAll(path);
        } finally {
            unlockIfLocked();
        }

        executor.execute(() -> {
            triggerPersistentWatches(path, null, EventType.NodeDataChanged);

            toNotify.forEach(watcher -> watcher
                    .process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path)));
        });

        return stat;
    }

    @Override
    public void setData(final String path, final byte[] data, int version, final StatCallback cb, final Object ctx) {
        if (stopped) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }

        executor.execute(() -> {
            try {
                final Set<Watcher> toNotify = Sets.newHashSet();
                lock();
                Stat stat;
                try {
                    Optional<KeeperException.Code> failure = programmedFailure(Op.SET, path);
                    if (failure.isPresent()) {
                        unlockIfLocked();
                        cb.processResult(failure.get().intValue(), path, ctx, null);
                        return;
                    } else if (stopped) {
                        unlockIfLocked();
                        cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                        return;
                    }

                    if (!tree.containsKey(path)) {
                        unlockIfLocked();
                        cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                        return;
                    }

                    MockZNode mockZNode = tree.get(path);
                    int currentVersion = mockZNode.getVersion();

                    // Check version
                    if (version != -1 && version != currentVersion) {
                        log.debug("[{}] Current version: {} -- Expected: {}", path, currentVersion, version);
                        Stat currentStat = mockZNode.getStat();
                        unlockIfLocked();
                        cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx, currentStat);
                        return;
                    }

                    log.debug("[{}] Updating -- current version: {}", path, currentVersion);
                    mockZNode.updateData(data);
                    stat = mockZNode.getStat();
                } finally {
                    unlockIfLocked();
                }
                cb.processResult(0, path, ctx, stat);

                toNotify.addAll(getWatchers(path));
                watchers.removeAll(path);

                for (Watcher watcher : toNotify) {
                    watcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path));
                }

                triggerPersistentWatches(path, null, EventType.NodeDataChanged);
            } catch (Throwable ex) {
                log.error("Update data : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            }
        });
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        maybeThrowProgrammedFailure(Op.DELETE, path);

        final Set<Watcher> toNotifyDelete;
        final Set<Watcher> toNotifyParent;
        final String parent;

        lock();
        try {
            if (stopped) {
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

            tree.remove(path);

            toNotifyDelete = Sets.newHashSet();
            toNotifyDelete.addAll(getWatchers(path));

            toNotifyParent = Sets.newHashSet();
            parent = path.substring(0, path.lastIndexOf("/"));
            if (!parent.isEmpty()) {
                toNotifyParent.addAll(getWatchers(parent));
            }

            watchers.removeAll(path);
        } finally {
            unlockIfLocked();
        }

        executor.execute(() -> {
            if (stopped) {
                return;
            }

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
        Runnable r = () -> {
            try {
                lock();
                final Set<Watcher> toNotifyDelete = Sets.newHashSet();
                toNotifyDelete.addAll(getWatchers(path));

                final Set<Watcher> toNotifyParent = Sets.newHashSet();
                final String parent = path.substring(0, path.lastIndexOf('/'));
                if (!parent.isEmpty()) {
                    toNotifyParent.addAll(getWatchers(parent));
                }
                watchers.removeAll(path);

                Optional<KeeperException.Code> failure = programmedFailure(Op.DELETE, path);
                if (failure.isPresent()) {
                    unlockIfLocked();
                    cb.processResult(failure.get().intValue(), path, ctx);
                } else if (stopped) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
                } else if (!tree.containsKey(path)) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx);
                } else if (hasChildren(path)) {
                    unlockIfLocked();
                    cb.processResult(KeeperException.Code.NOTEMPTY.intValue(), path, ctx);
                } else {
                    if (version != -1) {
                        int currentVersion = tree.get(path).getVersion();
                        if (version != currentVersion) {
                            unlockIfLocked();
                            cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx);
                            return;
                        }
                    }

                    tree.remove(path);

                    unlockIfLocked();
                    cb.processResult(0, path, ctx);

                    toNotifyDelete.forEach(watcher -> watcher
                            .process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path)));
                    toNotifyParent.forEach(watcher -> watcher
                            .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected,
                                    parent)));
                    triggerPersistentWatches(path, parent, EventType.NodeDeleted);
                }
            } catch (Throwable ex) {
                log.error("delete path : {} error", path, ex);
                cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx);
            } finally {
                unlockIfLocked();
            }
        };

        try {
            executor.execute(r);
        } catch (RejectedExecutionException ree) {
            cb.processResult(KeeperException.Code.SESSIONEXPIRED.intValue(), path, ctx);
        }

    }

    @Override
    public void multi(Iterable<org.apache.zookeeper.Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        try {
            List<OpResult> res = multi(ops);
            cb.processResult(KeeperException.Code.OK.intValue(), null, ctx, res);
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.APIERROR.intValue(), null, ctx, null);
        }
    }

    @Override
    public List<OpResult> multi(Iterable<org.apache.zookeeper.Op> ops) throws InterruptedException, KeeperException {
        List<OpResult> res = new ArrayList<>();
        try {
            for (org.apache.zookeeper.Op op : ops) {
                switch (op.getType()) {
                    case ZooDefs.OpCode.create -> {
                        org.apache.zookeeper.Op.Create opc = ((org.apache.zookeeper.Op.Create) op);
                        CreateMode cm = CreateMode.fromFlag(opc.flags);
                        String path = create(op.getPath(), opc.data, null, cm);
                        res.add(new OpResult.CreateResult(path));
                    }
                    case ZooDefs.OpCode.delete -> {
                        delete(op.getPath(), (int) FieldUtils.readField(op, "version", true));
                        res.add(new OpResult.DeleteResult());
                    }
                    case ZooDefs.OpCode.setData -> {
                        Stat stat = setData(
                                op.getPath(),
                                (byte[]) FieldUtils.readField(op, "data", true),
                                (int) FieldUtils.readField(op, "version", true));
                        res.add(new OpResult.SetDataResult(stat));
                    }
                    case ZooDefs.OpCode.getChildren -> {
                        try {
                            List<String> children = getChildren(op.getPath(), null);
                            res.add(new OpResult.GetChildrenResult(children));
                        } catch (KeeperException e) {
                            res.add(new OpResult.ErrorResult(e.code().intValue()));
                        }
                    }
                    case ZooDefs.OpCode.getData -> {
                        Stat stat = new Stat();
                        try {
                            byte[] payload = getData(op.getPath(), null, stat);
                            res.add(new OpResult.GetDataResult(payload, stat));
                        } catch (KeeperException e) {
                            res.add(new OpResult.ErrorResult(e.code().intValue()));
                        }
                    }
                }
            }
        } catch (KeeperException e) {
            res.add(new OpResult.ErrorResult(e.code().intValue()));
            int total = Iterables.size(ops);
            for (int i = res.size(); i < total; i++) {
                res.add(new OpResult.ErrorResult(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue()));
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return res;
    }

    @Override
    public synchronized void addWatch(String basePath, Watcher watcher, AddWatchMode mode) {
        persistentWatchers.add(new PersistentWatcher(basePath, watcher, mode, getSessionId()));
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        if (stopped) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), basePath, ctx);
            return;
        }
        long currentSessionId = getSessionId();
        executor.execute(() -> {
            synchronized (MockZooKeeper.this) {
                persistentWatchers.add(new PersistentWatcher(basePath, watcher, mode, currentSessionId));
            }
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
        lock();
        try {
            stopped = true;
            tree.clear();
            watchers.clear();
            try {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                log.error("MockZooKeeper shutdown had error", ex);
            }
        } finally {
            unlockIfLocked();
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

    private boolean hasChildren(String path) {
        return !tree.subMap(path + '/', path + '0').isEmpty();
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
        getPersistentWatchersCopy().forEach(w -> {
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

    private synchronized List<PersistentWatcher> getPersistentWatchersCopy() {
        return new ArrayList<>(persistentWatchers);
    }

    public void deleteEphemeralNodes(long sessionId) {
        if (sessionId != NOT_EPHEMERAL) {
            lock();
            try {
                tree.values().removeIf(zNode -> zNode.getEphemeralOwner() == sessionId);
            } finally {
                unlockIfLocked();
            }
        }
    }

    public synchronized void deleteWatchers(long sessionId) {
        lock();
        try {
            // remove all persistent watchers for the session
            persistentWatchers.removeIf(w -> w.sessionId == sessionId);
            // remove all watchers for the session
            List<Map.Entry<String, NodeWatcher>> watchersForSession =
                    watchers.entries().stream().filter(e -> e.getValue().sessionId == sessionId).toList();
            watchersForSession
                    .forEach(e -> watchers.remove(e.getKey(), e.getValue()));
        } finally {
            unlockIfLocked();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MockZooKeeper.class);
}
