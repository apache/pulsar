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
package org.apache.zookeeper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockZooKeeper extends ZooKeeper {
    private TreeMap<String, Pair<byte[], Integer>> tree;
    private SetMultimap<String, Watcher> watchers;
    private volatile boolean stopped;
    private AtomicReference<KeeperException.Code> alwaysFail;
    private CopyOnWriteArrayList<Failure> failures;
    private ExecutorService executor;

    private Watcher sessionWatcher;
    private long sessionId = 0L;
    private int readOpDelayMs;

    private ReentrantLock mutex;

    private AtomicLong sequentialIdGenerator;

    //see details of Objenesis caching - http://objenesis.org/details.html
    //see supported jvms - https://github.com/easymock/objenesis/blob/master/SupportedJVMs.md
    private static final Objenesis objenesis = new ObjenesisStd();

    public enum Op {
        CREATE, GET, SET, GET_CHILDREN, DELETE, EXISTS, SYNC,
    };

    private class Failure {
        final KeeperException.Code failReturnCode;
        final BiPredicate<Op, String> predicate;

        Failure(KeeperException.Code failReturnCode, BiPredicate<Op, String> predicate) {
            this.failReturnCode = failReturnCode;
            this.predicate = predicate;
        }
    }

    @Data
    @AllArgsConstructor
    private static class PersistentWatcher {
        final String path;
        final Watcher watcher;
        final AddWatchMode mode;
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
            ObjectInstantiator<MockZooKeeper> mockZooKeeperInstantiator =
                    new ObjenesisStd().getInstantiatorOf(MockZooKeeper.class);
            MockZooKeeper zk = (MockZooKeeper) mockZooKeeperInstantiator.newInstance();
            zk.init(executor);
            zk.readOpDelayMs = readOpDelayMs;
            zk.mutex = new ReentrantLock();
            zk.sequentialIdGenerator =  new AtomicLong();
            return zk;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    public static MockZooKeeper newInstance(ExecutorService executor, int readOpDelayMs) {
        try {
            ObjectInstantiator<MockZooKeeper> mockZooKeeperInstantiator = objenesis.getInstantiatorOf(MockZooKeeper.class);
            MockZooKeeper zk = (MockZooKeeper) mockZooKeeperInstantiator.newInstance();
            zk.init(executor);
            zk.readOpDelayMs = readOpDelayMs;
            zk.mutex = new ReentrantLock();
            ObjectInstantiator<ClientCnxn> clientCnxnObjectInstantiator = objenesis.getInstantiatorOf(ClientCnxn.class);
            Whitebox.setInternalState(zk, "cnxn", clientCnxnObjectInstantiator.newInstance());
            zk.sequentialIdGenerator =  new AtomicLong();
            return zk;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    private void init(ExecutorService executor) {
        tree = Maps.newTreeMap();
        if (executor != null) {
            this.executor = executor;
        } else {
            this.executor = Executors.newFixedThreadPool(1, new DefaultThreadFactory("mock-zookeeper"));
        }
        SetMultimap<String, Watcher> w = HashMultimap.create();
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
        super(quorum, 1, event -> {});
        assert false;
    }

    @Override
    public States getState() {
        return States.CONNECTED;
    }

    @Override
    public void register(Watcher watcher) {
        mutex.lock();
        sessionWatcher = watcher;
        mutex.unlock();
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        mutex.lock();

        final Set<Watcher> toNotifyCreate = Sets.newHashSet();
        final Set<Watcher> toNotifyParent = Sets.newHashSet();
        final String parent = path.substring(0, path.lastIndexOf("/"));

        try {
            maybeThrowProgrammedFailure(Op.CREATE, path);

            if (stopped)
                throw new KeeperException.ConnectionLossException();

            if (tree.containsKey(path)) {
                throw new KeeperException.NodeExistsException(path);
            }

            if (!parent.isEmpty() && !tree.containsKey(parent)) {
                throw new KeeperException.NoNodeException();
            }

            if (createMode == CreateMode.EPHEMERAL_SEQUENTIAL || createMode == CreateMode.PERSISTENT_SEQUENTIAL) {
                byte[] parentData = tree.get(parent).getLeft();
                int parentVersion = tree.get(parent).getRight();
                path = path + parentVersion;

                // Update parent version
                tree.put(parent, Pair.of(parentData, parentVersion + 1));
            }

            tree.put(path, Pair.of(data, 0));

            toNotifyCreate.addAll(watchers.get(path));

            if (!parent.isEmpty()) {
                toNotifyParent.addAll(watchers.get(parent));
            }
            watchers.removeAll(path);
        } finally {

            mutex.unlock();
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

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl, CreateMode createMode,
            final StringCallback cb, final Object ctx) {


        executor.execute(() -> {
            mutex.lock();

            if (stopped) {
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                return;
            }

            final Set<Watcher> toNotifyCreate = Sets.newHashSet();
            toNotifyCreate.addAll(watchers.get(path));

            final Set<Watcher> toNotifyParent = Sets.newHashSet();
            final String parent = path.substring(0, path.lastIndexOf("/"));
            if (!parent.isEmpty()) {
                toNotifyParent.addAll(watchers.get(parent));
            }

            final String name;
            if (createMode != null && createMode.isSequential()) {
                name = path + Long.toString(sequentialIdGenerator.getAndIncrement());
            } else {
                name = path;
            }

            Optional<KeeperException.Code> failure = programmedFailure(Op.CREATE, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null);
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            } else if (tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NODEEXISTS.intValue(), path, ctx, null);
            } else if (!parent.isEmpty() && !tree.containsKey(parent)) {
                mutex.unlock();
                toNotifyParent.forEach(watcher -> watcher
                        .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent)));
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
            } else {
                tree.put(name, Pair.of(data, 0));
                watchers.removeAll(name);
                mutex.unlock();
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
        });

    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException {
        mutex.lock();
        try {
            maybeThrowProgrammedFailure(Op.GET, path);
            Pair<byte[], Integer> value = tree.get(path);
            if (value == null) {
                throw new KeeperException.NoNodeException(path);
            } else {
                if (watcher != null) {
                    watchers.put(path, watcher);
                }
                if (stat != null) {
                    stat.setVersion(value.getRight());
                }
                return value.getLeft();
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void getData(final String path, boolean watch, final DataCallback cb, final Object ctx) {
        executor.execute(() -> {
            checkReadOpDelay();
            Optional<KeeperException.Code> failure = programmedFailure(Op.GET, path);
            if (failure.isPresent()) {
                cb.processResult(failure.get().intValue(), path, ctx, null, null);
                return;
            } else if (stopped) {
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                return;
            }

            Pair<byte[], Integer> value;
            mutex.lock();
            try {
                value = tree.get(path);
            } finally {
                mutex.unlock();
            }

            if (value == null) {
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
            } else {
                Stat stat = new Stat();
                stat.setVersion(value.getRight());
                cb.processResult(0, path, ctx, value.getLeft(), stat);
            }
        });
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
        executor.execute(() -> {
            checkReadOpDelay();
            mutex.lock();
            Optional<KeeperException.Code> failure = programmedFailure(Op.GET, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                return;
            }

            Pair<byte[], Integer> value = tree.get(path);
            if (value == null) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
            } else {
                if (watcher != null) {
                    watchers.put(path, watcher);
                }

                Stat stat = new Stat();
                stat.setVersion(value.getRight());
                mutex.unlock();
                cb.processResult(0, path, ctx, value.getLeft(), stat);
            }
        });
    }

    @Override
    public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
        executor.execute(() -> {
            mutex.lock();
            Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                return;
            }

            if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                return;
            }

            List<String> children = Lists.newArrayList();
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
                watchers.put(path, watcher);
            }
            mutex.unlock();

            cb.processResult(0, path, ctx, children);
        });
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
        mutex.lock();
        try {
            maybeThrowProgrammedFailure(Op.GET_CHILDREN, path);

            if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            List<String> children = Lists.newArrayList();
            for (String item : tree.tailMap(path).keySet()) {
                if (!item.startsWith(path)) {
                    break;
                } else {
                    if (path.length() >= item.length()) {
                        continue;
                    }

                    String child = item.substring(path.length() + 1);
                    if (!child.contains("/")) {
                        children.add(child);
                    }
                }
            }

            if (watcher != null) {
                watchers.put(path, watcher);
            }

            return children;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        mutex.lock();
        try {
            maybeThrowProgrammedFailure(Op.GET_CHILDREN, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            } else if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            List<String> children = Lists.newArrayList();
            for (String item : tree.tailMap(path).keySet()) {
                if (!item.startsWith(path)) {
                    break;
                } else {
                    if (path.length() >= item.length()) {
                        continue;
                    }
                    String child = item.substring(path.length());
                    if (child.indexOf("/") == 0) {
                        child = child.substring(1);
                        log.debug("child: '{}'", child);
                        if (!child.contains("/")) {
                            children.add(child);
                        }
                    }
                }
            }
            return children;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void getChildren(final String path, boolean watcher, final Children2Callback cb, final Object ctx) {
        executor.execute(() -> {
            mutex.lock();

            Optional<KeeperException.Code> failure = programmedFailure(Op.GET_CHILDREN, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
                return;
            } else if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, null);
                return;
            }

            log.debug("getChildren path={}", path);
            List<String> children = Lists.newArrayList();
            for (String item : tree.tailMap(path).keySet()) {
                log.debug("Checking path {}", item);
                if (!item.startsWith(path)) {
                    break;
                } else if (item.equals(path)) {
                    continue;
                } else {
                    String child = item.substring(path.length());
                    if (child.indexOf("/") == 0) {
                        child = child.substring(1);
                        log.debug("child: '{}'", child);
                        if (!child.contains("/")) {
                            children.add(child);
                        }
                    }
                }
            }

            log.debug("getChildren done path={} result={}", path, children);
            mutex.unlock();
            cb.processResult(0, path, ctx, children, new Stat());
        });

    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        mutex.lock();
        try {
            maybeThrowProgrammedFailure(Op.EXISTS, path);

            if (stopped)
                throw new KeeperException.ConnectionLossException();

            if (tree.containsKey(path)) {
                Stat stat = new Stat();
                stat.setVersion(tree.get(path).getRight());
                return stat;
            } else {
                return null;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        mutex.lock();
        try {
            maybeThrowProgrammedFailure(Op.EXISTS, path);

            if (stopped)
                throw new KeeperException.ConnectionLossException();

            if (watcher != null) {
                watchers.put(path, watcher);
            }

            if (tree.containsKey(path)) {
                Stat stat = new Stat();
                stat.setVersion(tree.get(path).getRight());
                return stat;
            } else {
                return null;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        executor.execute(() -> {
            mutex.lock();
            Optional<KeeperException.Code> failure = programmedFailure(Op.EXISTS, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                return;
            }

            if (tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(0, path, ctx, new Stat());
            } else {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
            }
        });
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        executor.execute(() -> {
            mutex.lock();
            Optional<KeeperException.Code> failure = programmedFailure(Op.EXISTS, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                return;
            }

            if (watcher != null) {
                watchers.put(path, watcher);
            }

            if (tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(0, path, ctx, new Stat());
            } else {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
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
        mutex.lock();

        final Set<Watcher> toNotify = Sets.newHashSet();
        int newVersion;

        try {
            maybeThrowProgrammedFailure(Op.SET, path);

            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            }

            if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException();
            }

            int currentVersion = tree.get(path).getRight();

            // Check version
            if (version != -1 && version != currentVersion) {
                throw new KeeperException.BadVersionException(path);
            }

            newVersion = currentVersion + 1;
            log.debug("[{}] Updating -- current version: {}", path, currentVersion);
            tree.put(path, Pair.of(data, newVersion));

            toNotify.addAll(watchers.get(path));
            watchers.removeAll(path);
        } finally {
            mutex.unlock();
        }

        executor.execute(() -> {
            triggerPersistentWatches(path, null, EventType.NodeDataChanged);

            toNotify.forEach(watcher -> watcher
                    .process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path)));
        });

        Stat stat = new Stat();
        stat.setVersion(newVersion);
        return stat;
    }

    @Override
    public void setData(final String path, final byte[] data, int version, final StatCallback cb, final Object ctx) {
        if (stopped) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            return;
        }

        executor.execute(() -> {
            final Set<Watcher> toNotify = Sets.newHashSet();

            mutex.lock();

            Optional<KeeperException.Code> failure = programmedFailure(Op.SET, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
                return;
            }

            if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
                return;
            }

            int currentVersion = tree.get(path).getRight();

            // Check version
            if (version != -1 && version != currentVersion) {
                log.debug("[{}] Current version: {} -- Expected: {}", path, currentVersion, version);
                mutex.unlock();
                cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx, null);
                return;
            }

            int newVersion = currentVersion + 1;
            log.debug("[{}] Updating -- current version: {}", path, currentVersion);
            tree.put(path, Pair.of(data, newVersion));
            Stat stat = new Stat();
            stat.setVersion(newVersion);

            mutex.unlock();
            cb.processResult(0, path, ctx, stat);

            toNotify.addAll(watchers.get(path));
            watchers.removeAll(path);

            for (Watcher watcher : toNotify) {
                watcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path));
            }

            triggerPersistentWatches(path, null, EventType.NodeDataChanged);
        });
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        maybeThrowProgrammedFailure(Op.DELETE, path);

        final Set<Watcher> toNotifyDelete;
        final Set<Watcher> toNotifyParent;
        final String parent;

        mutex.lock();
        try {
            if (stopped) {
                throw new KeeperException.ConnectionLossException();
            } else if (!tree.containsKey(path)) {
                throw new KeeperException.NoNodeException(path);
            } else if (hasChildren(path)) {
                throw new KeeperException.NotEmptyException(path);
            }

            if (version != -1) {
                int currentVersion = tree.get(path).getRight();
                if (version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
            }

            tree.remove(path);

            toNotifyDelete = Sets.newHashSet();
            toNotifyDelete.addAll(watchers.get(path));

            toNotifyParent = Sets.newHashSet();
            parent = path.substring(0, path.lastIndexOf("/"));
            if (!parent.isEmpty()) {
                toNotifyParent.addAll(watchers.get(parent));
            }

            watchers.removeAll(path);
        } finally {
            mutex.unlock();
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
            mutex.lock();
            final Set<Watcher> toNotifyDelete = Sets.newHashSet();
            toNotifyDelete.addAll(watchers.get(path));

            final Set<Watcher> toNotifyParent = Sets.newHashSet();
            final String parent = path.substring(0, path.lastIndexOf("/"));
            if (!parent.isEmpty()) {
                toNotifyParent.addAll(watchers.get(parent));
            }
            watchers.removeAll(path);

            Optional<KeeperException.Code> failure = programmedFailure(Op.DELETE, path);
            if (failure.isPresent()) {
                mutex.unlock();
                cb.processResult(failure.get().intValue(), path, ctx);
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx);
            } else if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx);
            } else if (hasChildren(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NOTEMPTY.intValue(), path, ctx);
            } else {
                if (version != -1) {
                    int currentVersion = tree.get(path).getRight();
                    if (version != currentVersion) {
                        mutex.unlock();
                        cb.processResult(KeeperException.Code.BADVERSION.intValue(), path, ctx);
                        return;
                    }
                }

                tree.remove(path);

                mutex.unlock();
                cb.processResult(0, path, ctx);

                toNotifyDelete.forEach(watcher -> watcher
                        .process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path)));
                toNotifyParent.forEach(watcher -> watcher
                        .process(new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent)));
                triggerPersistentWatches(path, parent, EventType.NodeDeleted);
            }
        };

        try {
            executor.execute(r);
        } catch (RejectedExecutionException ree) {
            cb.processResult(KeeperException.Code.SESSIONEXPIRED.intValue(), path, ctx);
            return;
        }

    }

    @Override
    public void multi(Iterable<org.apache.zookeeper.Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        try {
            List<OpResult> res = multi(ops);
            cb.processResult(KeeperException.Code.OK.intValue(), (String)null, ctx, res);
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.APIERROR.intValue(), (String)null, ctx, null);
        }
    }

    @Override
    public List<OpResult> multi(Iterable<org.apache.zookeeper.Op> ops) throws InterruptedException, KeeperException {
        List<OpResult> res = new ArrayList<>();
        for (org.apache.zookeeper.Op op : ops) {
            switch (op.getType()) {
                case ZooDefs.OpCode.create:
                    this.create(op.getPath(), ((org.apache.zookeeper.Op.Create)op).data, null, null);
                    res.add(new OpResult.CreateResult(op.getPath()));
                    break;
                case ZooDefs.OpCode.delete:
                    this.delete(op.getPath(), -1);
                    res.add(new OpResult.DeleteResult());
                    break;
                case ZooDefs.OpCode.setData:
                    this.create(op.getPath(), ((org.apache.zookeeper.Op.Create)op).data, null, null);
                    res.add(new OpResult.SetDataResult(null));
                    break;
                default:
            }
        }
        return res;
    }

    @Override
    public synchronized void addWatch(String basePath, Watcher watcher, AddWatchMode mode) {
        persistentWatchers.add(new PersistentWatcher(basePath, watcher, mode));
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        if (stopped) {
            cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), basePath, ctx);
            return;
        }

        executor.execute(() -> {
            synchronized (MockZooKeeper.this) {
                persistentWatchers.add(new PersistentWatcher(basePath, watcher, mode));
            }

            cb.processResult(KeeperException.Code.OK.intValue(), basePath, ctx);
        });
    }

    @Override
    public void close() throws InterruptedException {
    }

    public void shutdown() throws InterruptedException {
        mutex.lock();
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
            mutex.unlock();
        }
    }

    Optional<KeeperException.Code> programmedFailure(Op op, String path) {
        KeeperException.Code error = this.alwaysFail.get();
        if (error != KeeperException.Code.OK) {
            return Optional.of(error);
        }
        Optional<Failure> failure = failures.stream().filter(f -> f.predicate.test(op, path)).findFirst();
        if (failure.isPresent()) {
            failures.remove(failure.get());
            return Optional.of(failure.get().failReturnCode);
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

    public void setAlwaysFail(KeeperException.Code rc) {
        this.alwaysFail.set(rc);
    }

    public void unsetAlwaysFail() {
        this.alwaysFail.set(KeeperException.Code.OK);
    }

    public void setSessionId(long id) {
        sessionId = id;
    }

    @Override
    public long getSessionId() {
        return sessionId;
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
        persistentWatchers.forEach(w -> {
            if (w.mode == AddWatchMode.PERSISTENT_RECURSIVE) {
                if (path.startsWith(w.getPath())) {
                    w.watcher.process(new WatchedEvent(eventType, KeeperState.SyncConnected, path));
                }
            } else if (w.mode == AddWatchMode.PERSISTENT) {
                if (w.getPath().equals(path)) {
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

    private static final Logger log = LoggerFactory.getLogger(MockZooKeeper.class);
}
