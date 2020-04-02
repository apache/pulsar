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
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "deprecation", "restriction", "rawtypes" })
public class MockZooKeeper extends ZooKeeper {
    private TreeMap<String, Pair<byte[], Integer>> tree;
    private SetMultimap<String, Watcher> watchers;
    private volatile boolean stopped;
    private boolean alwaysFail = false;

    private ExecutorService executor;

    private AtomicInteger stepsToFail;
    private KeeperException.Code failReturnCode;
    private Watcher sessionWatcher;
    private long sessionId = 0L;
    private int readOpDelayMs;

    private ReentrantLock mutex;

    public static MockZooKeeper newInstance() {
        return newInstance(null);
    }

    public static MockZooKeeper newInstance(ExecutorService executor) {
        return newInstance(executor, -1);
    }

    public static MockZooKeeper newInstance(ExecutorService executor, int readOpDelayMs) {
        try {

            sun.reflect.ReflectionFactory rf = sun.reflect.ReflectionFactory.getReflectionFactory();
            Constructor objDef = Object.class.getDeclaredConstructor();
            Constructor intConstr = rf.newConstructorForSerialization(MockZooKeeper.class, objDef);
            MockZooKeeper zk = (MockZooKeeper) intConstr.newInstance();
            zk.init(executor);
            zk.readOpDelayMs = readOpDelayMs;
            zk.mutex = new ReentrantLock();
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
        stepsToFail = new AtomicInteger(-1);
        failReturnCode = KeeperException.Code.OK;
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
            checkProgrammedFail();

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

            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null);
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), path, ctx, null);
            } else if (tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NODEEXISTS.intValue(), path, ctx, null);
            } else if (!parent.isEmpty() && !tree.containsKey(parent)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
            } else {
                tree.put(path, Pair.of(data, 0));
                watchers.removeAll(path);
                mutex.unlock();
                cb.processResult(0, path, ctx, null);

                toNotifyCreate.forEach(
                        watcher -> watcher.process(
                                new WatchedEvent(EventType.NodeCreated,
                                                 KeeperState.SyncConnected,
                                                 path)));
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
            checkProgrammedFail();
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
            if (getProgrammedFailStatus()) {
                cb.processResult(failReturnCode.intValue(), path, ctx, null, null);
                return;
            } else if (stopped) {
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null, null);
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
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null, null);
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
            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null, null);
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
            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null);
                return;
            }

            if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null);
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
            checkProgrammedFail();

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
            checkProgrammedFail();

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
            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null, null);
                return;
            } else if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null, null);
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
            checkProgrammedFail();

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
            checkProgrammedFail();

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
            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null);
                return;
            }

            if (tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(0, path, ctx, new Stat());
            } else {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null);
            }
        });
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        executor.execute(() -> {
            mutex.lock();
            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null);
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
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null);
            }
        });
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        executor.execute(() -> {
            if (getProgrammedFailStatus()) {
                cb.processResult(failReturnCode.intValue(), path, ctx);
                return;
            } else if (stopped) {
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx);
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
            checkProgrammedFail();

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
            cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null);
            return;
        }

        executor.execute(() -> {
            final Set<Watcher> toNotify = Sets.newHashSet();

            mutex.lock();

            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx, null);
                return;
            } else if (stopped) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.ConnectionLoss, path, ctx, null);
                return;
            }

            if (!tree.containsKey(path)) {
                mutex.unlock();
                cb.processResult(KeeperException.Code.NoNode, path, ctx, null);
                return;
            }

            int currentVersion = tree.get(path).getRight();

            // Check version
            if (version != -1 && version != currentVersion) {
                log.debug("[{}] Current version: {} -- Expected: {}", path, currentVersion, version);
                mutex.unlock();
                cb.processResult(KeeperException.Code.BadVersion, path, ctx, null);
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
        });
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        checkProgrammedFail();

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
        });
    }

    @Override
    public void delete(final String path, int version, final VoidCallback cb, final Object ctx) {
        mutex.lock();
        if (executor.isShutdown()) {
            mutex.unlock();
            cb.processResult(KeeperException.Code.SESSIONEXPIRED.intValue(), path, ctx);
            return;
        }

        final Set<Watcher> toNotifyDelete = Sets.newHashSet();
        toNotifyDelete.addAll(watchers.get(path));

        final Set<Watcher> toNotifyParent = Sets.newHashSet();
        final String parent = path.substring(0, path.lastIndexOf("/"));
        if (!parent.isEmpty()) {
            toNotifyParent.addAll(watchers.get(parent));
        }

        executor.execute(() -> {
            mutex.lock();

            if (getProgrammedFailStatus()) {
                mutex.unlock();
                cb.processResult(failReturnCode.intValue(), path, ctx);
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
            }
        });

        watchers.removeAll(path);
        mutex.unlock();
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
            executor.shutdownNow();
        } finally {
            mutex.unlock();
        }
    }

    void checkProgrammedFail() throws KeeperException {
        if (stepsToFail.getAndDecrement() == 0 || this.alwaysFail) {
            throw KeeperException.create(failReturnCode);
        }
    }

    boolean getProgrammedFailStatus() {
        return stepsToFail.getAndDecrement() == 0;
    }

    public void failNow(KeeperException.Code rc) {
        failAfter(0, rc);
    }

    public void setAlwaysFail(KeeperException.Code rc) {
        this.alwaysFail = true;
        this.failReturnCode = rc;
    }

    public void unsetAlwaysFail() {
        this.alwaysFail = false;
    }

    public void failAfter(int steps, KeeperException.Code rc) {
        stepsToFail.set(steps);
        failReturnCode = rc;
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

    private static final Logger log = LoggerFactory.getLogger(MockZooKeeper.class);
}
