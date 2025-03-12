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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

/**
 * mock zookeeper with different session based on {@link MockZooKeeper}.
 */
public class MockZooKeeperSession extends ZooKeeper {

    private MockZooKeeper mockZooKeeper;

    private long sessionId = 1L;

    private static final Objenesis objenesis = new ObjenesisStd();

    private static final AtomicInteger sessionIdGenerator = new AtomicInteger(1000);

    private boolean closeMockZooKeeperOnClose;

    public static MockZooKeeperSession newInstance(MockZooKeeper mockZooKeeper) {
        return newInstance(mockZooKeeper, true);
    }

    public static MockZooKeeperSession newInstance(MockZooKeeper mockZooKeeper, boolean closeMockZooKeeperOnClose) {
        ObjectInstantiator<MockZooKeeperSession> instantiator = objenesis.getInstantiatorOf(MockZooKeeperSession.class);
        MockZooKeeperSession mockZooKeeperSession = instantiator.newInstance();

        mockZooKeeperSession.mockZooKeeper = mockZooKeeper;
        mockZooKeeperSession.sessionId = sessionIdGenerator.getAndIncrement();
        mockZooKeeperSession.closeMockZooKeeperOnClose = closeMockZooKeeperOnClose;
        if (closeMockZooKeeperOnClose) {
            mockZooKeeper.increaseRefCount();
        }
        return mockZooKeeperSession;
    }

    private MockZooKeeperSession(String quorum) throws Exception {
        // This constructor is never called
        super(quorum, 1, event -> {
        });
        assert false;
    }

    @Override
    public int getSessionTimeout() {
        return mockZooKeeper.getSessionTimeout();
    }

    @Override
    public States getState() {
        return mockZooKeeper.getState();
    }

    @Override
    public void register(Watcher watcher) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.register(watcher);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.create(path, data, acl, createMode);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl, CreateMode createMode,
                       final AsyncCallback.StringCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.create(path, data, acl, createMode, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.getData(path, watcher, stat);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void getData(final String path, boolean watch, final DataCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.getData(path, watch, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.getData(path, watcher, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.getChildren(path, watcher, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.getChildren(path, watcher);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.getChildren(path, watch);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void getChildren(final String path, boolean watcher, final Children2Callback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.getChildren(path, watcher, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.exists(path, watch);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.exists(path, watcher);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.exists(path, watch, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.exists(path, watcher, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.sync(path, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.setData(path, data, version);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void setData(final String path, final byte[] data, int version, final StatCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.setData(path, data, version, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.delete(path, version);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void delete(final String path, int version, final VoidCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.delete(path, version, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void multi(Iterable<org.apache.zookeeper.Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.multi(ops, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public List<OpResult> multi(Iterable<org.apache.zookeeper.Op> ops) throws InterruptedException, KeeperException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            return mockZooKeeper.multi(ops);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.addWatch(basePath, watcher, mode, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode)
            throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.addWatch(basePath, watcher, mode);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void addWatch(String basePath, AddWatchMode mode) throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.addWatch(basePath, mode);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void addWatch(String basePath, AddWatchMode mode, VoidCallback cb, Object ctx) {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.addWatch(basePath, mode, cb, ctx);
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    @Override
    public void close() throws InterruptedException {
        internalClose(false);
    }

    public void shutdown() throws InterruptedException {
        internalClose(true);
    }

    private void internalClose(boolean shutdown) throws InterruptedException {
        try {
            mockZooKeeper.overrideSessionId(getSessionId());
            mockZooKeeper.deleteEphemeralNodes(getSessionId());
            mockZooKeeper.deleteWatchers(getSessionId());
            if (closeMockZooKeeperOnClose) {
                if (shutdown) {
                    mockZooKeeper.shutdown();
                } else {
                    mockZooKeeper.close();
                }
            }
        } finally {
            mockZooKeeper.removeSessionIdOverride();
        }
    }

    Optional<KeeperException.Code> programmedFailure(MockZooKeeper.Op op, String path) {
        return mockZooKeeper.programmedFailure(op, path);
    }

    void maybeThrowProgrammedFailure(MockZooKeeper.Op op, String path) throws KeeperException {
        mockZooKeeper.maybeThrowProgrammedFailure(op, path);
    }

    public void failConditional(KeeperException.Code rc, BiPredicate<MockZooKeeper.Op, String> predicate) {
        mockZooKeeper.failConditional(rc, predicate);
    }

    public void setAlwaysFail(KeeperException.Code rc) {
        mockZooKeeper.setAlwaysFail(rc);
    }

    public void unsetAlwaysFail() {
        mockZooKeeper.unsetAlwaysFail();
    }

    public void setSessionId(long id) {
        sessionId = id;
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        return "MockZooKeeperSession{" + "sessionId=" + sessionId + '}';
    }
}
