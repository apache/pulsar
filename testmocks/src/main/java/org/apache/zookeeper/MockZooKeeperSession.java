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

    private long sessionId = 0L;

    private static final Objenesis objenesis = new ObjenesisStd();

    private static final AtomicInteger sessionIdGenerator = new AtomicInteger(1000);

    public static MockZooKeeperSession newInstance(MockZooKeeper mockZooKeeper) {
        ObjectInstantiator<MockZooKeeperSession> instantiator = objenesis.getInstantiatorOf(MockZooKeeperSession.class);
        MockZooKeeperSession mockZooKeeperSession = instantiator.newInstance();

        mockZooKeeperSession.mockZooKeeper = mockZooKeeper;
        mockZooKeeperSession.sessionId = sessionIdGenerator.getAndIncrement();
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
        mockZooKeeper.register(watcher);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        try {
            mockZooKeeper.overrideEpheralOwner(getSessionId());
            return mockZooKeeper.create(path, data, acl, createMode);
        } finally {
            mockZooKeeper.removeEpheralOwnerOverride();
        }
    }

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl, CreateMode createMode,
                       final AsyncCallback.StringCallback cb, final Object ctx) {
        try {
            mockZooKeeper.overrideEpheralOwner(getSessionId());
            mockZooKeeper.create(path, data, acl, createMode, cb, ctx);
        } finally {
            mockZooKeeper.removeEpheralOwnerOverride();
        }
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException {
        return mockZooKeeper.getData(path, watcher, stat);
    }

    @Override
    public void getData(final String path, boolean watch, final DataCallback cb, final Object ctx) {
        mockZooKeeper.getData(path, watch, cb, ctx);
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
        mockZooKeeper.getData(path, watcher, cb, ctx);
    }

    @Override
    public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
        mockZooKeeper.getChildren(path, watcher, cb, ctx);
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
        return mockZooKeeper.getChildren(path, watcher);
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return mockZooKeeper.getChildren(path, watch);
    }

    @Override
    public void getChildren(final String path, boolean watcher, final Children2Callback cb, final Object ctx) {
        mockZooKeeper.getChildren(path, watcher, cb, ctx);
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return mockZooKeeper.exists(path, watch);
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return mockZooKeeper.exists(path, watcher);
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        mockZooKeeper.exists(path, watch, cb, ctx);
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        mockZooKeeper.exists(path, watcher, cb, ctx);
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        mockZooKeeper.sync(path, cb, ctx);
    }

    @Override
    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return mockZooKeeper.setData(path, data, version);
    }

    @Override
    public void setData(final String path, final byte[] data, int version, final StatCallback cb, final Object ctx) {
        mockZooKeeper.setData(path, data, version, cb, ctx);
    }

    @Override
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        mockZooKeeper.delete(path, version);
    }

    @Override
    public void delete(final String path, int version, final VoidCallback cb, final Object ctx) {
        mockZooKeeper.delete(path, version, cb, ctx);
    }

    @Override
    public void multi(Iterable<org.apache.zookeeper.Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        mockZooKeeper.multi(ops, cb, ctx);
    }

    @Override
    public List<OpResult> multi(Iterable<org.apache.zookeeper.Op> ops) throws InterruptedException, KeeperException {
        return mockZooKeeper.multi(ops);
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        mockZooKeeper.addWatch(basePath, watcher, mode, cb, ctx);
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode)
            throws KeeperException, InterruptedException {
        mockZooKeeper.addWatch(basePath, watcher, mode);
    }

    @Override
    public void addWatch(String basePath, AddWatchMode mode) throws KeeperException, InterruptedException {
        mockZooKeeper.addWatch(basePath, mode);
    }

    @Override
    public void addWatch(String basePath, AddWatchMode mode, VoidCallback cb, Object ctx) {
        mockZooKeeper.addWatch(basePath, mode, cb, ctx);
    }

    @Override
    public void close() throws InterruptedException {
        mockZooKeeper.close();
    }

    public void shutdown() throws InterruptedException {
        mockZooKeeper.shutdown();
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
