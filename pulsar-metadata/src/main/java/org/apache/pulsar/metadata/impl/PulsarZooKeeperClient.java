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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Provide a zookeeper client to handle session expire.
 */
@Slf4j
public class PulsarZooKeeperClient extends ZooKeeper implements Watcher, AutoCloseable {

    private static final int DEFAULT_RETRY_EXECUTOR_THREAD_COUNT = 1;

    // ZooKeeper client connection variables
    private final String connectString;
    private final int sessionTimeoutMs;
    private final boolean allowReadOnlyMode;

    // state for the zookeeper client
    private final AtomicReference<ZooKeeper> zk = new AtomicReference<ZooKeeper>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ZooKeeperWatcherBase watcherManager;

    private final ScheduledExecutorService retryExecutor;
    private final ExecutorService connectExecutor;

    // rate limiter
    private final RateLimiter rateLimiter;

    // retry polices
    private final RetryPolicy connectRetryPolicy;
    private final RetryPolicy operationRetryPolicy;

    // Stats Logger
    private final OpStatsLogger createStats;
    private final OpStatsLogger getStats;
    private final OpStatsLogger setStats;
    private final OpStatsLogger deleteStats;
    private final OpStatsLogger getChildrenStats;
    private final OpStatsLogger existsStats;
    private final OpStatsLogger multiStats;
    private final OpStatsLogger getACLStats;
    private final OpStatsLogger setACLStats;
    private final OpStatsLogger syncStats;
    private final OpStatsLogger createClientStats;

    private final Callable<ZooKeeper> clientCreator = new Callable<ZooKeeper>() {

        @Override
        public ZooKeeper call() throws Exception {
            try {
                return ZooWorker.syncCallWithRetries(null, new ZooWorker.ZooCallable<ZooKeeper>() {

                    @Override
                    public ZooKeeper call() throws KeeperException, InterruptedException {
                        log.info("Reconnecting zookeeper {}.", connectString);
                        // close the previous one
                        closeZkHandle();
                        ZooKeeper newZk;
                        try {
                            newZk = createZooKeeper();
                        } catch (IOException ie) {
                            log.error("Failed to create zookeeper instance to " + connectString, ie);
                            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
                        }
                        waitForConnection();
                        zk.set(newZk);
                        log.info("ZooKeeper session {} is created to {}.",
                                Long.toHexString(newZk.getSessionId()), connectString);
                        return newZk;
                    }

                    @Override
                    public String toString() {
                        return String.format("ZooKeeper Client Creator (%s)", connectString);
                    }

                }, connectRetryPolicy, rateLimiter, createClientStats);
            } catch (Exception e) {
                log.error("Gave up reconnecting to ZooKeeper : ", e);
                Runtime.getRuntime().exit(-1);
                return null;
            }
        }

    };

    @VisibleForTesting
    static PulsarZooKeeperClient createConnectedZooKeeperClient(
            String connectString, int sessionTimeoutMs, Set<Watcher> childWatchers,
            RetryPolicy operationRetryPolicy)
            throws KeeperException, InterruptedException, IOException {
        return PulsarZooKeeperClient.newBuilder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .watchers(childWatchers)
                .operationRetryPolicy(operationRetryPolicy)
                .build();
    }

    /**
     * A builder to build retryable zookeeper client.
     */
    public static class Builder {
        String connectString = null;
        int sessionTimeoutMs = 10000;
        Set<Watcher> watchers = null;
        RetryPolicy connectRetryPolicy = null;
        RetryPolicy operationRetryPolicy = null;
        StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        int retryExecThreadCount = DEFAULT_RETRY_EXECUTOR_THREAD_COUNT;
        double requestRateLimit = 0;
        boolean allowReadOnlyMode = false;

        private Builder() {}

        public Builder connectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public Builder sessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public Builder watchers(Set<Watcher> watchers) {
            this.watchers = watchers;
            return this;
        }

        public Builder connectRetryPolicy(RetryPolicy retryPolicy) {
            this.connectRetryPolicy = retryPolicy;
            return this;
        }

        public Builder operationRetryPolicy(RetryPolicy retryPolicy) {
            this.operationRetryPolicy = retryPolicy;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public Builder requestRateLimit(double requestRateLimit) {
            this.requestRateLimit = requestRateLimit;
            return this;
        }

        public Builder retryThreadCount(int numThreads) {
            this.retryExecThreadCount = numThreads;
            return this;
        }

        public Builder allowReadOnlyMode(boolean allowReadOnlyMode) {
            this.allowReadOnlyMode = allowReadOnlyMode;
            return this;
        }

        public PulsarZooKeeperClient build() throws IOException, KeeperException, InterruptedException {
            checkNotNull(connectString);
            checkArgument(sessionTimeoutMs > 0);
            checkNotNull(statsLogger);
            checkArgument(retryExecThreadCount > 0);

            if (null == connectRetryPolicy) {
                // Session expiry event is received by client only when zk quorum is well established.
                // All other connection loss retries happen at zk client library transparently.
                // Hence, we don't need to wait before retrying.
                connectRetryPolicy =
                        new BoundExponentialBackoffRetryPolicy(0, 0, Integer.MAX_VALUE);
            }
            if (null == operationRetryPolicy) {
                operationRetryPolicy =
                        new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, 0);
            }

            // Create a watcher manager
            StatsLogger watcherStatsLogger = statsLogger.scope("watcher");
            ZooKeeperWatcherBase watcherManager =
                    null == watchers ? new ZooKeeperWatcherBase(sessionTimeoutMs, watcherStatsLogger) :
                            new ZooKeeperWatcherBase(sessionTimeoutMs, watchers, watcherStatsLogger);
            PulsarZooKeeperClient client = new PulsarZooKeeperClient(
                    connectString,
                    sessionTimeoutMs,
                    watcherManager,
                    connectRetryPolicy,
                    operationRetryPolicy,
                    statsLogger,
                    retryExecThreadCount,
                    requestRateLimit,
                    allowReadOnlyMode
            );
            // Wait for connection to be established.
            try {
                watcherManager.waitForConnection();
            } catch (KeeperException ke) {
                client.close();
                throw ke;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                client.close();
                throw ie;
            }
            return client;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    protected PulsarZooKeeperClient(String connectString,
                              int sessionTimeoutMs,
                              ZooKeeperWatcherBase watcherManager,
                              RetryPolicy connectRetryPolicy,
                              RetryPolicy operationRetryPolicy,
                              StatsLogger statsLogger,
                              int retryExecThreadCount,
                              double rate,
                              boolean allowReadOnlyMode) throws IOException {
        super(connectString, sessionTimeoutMs, watcherManager, allowReadOnlyMode);
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.allowReadOnlyMode =  allowReadOnlyMode;
        this.watcherManager = watcherManager;
        this.connectRetryPolicy = connectRetryPolicy;
        this.operationRetryPolicy = operationRetryPolicy;
        this.rateLimiter = rate > 0 ? RateLimiter.create(rate) : null;
        this.retryExecutor =
                Executors.newScheduledThreadPool(retryExecThreadCount,
                        new ThreadFactoryBuilder().setNameFormat("ZKC-retry-executor-%d").build());
        this.connectExecutor =
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("ZKC-connect-executor-%d").build());
        // added itself to the watcher
        watcherManager.addChildWatcher(this);

        // Stats
        StatsLogger scopedStatsLogger = statsLogger.scope("zk");
        createClientStats = scopedStatsLogger.getOpStatsLogger("create_client");
        createStats = scopedStatsLogger.getOpStatsLogger("create");
        getStats = scopedStatsLogger.getOpStatsLogger("get_data");
        setStats = scopedStatsLogger.getOpStatsLogger("set_data");
        deleteStats = scopedStatsLogger.getOpStatsLogger("delete");
        getChildrenStats = scopedStatsLogger.getOpStatsLogger("get_children");
        existsStats = scopedStatsLogger.getOpStatsLogger("exists");
        multiStats = scopedStatsLogger.getOpStatsLogger("multi");
        getACLStats = scopedStatsLogger.getOpStatsLogger("get_acl");
        setACLStats = scopedStatsLogger.getOpStatsLogger("set_acl");
        syncStats = scopedStatsLogger.getOpStatsLogger("sync");
    }

    @Override
    public void close() throws InterruptedException {
        closed.set(true);
        connectExecutor.shutdown();
        retryExecutor.shutdown();
        closeZkHandle();
    }

    private void closeZkHandle() throws InterruptedException {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            super.close();
        } else {
            zkHandle.close();
        }
    }

    public void waitForConnection() throws KeeperException, InterruptedException {
        watcherManager.waitForConnection();
    }

    protected ZooKeeper createZooKeeper() throws IOException {
        return new ZooKeeper(connectString, sessionTimeoutMs, watcherManager, allowReadOnlyMode);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == EventType.None
                && event.getState() == KeeperState.Expired) {
            onExpired();
        }
    }

    private void onExpired() {
        if (closed.get()) {
            // we don't schedule any tries if the client is closed.
            return;
        }

        log.info("ZooKeeper session {} is expired from {}.",
                Long.toHexString(getSessionId()), connectString);
        try {
            connectExecutor.submit(clientCreator);
        } catch (RejectedExecutionException ree) {
            if (!closed.get()) {
                log.error("ZooKeeper reconnect task is rejected : ", ree);
            }
        } catch (Exception t) {
            log.error("Failed to submit zookeeper reconnect task due to runtime exception : ", t);
        }
    }

    /**
     * A runnable that retries zookeeper operations.
     */
    abstract static class ZkRetryRunnable implements Runnable {

        final ZooWorker worker;
        final RateLimiter rateLimiter;
        final Runnable that;

        ZkRetryRunnable(RetryPolicy retryPolicy,
                        RateLimiter rateLimiter,
                        OpStatsLogger statsLogger) {
            this.worker = new ZooWorker(retryPolicy, statsLogger);
            this.rateLimiter = rateLimiter;
            that = this;
        }

        @Override
        public void run() {
            if (null != rateLimiter) {
                rateLimiter.acquire();
            }
            zkRun();
        }

        abstract void zkRun();
    }

    // inherits from ZooKeeper client for all operations

    @Override
    public long getSessionId() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionId();
        }
        return zkHandle.getSessionId();
    }

    @Override
    public byte[] getSessionPasswd() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionPasswd();
        }
        return zkHandle.getSessionPasswd();
    }

    @Override
    public int getSessionTimeout() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.getSessionTimeout();
        }
        return zkHandle.getSessionTimeout();
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            super.addAuthInfo(scheme, auth);
            return;
        }
        zkHandle.addAuthInfo(scheme, auth);
    }

    private void backOffAndRetry(Runnable r, long nextRetryWaitTimeMs) {
        try {
            retryExecutor.schedule(r, nextRetryWaitTimeMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            if (!closed.get()) {
                log.error("ZooKeeper Operation {} is rejected : ", r, ree);
            }
        }
    }

    private boolean allowRetry(ZooWorker worker, int rc) {
        return worker.allowRetry(rc) && !closed.get();
    }

    @Override
    public synchronized void register(Watcher watcher) {
        watcherManager.addChildWatcher(watcher);
    }

    @Override
    public List<OpResult> multi(final Iterable<Op> ops) throws InterruptedException, KeeperException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<OpResult>>() {

            @Override
            public String toString() {
                return "multi";
            }

            @Override
            public List<OpResult> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.multi(ops);
                }
                return zkHandle.multi(ops);
            }

        }, operationRetryPolicy, rateLimiter, multiStats);
    }

    @Override
    public void multi(final Iterable<Op> ops,
                      final MultiCallback cb,
                      final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, createStats) {

            final MultiCallback multiCb = new MultiCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, List<OpResult> results) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, results);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.multi(ops, multiCb, worker);
                } else {
                    zkHandle.multi(ops, multiCb, worker);
                }
            }

            @Override
            public String toString() {
                return "multi";
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    @Deprecated
    public Transaction transaction() {
        // since there is no reference about which client that the transaction could use
        // so just use ZooKeeper instance directly.
        // you'd better to use {@link #multi}.
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return super.transaction();
        }
        return zkHandle.transaction();
    }

    @Override
    public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<ACL>>() {

            @Override
            public String toString() {
                return String.format("getACL (%s, stat = %s)", path, stat);
            }

            @Override
            public List<ACL> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getACL(path, stat);
                }
                return zkHandle.getACL(path, stat);
            }

        }, operationRetryPolicy, rateLimiter, getACLStats);
    }

    @Override
    public void getACL(final String path, final Stat stat, final ACLCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getACLStats) {

            final ACLCallback aclCb = new ACLCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, acl, stat);
                    }
                }

            };

            @Override
            public String toString() {
                return String.format("getACL (%s, stat = %s)", path, stat);
            }

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getACL(path, stat, aclCb, worker);
                } else {
                    zkHandle.getACL(path, stat, aclCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat setACL(final String path, final List<ACL> acl, final int version)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Stat>() {

            @Override
            public String toString() {
                return String.format("setACL (%s, acl = %s, version = %d)", path, acl, version);
            }

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.setACL(path, acl, version);
                }
                return zkHandle.setACL(path, acl, version);
            }

        }, operationRetryPolicy, rateLimiter, setACLStats);
    }

    @Override
    public void setACL(final String path, final List<ACL> acl, final int version,
                       final StatCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, setACLStats) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            public String toString() {
                return String.format("setACL (%s, acl = %s, version = %d)", path, acl, version);
            }

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.setACL(path, acl, version, stCb, worker);
                } else {
                    zkHandle.setACL(path, acl, version, stCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void sync(final String path, final VoidCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, syncStats) {

            final VoidCallback vCb = new VoidCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context);
                    }
                }

            };

            @Override
            public String toString() {
                return String.format("sync (%s)", path);
            }

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.sync(path, vCb, worker);
                } else {
                    zkHandle.sync(path, vCb, worker);
                }
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public States getState() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return PulsarZooKeeperClient.super.getState();
        } else {
            return zkHandle.getState();
        }
    }

    @Override
    public String toString() {
        ZooKeeper zkHandle = zk.get();
        if (null == zkHandle) {
            return PulsarZooKeeperClient.super.toString();
        } else {
            return zkHandle.toString();
        }
    }

    @Override
    public String create(final String path, final byte[] data,
                         final List<ACL> acl, final CreateMode createMode)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<String>() {

            @Override
            public String call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.create(path, data, acl, createMode);
                }
                return zkHandle.create(path, data, acl, createMode);
            }

            @Override
            public String toString() {
                return String.format("create (%s, acl = %s, mode = %s)", path, acl, createMode);
            }

        }, operationRetryPolicy, rateLimiter, createStats);
    }

    @Override
    public void create(final String path, final byte[] data, final List<ACL> acl,
                       final CreateMode createMode, final StringCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, createStats) {

            final StringCallback createCb = new StringCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, name);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.create(path, data, acl, createMode, createCb, worker);
                } else {
                    zkHandle.create(path, data, acl, createMode, createCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("create (%s, acl = %s, mode = %s)", path, acl, createMode);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void delete(final String path, final int version) throws KeeperException, InterruptedException {
        ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Void>() {

            @Override
            public Void call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.delete(path, version);
                } else {
                    zkHandle.delete(path, version);
                }
                return null;
            }

            @Override
            public String toString() {
                return String.format("delete (%s, version = %d)", path, version);
            }

        }, operationRetryPolicy, rateLimiter, deleteStats);
    }

    @Override
    public void delete(final String path, final int version, final VoidCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, deleteStats) {

            final VoidCallback deleteCb = new VoidCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.delete(path, version, deleteCb, worker);
                } else {
                    zkHandle.delete(path, version, deleteCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("delete (%s, version = %d)", path, version);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.exists(path, watcher);
                }
                return zkHandle.exists(path, watcher);
            }

            @Override
            public String toString() {
                return String.format("exists (%s, watcher = %s)", path, watcher);
            }

        }, operationRetryPolicy, rateLimiter, existsStats);
    }

    @Override
    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.exists(path, watch);
                }
                return zkHandle.exists(path, watch);
            }

            @Override
            public String toString() {
                return String.format("exists (%s, watcher = %s)", path, watch);
            }

        }, operationRetryPolicy, rateLimiter, existsStats);
    }

    @Override
    public void exists(final String path, final Watcher watcher, final StatCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, existsStats) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.exists(path, watcher, stCb, worker);
                } else {
                    zkHandle.exists(path, watcher, stCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("exists (%s, watcher = %s)", path, watcher);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void exists(final String path, final boolean watch, final StatCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, existsStats) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.exists(path, watch, stCb, worker);
                } else {
                    zkHandle.exists(path, watch, stCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("exists (%s, watcher = %s)", path, watch);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public byte[] getData(final String path, final Watcher watcher, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<byte[]>() {

            @Override
            public byte[] call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getData(path, watcher, stat);
                }
                return zkHandle.getData(path, watcher, stat);
            }

            @Override
            public String toString() {
                return String.format("getData (%s, watcher = %s)", path, watcher);
            }

        }, operationRetryPolicy, rateLimiter, getStats);
    }

    @Override
    public byte[] getData(final String path, final boolean watch, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<byte[]>() {

            @Override
            public byte[] call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getData(path, watch, stat);
                }
                return zkHandle.getData(path, watch, stat);
            }

            @Override
            public String toString() {
                return String.format("getData (%s, watcher = %s)", path, watch);
            }

        }, operationRetryPolicy, rateLimiter, getStats);
    }

    @Override
    public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getStats) {

            final DataCallback dataCb = new DataCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, data, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getData(path, watcher, dataCb, worker);
                } else {
                    zkHandle.getData(path, watcher, dataCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getData (%s, watcher = %s)", path, watcher);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getData(final String path, final boolean watch, final DataCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getStats) {

            final DataCallback dataCb = new DataCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, data, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getData(path, watch, dataCb, worker);
                } else {
                    zkHandle.getData(path, watch, dataCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getData (%s, watcher = %s)", path, watch);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public Stat setData(final String path, final byte[] data, final int version)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Stat>() {

            @Override
            public Stat call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.setData(path, data, version);
                }
                return zkHandle.setData(path, data, version);
            }

            @Override
            public String toString() {
                return String.format("setData (%s, version = %d)", path, version);
            }

        }, operationRetryPolicy, rateLimiter, setStats);
    }

    @Override
    public void setData(final String path, final byte[] data, final int version,
                        final StatCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, setStats) {

            final StatCallback stCb = new StatCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.setData(path, data, version, stCb, worker);
                } else {
                    zkHandle.setData(path, data, version, stCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("setData (%s, version = %d)", path, version);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode)
            throws KeeperException, InterruptedException {
        ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<Void>() {

            @Override
            public Void call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.addWatch(basePath, watcher, mode);
                } else {
                    zkHandle.addWatch(basePath, watcher, mode);
                }
                return null;
            }

            @Override
            public String toString() {
                return String.format("addWatch (%s, mode = %s)", basePath, mode);
            }

        }, operationRetryPolicy, rateLimiter, setStats);
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, VoidCallback cb, Object ctx) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, setStats) {

            final VoidCallback vCb = new VoidCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        vCb.processResult(rc, basePath, ctx);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.addWatch(basePath, watcher, mode, cb, ctx);
                } else {
                    zkHandle.addWatch(basePath, watcher, mode, cb, ctx);
                }
            }

            @Override
            public String toString() {
                return String.format("setData (%s, mode = %s)", basePath, mode.name());
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public List<String> getChildren(final String path, final Watcher watcher, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getChildren(path, watcher, stat);
                }
                return zkHandle.getChildren(path, watcher, stat);
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watcher);
            }

        }, operationRetryPolicy, rateLimiter, getChildrenStats);
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch, final Stat stat)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getChildren(path, watch, stat);
                }
                return zkHandle.getChildren(path, watch, stat);
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watch);
            }

        }, operationRetryPolicy, rateLimiter, getChildrenStats);
    }

    @Override
    public void getChildren(final String path, final Watcher watcher,
                            final Children2Callback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getChildrenStats) {

            final Children2Callback childCb = new Children2Callback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                                          List<String> children, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, children, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getChildren(path, watcher, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watcher, childCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watcher);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getChildren(final String path, final boolean watch, final Children2Callback cb,
                            final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getChildrenStats) {

            final Children2Callback childCb = new Children2Callback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                                          List<String> children, Stat stat) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, children, stat);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getChildren(path, watch, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watch, childCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watch);
            }
        };
        // execute it immediately
        proc.run();
    }


    @Override
    public List<String> getChildren(final String path, final Watcher watcher)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getChildren(path, watcher);
                }
                return zkHandle.getChildren(path, watcher);
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watcher);
            }

        }, operationRetryPolicy, rateLimiter, getChildrenStats);
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch)
            throws KeeperException, InterruptedException {
        return ZooWorker.syncCallWithRetries(this, new ZooWorker.ZooCallable<List<String>>() {

            @Override
            public List<String> call() throws KeeperException, InterruptedException {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    return PulsarZooKeeperClient.super.getChildren(path, watch);
                }
                return zkHandle.getChildren(path, watch);
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watch);
            }

        }, operationRetryPolicy, rateLimiter, getChildrenStats);
    }

    @Override
    public void getChildren(final String path, final Watcher watcher,
                            final ChildrenCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getChildrenStats) {

            final ChildrenCallback childCb = new ChildrenCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                                          List<String> children) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, children);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getChildren(path, watcher, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watcher, childCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watcher);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Override
    public void getChildren(final String path, final boolean watch,
                            final ChildrenCallback cb, final Object context) {
        final Runnable proc = new ZkRetryRunnable(operationRetryPolicy, rateLimiter, getChildrenStats) {

            final ChildrenCallback childCb = new ChildrenCallback() {

                @Override
                public void processResult(int rc, String path, Object ctx,
                                          List<String> children) {
                    ZooWorker worker = (ZooWorker) ctx;
                    if (allowRetry(worker, rc)) {
                        backOffAndRetry(that, worker.nextRetryWaitTime());
                    } else {
                        cb.processResult(rc, path, context, children);
                    }
                }

            };

            @Override
            void zkRun() {
                ZooKeeper zkHandle = zk.get();
                if (null == zkHandle) {
                    PulsarZooKeeperClient.super.getChildren(path, watch, childCb, worker);
                } else {
                    zkHandle.getChildren(path, watch, childCb, worker);
                }
            }

            @Override
            public String toString() {
                return String.format("getChildren (%s, watcher = %s)", path, watch);
            }
        };
        // execute it immediately
        proc.run();
    }

    @Slf4j
    static final class ZooWorker {
        int attempts = 0;
        long startTimeNanos;
        long elapsedTimeMs = 0L;
        final RetryPolicy retryPolicy;
        final OpStatsLogger statsLogger;

        ZooWorker(RetryPolicy retryPolicy, OpStatsLogger statsLogger) {
            this.retryPolicy = retryPolicy;
            this.statsLogger = statsLogger;
            this.startTimeNanos = MathUtils.nowInNano();
        }

        public boolean allowRetry(int rc) {
            elapsedTimeMs = MathUtils.elapsedMSec(startTimeNanos);
            if (!ZooWorker.isRecoverableException(rc)) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos),
                            TimeUnit.MICROSECONDS);
                } else {
                    statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos),
                            TimeUnit.MICROSECONDS);
                }
                return false;
            }
            ++attempts;
            return retryPolicy.allowRetry(attempts, elapsedTimeMs);
        }

        public long nextRetryWaitTime() {
            return retryPolicy.nextRetryWaitTime(attempts, elapsedTimeMs);
        }

        /**
         * Check whether the given result code is recoverable by retry.
         *
         * @param rc result code
         * @return true if given result code is recoverable.
         */
        public static boolean isRecoverableException(int rc) {
            return KeeperException.Code.CONNECTIONLOSS.intValue() == rc
                    || KeeperException.Code.OPERATIONTIMEOUT.intValue() == rc
                    || KeeperException.Code.SESSIONMOVED.intValue() == rc
                    || KeeperException.Code.SESSIONEXPIRED.intValue() == rc;
        }

        /**
         * Check whether the given exception is recoverable by retry.
         *
         * @param exception given exception
         * @return true if given exception is recoverable.
         */
        public static boolean isRecoverableException(KeeperException exception) {
            return isRecoverableException(exception.code().intValue());
        }

        interface ZooCallable<T> {
            /**
             * Be compatible with ZooKeeper interface.
             *
             * @return value
             * @throws InterruptedException
             * @throws KeeperException
             */
            T call() throws InterruptedException, KeeperException;
        }

        /**
         * Execute a sync zookeeper operation with a given retry policy.
         *
         * @param client
         *          ZooKeeper client.
         * @param proc
         *          Synchronous zookeeper operation wrapped in a {@link Callable}.
         * @param retryPolicy
         *          Retry policy to execute the synchronous operation.
         * @param rateLimiter
         *          Rate limiter for zookeeper calls
         * @param statsLogger
         *          Stats Logger for zookeeper client.
         * @return result of the zookeeper operation
         * @throws KeeperException any non-recoverable exception or recoverable exception exhausted all retires.
         * @throws InterruptedException the operation is interrupted.
         */
        public static<T> T syncCallWithRetries(PulsarZooKeeperClient client,
                                               ZooWorker.ZooCallable<T> proc,
                                               RetryPolicy retryPolicy,
                                               RateLimiter rateLimiter,
                                               OpStatsLogger statsLogger)
                throws KeeperException, InterruptedException {
            T result = null;
            boolean isDone = false;
            int attempts = 0;
            long startTimeNanos = MathUtils.nowInNano();
            while (!isDone) {
                try {
                    if (null != client) {
                        client.waitForConnection();
                    }
                    log.debug("Execute {} at {} retry attempt.", proc, attempts);
                    if (null != rateLimiter) {
                        rateLimiter.acquire();
                    }
                    result = proc.call();
                    isDone = true;
                    statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos),
                            TimeUnit.MICROSECONDS);
                } catch (KeeperException e) {
                    ++attempts;
                    boolean rethrow = true;
                    long elapsedTime = MathUtils.elapsedMSec(startTimeNanos);
                    if (((null != client && isRecoverableException(e)) || null == client)
                            && retryPolicy.allowRetry(attempts, elapsedTime)) {
                        rethrow = false;
                    }
                    if (rethrow) {
                        statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos),
                                TimeUnit.MICROSECONDS);
                        log.debug("Stopped executing {} after {} attempts.", proc, attempts);
                        throw e;
                    }
                    TimeUnit.MILLISECONDS.sleep(retryPolicy.nextRetryWaitTime(attempts, elapsedTime));
                }
            }
            return result;
        }

    }


}
