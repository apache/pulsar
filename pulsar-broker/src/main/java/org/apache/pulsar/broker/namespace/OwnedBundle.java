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
package org.apache.pulsar.broker.namespace;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;

@EqualsAndHashCode
@ToString
@CustomLog
public class OwnedBundle {
    private final NamespaceBundle bundle;

    /**
     * The resource lock acquired for this ownership. Ties this instance to a single ownership generation: a
     * bundle can be re-acquired (new lock, new {@link OwnedBundle}) after this instance's lock expired, and
     * cleanup done through this instance must never touch the newer generation.
     */
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final ResourceLock<NamespaceEphemeralData> resourceLock;

    /**
     * {@link #nsLock} is used to protect read/write access to {@link #isActive} flag and the corresponding code section
     * based on {@link #isActive} flag.
     */
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final ReentrantReadWriteLock nsLock = new ReentrantReadWriteLock();
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<OwnedBundle> IS_ACTIVE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(OwnedBundle.class, "isActive");
    private volatile int isActive = TRUE;

    /**
     * constructor.
     *
     * @param suName
     */
    public OwnedBundle(NamespaceBundle suName) {
        this(suName, true);
    }

    /**
     * Constructor to allow set initial active flag.
     *
     * @param suName
     * @param active
     */
    public OwnedBundle(NamespaceBundle suName, boolean active) {
        this.bundle = suName;
        this.resourceLock = null;
        IS_ACTIVE_UPDATER.set(this, active ? TRUE : FALSE);
    }

    OwnedBundle(NamespaceBundle suName, ResourceLock<NamespaceEphemeralData> resourceLock) {
        this.bundle = suName;
        this.resourceLock = resourceLock;
        IS_ACTIVE_UPDATER.set(this, TRUE);
    }

    ResourceLock<NamespaceEphemeralData> getResourceLock() {
        return resourceLock;
    }

    /**
     * Access to the namespace name.
     *
     * @return NamespaceName
     */
    public NamespaceBundle getNamespaceBundle() {
        return this.bundle;
    }

    /**
     * It unloads the bundle by closing all topics concurrently under this bundle.
     *
     * <pre>
     * a. disable bundle ownership in memory and not in zk
     * b. close all the topics concurrently
     * c. delete ownership znode from zookeeper.
     * </pre>
     *
     * @param pulsar
     * @param timeout
     *            timeout for unloading bundle. It doesn't throw exception if it times out while waiting on closing all
     *            topics
     * @param timeoutUnit
     * @throws Exception
     */
    public CompletableFuture<Void> handleUnloadRequest(PulsarService pulsar, long timeout, TimeUnit timeoutUnit) {
        return handleUnloadRequest(pulsar, timeout, timeoutUnit, true);
    }

    public CompletableFuture<Void> handleUnloadRequest(PulsarService pulsar, long timeout, TimeUnit timeoutUnit,
                                                       boolean closeWithoutWaitingClientDisconnect) {
        long unloadBundleStartTime = System.nanoTime();
        // Need a per namespace ReentrantReadWriteLock
        // Here to do a writeLock to set the flag and proceed to check and close connections
        try {
            while (!this.nsLock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                // Using tryLock to avoid deadlocks caused by 2 threads trying to acquire 2 readlocks (eg: replicators)
                // while a handleUnloadRequest happens in the middle
                log.warn("Contention on OwnedBundle rw lock. Retrying to acquire lock write lock");
            }

            try {
                // set the flag locally s.t. no more producer/consumer to this namespace is allowed
                if (!IS_ACTIVE_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // An exception is thrown when the namespace is not in active state (i.e. another thread is
                    // removing/have removed it)
                    return FutureUtil.failedFuture(new IllegalStateException(
                            "Namespace is not active. ns:" + this.bundle + "; state:" + IS_ACTIVE_UPDATER.get(this)));
                }
            } finally {
                // no matter success or not, unlock
                this.nsLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            return FutureUtil.failedFuture(e);
        }

        AtomicInteger unloadedTopics = new AtomicInteger();
        log.info().attr("ownership", this.bundle).log("Disabling ownership");

        // Capture the topic futures for this bundle once, before unloading starts, and reuse the same snapshot
        // both to close the topics below and to clean up afterward: if this generation's lock has already expired
        // and the bundle is re-acquired while the close futures are still running, the cleanup step must only
        // touch what this snapshot observed, never a newer generation's topic.
        //
        // A straggler topic loaded into BrokerService's topic cache for this bundle *after* the snapshot is
        // taken is deliberately left alone: neither closed below nor evicted by the cleanup. The alternative —
        // force-evicting whatever is in the cache at cleanup time regardless of identity — could yank a topic
        // that was never closed, letting a second, independent instance for the same name be created on the
        // next load. A straggler surviving the unload does not need this unload to fix it: new connections
        // converge through the normal lookup path once ownership has actually moved, and BookKeeper's ledger
        // fencing (see ManagedLedgerImpl#addEntryFailedDueToConcurrentlyModified) is what prevents it from
        // silently double-writing if a new owner's ManagedLedger instance does start writing the same topic.
        Map<String, CompletableFuture<Optional<Topic>>> topicFutures =
                pulsar.getBrokerService().getTopicFuturesInBundle(bundle);

        // close topics forcefully
        // isActive was already flipped to false above; looking the bundle up in the ownership cache here could
        // deactivate a newer OwnedBundle that re-acquired the bundle after this instance's lock expired.
        return pulsar.getBrokerService().unloadServiceUnit(
                        bundle, true, closeWithoutWaitingClientDisconnect, timeout, timeoutUnit, topicFutures)
                .handle((numUnloadedTopics, ex) -> {
                    if (ex != null) {
                        // ignore topic-close failure to unload bundle
                        log.error()
                                .attr("namespace", bundle.toString())
                                .exception(ex)
                                .log("Failed to close topics under namespace");
                    } else {
                        unloadedTopics.set(numUnloadedTopics);
                    }
                    // clean up topics that failed to unload from the broker ownership cache
                    pulsar.getBrokerService().cleanUnloadedTopicFromCache(bundle, topicFutures);
                    return null;
                })
                .thenCompose(v -> {
                    // delete ownership node on zk, but only for this instance's ownership generation
                    return pulsar.getNamespaceService().getOwnershipCache().removeOwnership(this);
                }).whenComplete((ignored, ex) -> {
                    double unloadBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - unloadBundleStartTime));
                    log.info()
                            .attr("bundle", this.bundle)
                            .attr("unloadedTopics", unloadedTopics)
                            .attr("unloadBundleTimeMs", unloadBundleTime)
                            .exception(ex)
                            .log("Unloading namespace-bundle completed");
                });
    }

    /**
     * Access method to the namespace state to check whether the namespace is active or not.
     *
     * @return boolean value indicate that the namespace is active or not.
     */
    public boolean isActive() {
        return IS_ACTIVE_UPDATER.get(this) == TRUE;
    }

    public void setActive(boolean active) {
        IS_ACTIVE_UPDATER.set(this, active ? TRUE : FALSE);
    }
}
