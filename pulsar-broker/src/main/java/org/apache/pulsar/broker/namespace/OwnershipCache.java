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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;

/**
 * This class provides a cache service for all the service unit ownership among the brokers. It provide a cache service
 * as well as MetadataStore read/write functions for a) lookup of a service unit ownership to a broker; b) take
 * ownership of a service unit by the local broker
 */
@CustomLog
public class OwnershipCache {
    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner.
     */
    private final String ownerBrokerUrl;

    /**
     * The local broker URL that this <code>OwnershipCache</code> will set as owner.
     */
    private final String ownerBrokerUrlTls;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner.
     */
    private NamespaceEphemeralData selfOwnerInfo;

    /**
     * The NamespaceEphemeralData objects that can be associated with the current owner, when the broker is disabled.
     */
    private NamespaceEphemeralData selfOwnerInfoDisabled;

    private final LockManager<NamespaceEphemeralData> lockManager;

    private final Map<NamespaceBundle, ResourceLock<NamespaceEphemeralData>> locallyAcquiredLocks;

    /**
     * Serializes, per bundle, this cache's own acquire ({@link #tryAcquiringOwnership(NamespaceBundle)}) against
     * both of its release paths: the generation-agnostic {@link #removeOwnership(NamespaceBundle)} and the
     * generation-aware {@link #removeOwnership(OwnedBundle)} — the one every normal
     * {@link OwnedBundle#handleUnloadRequest} unload actually goes through. All three touch
     * {@link #locallyAcquiredLocks} without otherwise coordinating with each other: an acquire installs its lock
     * asynchronously, well after the cache entry that triggered it becomes visible, while a release removes
     * whatever lock is currently registered for the bundle (blindly for the {@code NamespaceBundle}-keyed
     * overload, or only if it still matches a specific generation for the {@code OwnedBundle}-keyed one).
     * Without this barrier, a release can run in the gap between those two points and either release a lock a
     * concurrent acquire just installed (reporting success to that acquire's caller while the underlying ZK
     * lock is gone) or run before the install and be silently bypassed by it (reporting ownership fully
     * released while a fresh acquisition lands moments later).
     */
    private final Map<NamespaceBundle, CompletableFuture<Void>> bundleOperationBarriers = new ConcurrentHashMap<>();

    /**
     * The loading cache of locally owned <code>NamespaceBundle</code> objects.
     */
    private final AsyncLoadingCache<NamespaceBundle, OwnedBundle> ownedBundlesCache;

    /**
     * The <code>NamespaceService</code> which using <code>OwnershipCache</code>.
     */
    private final NamespaceService namespaceService;

    private final PulsarService pulsar;

    private class OwnedServiceUnitCacheLoader implements AsyncCacheLoader<NamespaceBundle, OwnedBundle> {

        @Override
        public CompletableFuture<OwnedBundle> asyncLoad(NamespaceBundle namespaceBundle, Executor executor) {
            return lockManager.acquireLock(ServiceUnitUtils.path(namespaceBundle), selfOwnerInfo)
                    .thenApply(rl -> {
                        locallyAcquiredLocks.put(namespaceBundle, rl);
                        OwnedBundle ownedBundle = new OwnedBundle(namespaceBundle, rl);
                        // Set by the expiry listener below once it runs, whether that happens synchronously as
                        // part of registering it on the next line (the lock was already expired at that point)
                        // or later, asynchronously on whatever thread completes the future. thenRun() only
                        // guarantees inline, synchronous execution for the former case: per the CompletableFuture
                        // class javadoc, a non-async dependent action "may be performed by the thread that
                        // completes the current CompletableFuture, or by any other caller of a completion
                        // method" — there is no guarantee that a *concurrent* completion is reflected here by
                        // the time thenRun() returns. So this flag alone cannot be trusted to catch every case
                        // where the lock died right around registration; the isDone() recheck below closes that
                        // gap by reading the future's own state directly instead of relying on the listener
                        // having finished running.
                        AtomicBoolean expiredBeforePublication = new AtomicBoolean(false);
                        rl.getLockExpiredFuture()
                                .thenRun(() -> {
                                    log.info().attr("path", rl.getPath()).log("Resource lock has expired");
                                    expiredBeforePublication.set(true);
                                    locallyAcquiredLocks.remove(namespaceBundle, rl);
                                    namespaceService.unloadNamespaceBundle(namespaceBundle)
                                            .exceptionally(ex -> {
                                                log.debug()
                                                        .attr("bundle", namespaceBundle)
                                                        .exception(ex)
                                                        .log("Failed to unload namespace bundle after its"
                                                                + " resource lock expired");
                                                return null;
                                            });
                                    invalidateLocalOwnerCache(namespaceBundle, ownedBundle);
                                    namespaceService.onNamespaceBundleUnload(namespaceBundle);
                                });
                        if (expiredBeforePublication.get() || rl.getLockExpiredFuture().isDone()) {
                            // Expiry won the race: never publish an OwnedBundle whose lock is already gone. Let
                            // this load fail instead; Caffeine removes a failed load from the cache automatically,
                            // so no separate cache invalidation is needed here, and the caller of
                            // tryAcquiringOwnership observes a failure instead of a fleeting, already-invalid
                            // success. The isDone() check is a defensive addition alongside the flag: it reads
                            // the future's state directly, so it also catches the case where the lock expired
                            // concurrently with registration above but the listener callback (and therefore the
                            // flag) hasn't finished running yet. Even without it, a lock that expires around here
                            // and is missed by both checks still converges correctly once the listener does run
                            // (see invalidateLocalOwnerCache(NamespaceBundle, OwnedBundle)) — this just narrows
                            // that window rather than being the sole safeguard against it.
                            throw new IllegalStateException(
                                    "Lock for bundle " + namespaceBundle
                                            + " expired before ownership could be published");
                        }
                        return ownedBundle;
                    });
        }
    }

    /**
     * Constructor of <code>OwnershipCache</code>.
     *
     * the local broker URL that will be set as owner for the <code>ServiceUnit</code>
     */
    public OwnershipCache(PulsarService pulsar, NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
        this.pulsar = pulsar;
        this.ownerBrokerUrl = pulsar.getBrokerServiceUrl();
        this.ownerBrokerUrlTls = pulsar.getBrokerServiceUrlTls();
        // At this moment, the variables "webServiceAddress" and "webServiceAddressTls" and so on have not been
        // initialized, so we will get an empty "selfOwnerInfo" and an empty "selfOwnerInfoDisabled" here.
        // But do not worry, these two fields will be set by the method "refreshSelfOwnerInfo" soon.
        this.selfOwnerInfo = new NamespaceEphemeralData(null, ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                false, pulsar.getAdvertisedListeners());
        this.selfOwnerInfoDisabled = new NamespaceEphemeralData(null, ownerBrokerUrl, ownerBrokerUrlTls,
                pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                true, pulsar.getAdvertisedListeners());
        this.lockManager = pulsar.getCoordinationService().getLockManager(NamespaceEphemeralData.class);
        this.locallyAcquiredLocks = new ConcurrentHashMap<>();
        // ownedBundlesCache contains all namespaces that are owned by the local broker
        this.ownedBundlesCache = Caffeine.newBuilder()
                .executor(MoreExecutors.directExecutor())
                .recordStats()
                .buildAsync(new OwnedServiceUnitCacheLoader());
        CacheMetricsCollector.CAFFEINE.addCache("owned-bundles", this.ownedBundlesCache);
    }

    /**
     * Check whether this broker owns given namespace bundle.
     *
     * @param bundle namespace bundle
     * @return future that will complete with check result
     */
    public CompletableFuture<Boolean> checkOwnershipAsync(NamespaceBundle bundle) {
        Optional<CompletableFuture<OwnedBundle>> ownedBundleFuture = getOwnedBundleAsync(bundle);
        if (!ownedBundleFuture.isPresent()) {
            return CompletableFuture.completedFuture(false);
        }
        return ownedBundleFuture.get()
                .thenApply(bd -> bd != null && bd.isActive());
    }

    /**
     * Method to get the current owner of the <code>ServiceUnit</code>.
     *
     * @param suName
     *            name of the <code>ServiceUnit</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * or empty if no ownership info is found
     */
    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suName) {
        CompletableFuture<OwnedBundle> ownedBundleFuture = ownedBundlesCache.getIfPresent(suName);
        if (ownedBundleFuture != null) {
            // Either we're the owners or we're trying to become the owner.
            return ownedBundleFuture.thenApply(serviceUnit -> {
                // We are the owner of the service unit
                return Optional.of(serviceUnit.isActive() ? selfOwnerInfo : selfOwnerInfoDisabled);
            });
        }

        // If we're not the owner, we need to check if anybody else is
        String path = ServiceUnitUtils.path(suName);
        return lockManager.readLock(path).thenCompose(owner -> {
            // If the current broker is the owner, attempt to reacquire ownership to avoid cache loss.
            if (owner.isPresent() && owner.get().equals(selfOwnerInfo)) {
                log.warn()
                        .attr("broker", selfOwnerInfo)
                        .attr("bundle", suName)
                        .log("Detected ownership loss for broker on namespace bundle . Attempting to reacquire"
                                + " ownership to maintain cache consistency.");
                try {
                    return tryAcquiringOwnership(suName).thenApply(Optional::ofNullable);
                } catch (Exception e) {
                    log.error()
                            .attr("bundle", suName)
                            .attr("broker", selfOwnerInfo)
                            .exception(e)
                            .log("Failed to reacquire ownership for namespace bundle on broker");
                    return CompletableFuture.failedFuture(e);
                }
            }
            return CompletableFuture.completedFuture(owner);
        });
    }

    /**
     * Method to get the current owner of the <code>NamespaceBundle</code>
     * or set the local broker as the owner if absent.
     *
     * @param bundle
     *            the <code>NamespaceBundle</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     */
    public CompletableFuture<NamespaceEphemeralData> tryAcquiringOwnership(NamespaceBundle bundle) throws Exception {
        if (!refreshSelfOwnerInfo()) {
            return FutureUtil.failedFuture(
                    new RuntimeException("Namespace service is not ready for acquiring ownership"));
        }

        log.info().attr("bundle", bundle).log("Trying to acquire ownership");

        // Doing a get() on the ownedBundlesCache will trigger an async metadata write to acquire the lock over the
        // service unit. Serialized against both removeOwnership(NamespaceBundle) and removeOwnership(OwnedBundle)
        // for the same bundle: see bundleOperationBarriers.
        return serialize(bundle, () -> ownedBundlesCache.get(bundle)
                .thenApply(namespaceBundle -> {
                    log.info().attr("bundle", namespaceBundle).log("Successfully acquired ownership");
                    namespaceService.onNamespaceBundleOwned(bundle);
                    return selfOwnerInfo;
                }));
    }

    /**
     * Method to remove the ownership of local broker on the <code>NamespaceBundle</code>, if owned.
     *
     */
    public CompletableFuture<Void> removeOwnership(NamespaceBundle bundle) {
        // Serialized against tryAcquiringOwnership(NamespaceBundle) for the same bundle: see
        // bundleOperationBarriers. This also subsumes waiting for a concurrently in-flight acquire to settle
        // before this blind, generation-agnostic release runs.
        return serialize(bundle, () -> {
            ResourceLock<NamespaceEphemeralData> lock = locallyAcquiredLocks.remove(bundle);
            if (lock == null) {
                // We don't own the specified bundle anymore
                return CompletableFuture.completedFuture(null);
            }

            return lock.release();
        });
    }

    /**
     * Runs {@code operation} only after any previously queued {@link #tryAcquiringOwnership(NamespaceBundle)},
     * {@link #removeOwnership(NamespaceBundle)}, or {@link #removeOwnership(OwnedBundle)} call for the same
     * bundle has settled, and queues subsequent calls for that bundle behind this one in turn. The barrier entry
     * for a bundle is removed once no further operation is queued behind it, so {@link #bundleOperationBarriers}
     * does not grow unboundedly.
     *
     * <p>The {@code compute} call below only captures the preceding barrier and installs the new one; it does not
     * chain {@code operation} itself. {@code ConcurrentHashMap.compute} runs its remapping function while holding
     * the map's per-bucket lock, and a same-thread reentrant call into {@code compute} for the same key from
     * inside that function is a usage the map's contract leaves undefined. If {@code operation} (or the preceding
     * barrier) were chained inline here, an already-completed source — a fast-failing metadata call, or the
     * completed futures test doubles commonly return — would make {@code thenCompose} run {@code operation}
     * synchronously right there, still under that lock; were {@code operation} to then reach {@code serialize()}
     * again for the same bundle, that would be exactly such a reentrant call. Building the chain after
     * {@code compute} returns means even a fully synchronous, self-reentrant {@code operation} only ever produces
     * ordinary, non-nested {@code compute} calls.
     */
    private <T> CompletableFuture<T> serialize(NamespaceBundle bundle, Supplier<CompletableFuture<T>> operation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<Void> opDone = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Void>> precedingOpRef = new AtomicReference<>();
        bundleOperationBarriers.compute(bundle, (k, previous) -> {
            precedingOpRef.set(previous != null ? previous : CompletableFuture.completedFuture(null));
            return opDone;
        });
        precedingOpRef.get().handle((r, e) -> null)
                .thenCompose(ignore -> operation.get())
                .whenComplete((r, e) -> {
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                    opDone.complete(null);
                });
        opDone.whenComplete((r, e) -> bundleOperationBarriers.remove(bundle, opDone));
        return result;
    }

    /**
     * Method to remove the ownership that was acquired for the given {@link OwnedBundle} instance only.
     *
     * <p>If the bundle has since been re-acquired (the given instance's lock expired and a newer
     * {@link OwnedBundle} with a newer lock owns the bundle now), the newer ownership is left untouched.
     *
     * <p>Serialized against {@link #tryAcquiringOwnership(NamespaceBundle)} for the same bundle: see
     * {@link #bundleOperationBarriers}. This is the release path every normal {@link OwnedBundle#handleUnloadRequest}
     * unload actually goes through, so without this barrier a concurrent acquire could still observe and
     * report success for the generation this call is in the middle of releasing.
     */
    public CompletableFuture<Void> removeOwnership(OwnedBundle ownedBundle) {
        ResourceLock<NamespaceEphemeralData> lock = ownedBundle.getResourceLock();
        if (lock == null) {
            // The instance is not bound to a lock (not created by this cache): fall back to removing whatever
            // ownership currently exists for the bundle, which is itself serialized already.
            return removeOwnership(ownedBundle.getNamespaceBundle());
        }
        return serialize(ownedBundle.getNamespaceBundle(), () -> {
            if (!locallyAcquiredLocks.remove(ownedBundle.getNamespaceBundle(), lock)) {
                // This ownership generation was already released or has expired; a newer acquisition may own the
                // bundle now and must not be disturbed.
                return CompletableFuture.completedFuture(null);
            }
            return lock.release();
        });
    }

    /**
     * Method to remove ownership of all owned bundles.
     *
     * @param bundles
     *            <code>NamespaceBundles</code> to remove from ownership cache
     */
    public CompletableFuture<Void> removeOwnership(NamespaceBundles bundles) {
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();
        for (NamespaceBundle bundle : bundles.getBundles()) {
            if (getOwnedBundle(bundle) == null) {
                // continue
                continue;
            }
            allFutures.add(this.removeOwnership(bundle));
        }
        return FutureUtil.waitForAll(allFutures);
    }

    /**
     * Method to access the map of all <code>ServiceUnit</code> objects owned by the local broker.
     *
     * @return a map of owned <code>ServiceUnit</code> objects
     */
    public Map<NamespaceBundle, OwnedBundle> getOwnedBundles() {
        return this.ownedBundlesCache.synchronous().asMap();
    }

    public Map<NamespaceBundle, CompletableFuture<OwnedBundle>> getOwnedBundlesAsync() {
        return ownedBundlesCache.asMap();
    }

    /**
     * Checked whether a particular bundle is currently owned by this broker.
     *
     * @param bundle
     * @return
     */
    public boolean isNamespaceBundleOwned(NamespaceBundle bundle) {
        OwnedBundle ownedBundle = getOwnedBundle(bundle);
        return ownedBundle != null && ownedBundle.isActive();
    }

    /**
     * Return the {@link OwnedBundle} instance from the local cache. Does not block.
     *
     * @param bundle
     * @return
     */
    public OwnedBundle getOwnedBundle(NamespaceBundle bundle) {
        CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(bundle);

        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            try {
                return future.get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(),
                        TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        } else {
            return null;
        }
    }

    public Optional<CompletableFuture<OwnedBundle>> getOwnedBundleAsync(NamespaceBundle bundle) {
        return Optional.ofNullable(ownedBundlesCache.getIfPresent(bundle));
    }

    /**
     * Disable bundle in local cache and on zk.
     * @Deprecated This is a dangerous method  which is currently only used for test, it will occupy the ZK thread.
     * Please switch to your own thread after calling this method.
     */
    @Deprecated
    public CompletableFuture<Void> disableOwnership(NamespaceBundle bundle) {
        return updateBundleState(bundle, false)
                .thenCompose(__ -> {
                    ResourceLock<NamespaceEphemeralData> lock = locallyAcquiredLocks.get(bundle);
                    if (lock == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return lock.updateValue(selfOwnerInfoDisabled);
                    }
                });
    }

    /**
     * Update bundle state in a local cache.
     *
     * @param bundle
     * @throws Exception
     */
    public CompletableFuture<Void> updateBundleState(NamespaceBundle bundle, boolean isActive) {
        // Disable owned instance in local cache
        CompletableFuture<OwnedBundle> f = ownedBundlesCache.getIfPresent(bundle);
        if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
            return f.thenAccept(ob -> ob.setActive(isActive));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public void invalidateLocalOwnerCache() {
        this.ownedBundlesCache.synchronous().invalidateAll();
    }

    public void invalidateLocalOwnerCache(NamespaceBundle namespaceBundle) {
        this.ownedBundlesCache.synchronous().invalidate(namespaceBundle);
    }

    /**
     * Invalidate the local owner cache entry once it holds the given {@link OwnedBundle} instance, so that a
     * stale lock-expiry callback cannot drop an entry installed by a newer acquisition.
     *
     * <p>The callback that calls this can run before the cache's own load future for this bundle has completed:
     * the lock-expiry listener is registered inside the cache loader's {@code thenApply}, and if the lock was
     * already expired at that point the listener fires inline, synchronously, before the loader returns and the
     * future is published as done. (If the lock instead expires concurrently with registration, {@code thenRun}
     * gives no such synchronous guarantee — the listener may run later, on whichever thread completes the
     * future — but that only changes when this method is called relative to publication, not whether it is
     * called; the handling below covers both.) Waiting on the future via {@code whenComplete} instead of
     * requiring {@code isDone()} up front handles both cases: if the future is already done the callback runs
     * immediately, and if not, the removal is deferred until the loader publishes it, so the newly-published
     * {@link OwnedBundle} — whose lock has already expired — is not left claiming active ownership forever.
     */
    private void invalidateLocalOwnerCache(NamespaceBundle namespaceBundle, OwnedBundle expectedOwnedBundle) {
        CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(namespaceBundle);
        if (future == null) {
            return;
        }
        future.whenComplete((ownedBundle, ex) -> {
            if (ex == null && ownedBundle == expectedOwnedBundle) {
                ownedBundlesCache.asMap().remove(namespaceBundle, future);
            }
        });
    }

    @VisibleForTesting
    public Map<NamespaceBundle, ResourceLock<NamespaceEphemeralData>> getLocallyAcquiredLocks() {
        return locallyAcquiredLocks;
    }

    public synchronized boolean refreshSelfOwnerInfo() {
        this.selfOwnerInfo = new NamespaceEphemeralData(pulsar.getBrokerId(), pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(), pulsar.getWebServiceAddress(),
                pulsar.getWebServiceAddressTls(), false, pulsar.getAdvertisedListeners());
        this.selfOwnerInfoDisabled = new NamespaceEphemeralData(pulsar.getBrokerId(), pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(), pulsar.getWebServiceAddress(),
                pulsar.getWebServiceAddressTls(), true, pulsar.getAdvertisedListeners());
        return selfOwnerInfo.getNativeUrl() != null || selfOwnerInfo.getNativeUrlTls() != null;
    }
}
