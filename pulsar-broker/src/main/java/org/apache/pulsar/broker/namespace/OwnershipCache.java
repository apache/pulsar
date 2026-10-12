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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException.MetadataSessionLostException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.TopicName;
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
                        rl.getLockExpiredFuture()
                                .thenRun(() -> {
                                    // Normal ownership releases remove the registration before releasing the lock.
                                    // During shutdown, LockManager also releases any remaining locks directly,
                                    // so those callbacks can still find a registration. Always remove it, but
                                    // only report expiry at INFO while the broker is running.
                                    if (locallyAcquiredLocks.remove(namespaceBundle, rl) && pulsar.isRunning()) {
                                        log.info().attr("path", rl.getPath()).log("Resource lock has expired");
                                    } else {
                                        log.debug().attr("path", rl.getPath())
                                                .log("Resource lock is no longer active; running the expiry listener");
                                    }
                                    // Only unload the generation this listener belongs to.
                                    // unloadNamespaceBundle resolves the owner by bundle name, so a listener
                                    // that runs late (the load already failed the publication check below and
                                    // a lookup re-acquired the bundle in the meantime) would otherwise close a
                                    // newer generation's topics and release its lock. If the cache does not
                                    // hold this instance, this generation never served anything (or is already
                                    // gone), so there is nothing to unload.
                                    if (getOwnedBundle(namespaceBundle) == ownedBundle) {
                                        namespaceService.unloadNamespaceBundle(namespaceBundle)
                                                .exceptionally(ex -> {
                                                    log.debug()
                                                            .attr("bundle", namespaceBundle)
                                                            .exception(ex)
                                                            .log("Failed to unload namespace bundle after its"
                                                                    + " resource lock expired");
                                                    return null;
                                                });
                                    }
                                    // The unload event is tied to the same generation check: it is delivered
                                    // only if this generation's entry was the one published in the cache (and is
                                    // removed here). A generation that never got published never produced an
                                    // owned event, and if the cache holds a newer generation that one is still
                                    // owned; in both cases an unload event would be unmatched. Listeners keep
                                    // per-namespace bookkeeping on these events (the topic-policies service counts
                                    // owned bundles and tears the namespace's cache down at zero), so an
                                    // unmatched event is not harmless.
                                    invalidateLocalOwnerCache(namespaceBundle, ownedBundle,
                                            () -> namespaceService.onNamespaceBundleUnload(namespaceBundle));
                                });
                        if (rl.getLockExpiredFuture().isDone()) {
                            // Expiry won the race: never publish an OwnedBundle whose lock is already gone. Let
                            // this load fail instead; Caffeine removes a failed load from the cache automatically,
                            // so no separate cache invalidation is needed here, and the caller of
                            // tryAcquiringOwnership observes a failure instead of a fleeting, already-invalid
                            // success. Reading the future's own state covers both the lock that was already
                            // expired when the listener above was registered (the listener ran inline) and one
                            // that expired concurrently with the registration (the listener may still be
                            // running on the completing thread). A lock that expires right after this check
                            // still converges once the listener runs: see
                            // invalidateLocalOwnerCache(NamespaceBundle, OwnedBundle).
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
        if (ownedBundleFuture != null && !ownedBundleFuture.isDone() && pulsar.isMetadataSessionLost()) {
            // an acquisition that started before the session was lost waits for the metadata store
            return FutureUtil.failedFuture(sessionLostException(suName));
        }
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
                if (pulsar.isMetadataSessionLost()) {
                    // The metadata cache can still hold this broker's lock value although the store deleted it with
                    // the session, and another broker may own the bundle now. Reacquiring it would wait for the
                    // metadata store, so fail at once, and the client looks the topic up at another broker.
                    return FutureUtil.failedFuture(sessionLostException(suName));
                }
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

    private static MetadataSessionLostException sessionLostException(NamespaceBundle bundle) {
        return new MetadataSessionLostException("Namespace bundle " + bundle
                + " can't be acquired by this broker while its metadata session is lost");
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
        if (pulsar.isMetadataSessionLost()) {
            // acquiring the lock would wait for the session to be reestablished
            return FutureUtil.failedFuture(sessionLostException(bundle));
        }
        if (!refreshSelfOwnerInfo()) {
            return FutureUtil.failedFuture(
                    new RuntimeException("Namespace service is not ready for acquiring ownership"));
        }

        log.info().attr("bundle", bundle).log("Trying to acquire ownership");

        // Doing a get() on the ownedBundlesCache will trigger an async metadata write to acquire the lock over the
        // service unit. Serialized against both removeOwnership(NamespaceBundle) and removeOwnership(OwnedBundle)
        // for the same bundle: see bundleOperationBarriers.
        return serialize(bundle, () -> {
            // Again when the acquisition's turn comes: an acquisition queued behind another operation of the bundle
            // may start after the session was lost
            if (pulsar.isMetadataSessionLost()) {
                return FutureUtil.failedFuture(sessionLostException(bundle));
            }
            return ownedBundlesCache.get(bundle)
                    .thenApply(namespaceBundle -> {
                        log.info().attr("bundle", namespaceBundle).log("Successfully acquired ownership");
                        namespaceService.onNamespaceBundleOwned(bundle);
                        return selfOwnerInfo;
                    });
        });
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
     * does not grow unboundedly. Each operation's completion hands the barrier to the next queued operation via
     * {@link PulsarService#getExecutor()}, so the depth of the drain does not depend on the length of the queue
     * (and a caller's completion callbacks never run the next queued operation on their own stack).
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
                    // Release the barrier on the executor rather than inline: the next queued operation runs
                    // as a dependent of opDone, and a cache-hit acquire completes synchronously, so completing
                    // inline would run each queued operation nested inside the previous one's stack frame. A
                    // deep enough backlog then overflows the stack, the StackOverflowError is swallowed by the
                    // CompletableFuture machinery, and this barrier entry is never released, leaving the bundle
                    // permanently unacquirable and unreleasable on this broker.
                    try {
                        opDone.completeAsync(() -> null, pulsar.getExecutor());
                    } catch (RejectedExecutionException ree) {
                        // The executor is gone (broker shutting down): still release the barrier, inline, so
                        // that the operations queued behind it (e.g. the releases of the remaining bundles) can
                        // run instead of hanging forever.
                        opDone.complete(null);
                    }
                });
        opDone.whenComplete((r, e) -> bundleOperationBarriers.remove(bundle, opDone));
        return result;
    }

    /**
     * Method to remove the ownership that was acquired for the given {@link OwnedBundle} instance only.
     *
     * <p>If the bundle has since been re-acquired (the given instance's lock expired and a newer
     * {@link OwnedBundle} with a newer lock owns the bundle now), the newer ownership is left untouched. An
     * instance that is not bound to a lock at all (not created by this cache) releases nothing.
     *
     * <p>Serialized against {@link #tryAcquiringOwnership(NamespaceBundle)} for the same bundle: see
     * {@link #bundleOperationBarriers}. This is the release path every normal {@link OwnedBundle#handleUnloadRequest}
     * unload actually goes through, so without this barrier a concurrent acquire could still observe and
     * report success for the generation this call is in the middle of releasing.
     */
    public CompletableFuture<Void> removeOwnership(OwnedBundle ownedBundle) {
        ResourceLock<NamespaceEphemeralData> lock = ownedBundle.getResourceLock();
        if (lock == null) {
            // The instance is not bound to a lock (not created by this cache), so it represents no ownership
            // generation and there is nothing of its own to release. Releasing whatever generation currently
            // owns the bundle instead would be exactly the generation-blind release this method exists to avoid.
            log.warn().attr("bundle", ownedBundle.getNamespaceBundle())
                    .log("Ignoring release request for an OwnedBundle that is not bound to a resource lock");
            return CompletableFuture.completedFuture(null);
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
     * Stops serving the bundle of a topic whose ledger another broker fenced after this broker lost its metadata
     * session, which shows that the other broker took the bundle over.
     *
     * <p>A broker that has lost its metadata session keeps serving its bundles from this cache until it can
     * revalidate their locks. When the metadata store expires the session, it deletes the session's locks, and
     * another broker can acquire a bundle's lock and load its topics, fencing the topics' ledgers. This broker's
     * writes then fail: that is the only sign of the takeover it gets before its session is reestablished. Without
     * acting on it, this broker keeps answering lookups for the bundle with itself and keeps loading its topics,
     * which wait for the metadata store, so the clients can't move to the new owner until then.
     *
     * <p>This removes the bundle's ownership generation from the cache and closes the bundle's topics, as an unload
     * does, but doesn't release the lock, since releasing it needs the metadata store and another broker holds it.
     * While the session is lost, this broker doesn't acquire bundles (see {@link #tryAcquiringOwnership}) nor
     * reacquire a bundle whose lock value the metadata cache still holds as its own (see
     * {@link #getOwnerAsync(NamespaceBundle)}), and topic loads fail the ownership check, so the clients look the
     * topic up again until they reach the new owner. When the session is reestablished, the lock manager
     * revalidates the lock: it finds another broker's lock and expires it, and its expiry listener finds this
     * generation gone and has nothing left to do. If no other broker holds the lock after all, the lock revalidates,
     * and the next lookup reacquires the ownership from it with a new lock instance, as after any loss of the cache
     * entry; the revalidated instance is then no longer tracked and needs nothing.
     *
     * <p>The handling runs on the broker's executor in the bundle's operation order with the acquisitions (see
     * {@link #serialize}), until the closing of the topics has started, and only while the session is lost, when no
     * acquisition can complete, so it can't race with an acquisition. A report that comes after the session was
     * reestablished is ignored, leaving the takeover to the lock's revalidation. The report applies to the ownership
     * generation that served the topic when it was made and to that loss of the session only, so a report that runs
     * after the bundle was acquired again, or after the session came back and was lost again, changes nothing.
     * Besides, only the loaded instance of a topic reports (see {@code PersistentTopic.addFailed}).
     *
     * @return a future that completes when the report has been handled: the bundle is no longer served and its
     *     topics' closing has started, or the report was ignored
     */
    CompletableFuture<Void> handleOwnershipTakenOver(TopicName topicName) {
        // Find the bundle among the owned ones, which needs no metadata, rather than in the namespace's bundles,
        // which can have been invalidated in the cache.
        OwnedBundle reported = null;
        for (CompletableFuture<OwnedBundle> future : ownedBundlesCache.asMap().values()) {
            OwnedBundle candidate = future.isDone() && !future.isCompletedExceptionally() ? future.join() : null;
            if (candidate != null && candidate.getNamespaceBundle().includes(topicName)) {
                reported = candidate;
                break;
            }
        }
        if (reported == null || !pulsar.isMetadataSessionLost()) {
            return CompletableFuture.completedFuture(null);
        }
        // The report applies to this ownership generation and this loss of the session only: a report that runs
        // after the bundle was reacquired, or after the session came back and was lost again, changes nothing
        OwnedBundle reportedBundle = reported;
        long sessionLoss = pulsar.getMetadataSessionLossCount();
        NamespaceBundle takenOverBundle = reported.getNamespaceBundle();
        // On the broker's executor, not on the thread that reported the takeover, and the bundle's next operation
        // waits until the closing of its topics has started, so that the closing can't reach a newer generation's
        return serialize(takenOverBundle, () -> {
            try {
                return CompletableFuture.runAsync(() -> stopServingTakenOverBundle(takenOverBundle, reportedBundle,
                                sessionLoss, topicName), pulsar.getExecutor());
            } catch (RejectedExecutionException e) {
                // the broker is shutting down
                stopServingTakenOverBundle(takenOverBundle, reportedBundle, sessionLoss, topicName);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private void stopServingTakenOverBundle(NamespaceBundle bundle, OwnedBundle ownedBundle, long sessionLoss,
                                            TopicName topicName) {
        if (!pulsar.isMetadataSessionLost() || pulsar.getMetadataSessionLossCount() != sessionLoss) {
            return;
        }
        if (getOwnedBundle(bundle) != ownedBundle) {
            // another topic of the bundle reported the takeover first, or the bundle was acquired again since
            return;
        }
        AtomicBoolean invalidated = new AtomicBoolean();
        // the entry is a completed future, so the invalidation runs synchronously
        invalidateLocalOwnerCache(bundle, ownedBundle, () -> invalidated.set(true));
        if (!invalidated.get()) {
            return;
        }
        ownedBundle.setActive(false);
        log.warn().attr("bundle", bundle).attr("topic", topicName)
                .log("Another broker took the bundle over after the metadata session was lost; stopped serving it"
                        + " and closing its topics");
        Map<String, CompletableFuture<Optional<Topic>>> topicFutures;
        try {
            topicFutures = pulsar.getBrokerService().getTopicFuturesInBundle(bundle);
        } catch (RuntimeException e) {
            log.warn().attr("bundle", bundle).exception(e)
                    .log("Failed to close the topics of a bundle that another broker took over");
            namespaceService.onNamespaceBundleUnload(bundle);
            return;
        }
        closeTakenOverTopics(bundle, topicFutures);
    }

    private void closeTakenOverTopics(NamespaceBundle bundle,
                                      Map<String, CompletableFuture<Optional<Topic>>> topicFutures) {
        CompletableFuture<Integer> unloadFuture;
        try {
            unloadFuture = pulsar.getBrokerService().unloadServiceUnit(bundle, true, true,
                    pulsar.getConfiguration().getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS,
                    topicFutures);
        } catch (RuntimeException e) {
            unloadFuture = FutureUtil.failedFuture(e);
        }
        unloadFuture.whenComplete((unloadedTopics, ex) -> {
            if (ex != null) {
                log.warn().attr("bundle", bundle).exception(ex)
                        .log("Failed to close the topics of a bundle that another broker took over");
            }
            pulsar.getBrokerService().cleanUnloadedTopicFromCache(bundle, topicFutures);
            namespaceService.onNamespaceBundleUnload(bundle);
        });
    }

    /**
     * Invalidate the local owner cache entry once it holds the given {@link OwnedBundle} instance, so that a
     * stale lock-expiry callback cannot drop an entry installed by a newer acquisition. {@code onInvalidated}
     * runs only if this call actually removed that entry, i.e. only for a generation that had been published.
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
    private void invalidateLocalOwnerCache(NamespaceBundle namespaceBundle, OwnedBundle expectedOwnedBundle,
                                           Runnable onInvalidated) {
        CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(namespaceBundle);
        if (future == null) {
            return;
        }
        future.whenComplete((ownedBundle, ex) -> {
            if (ex == null && ownedBundle == expectedOwnedBundle
                    && ownedBundlesCache.asMap().remove(namespaceBundle, future)) {
                onInvalidated.run();
            }
        });
    }

    @VisibleForTesting
    public Map<NamespaceBundle, ResourceLock<NamespaceEphemeralData>> getLocallyAcquiredLocks() {
        return locallyAcquiredLocks;
    }

    @VisibleForTesting
    Map<NamespaceBundle, CompletableFuture<Void>> getBundleOperationBarriers() {
        return bundleOperationBarriers;
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
