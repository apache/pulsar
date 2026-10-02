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
package org.apache.pulsar.metadata.bookkeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.INSTANCEID;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver.BLOCKING_CALL_TIMEOUT;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;

/**
 * Registration manager for bookies on top of the pulsar metadata store.
 *
 * <p>Registrations are held as ephemeral resource locks, which the coordination layer
 * revalidates and re-establishes on its own after a metadata store session loss: when the
 * session is re-established, every tracked lock is revalidated, a lock whose record was swept
 * is re-created on the live session, and a record still owned by an expired session is
 * re-written and re-bound. This manager therefore carries no re-registration loop of its own:
 * it maps the expiry of a registration lock — the point where the lock layer concludes the
 * registration cannot be held — to the bookkeeper
 * {@link RegistrationListener#onRegistrationExpired()} contract, so that the consumer can
 * attempt a fresh registration.
 *
 * <p>The bookkeeper {@code ZKRegistrationManager} reports a registration expiry on every
 * session loss and relies on the surrounding zk client stack to keep re-issuing its
 * operations against the rebuilt session. The metadata store stack used here does not retry
 * store operations across a session loss; the lock layer's revalidation is what converges the
 * registration instead, and the expiry notification fires only when the registration was
 * genuinely lost to another owner — a same-value record of this bookie, including a stale
 * copy left by an expired session, is re-adopted or re-created rather than waited out. A
 * genuine conflict still ends in the same terminal state: the consumer's re-registration
 * fails and the bookie exits.
 */
@CustomLog
public class PulsarRegistrationManager implements RegistrationManager {

    private static final long MUTATION_EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5000;

    private final MetadataStoreExtended store;
    private final CoordinationService coordinationService;
    private final LockManager<BookieServiceInfo> lockManager;
    private final AbstractConfiguration<?> conf;

    private final String ledgersRootPath;
    private final String cookiePath;
    private final String bookieRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    private final Map<BookieId, ResourceLock<BookieServiceInfo>> bookieRegistration = new ConcurrentHashMap<>();
    private final Map<BookieId, ResourceLock<BookieServiceInfo>> bookieRegistrationReadOnly = new ConcurrentHashMap<>();
    private final List<RegistrationListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Single-threaded executor that serializes all the registration mutations and the
     * session-loss revalidation loop. Public registration calls submit here and block until
     * completion, because the bookkeeper state machine invokes them synchronously and expects
     * failures to propagate to the calling thread.
     */
    private final ScheduledExecutorService mutationExecutor = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("bookie-registration-mutation"));

    /**
     * The registration-expired listeners are notified from this dedicated executor instead of
     * inline on the mutation executor or on a store event thread: the listeners are third-party
     * code and must stay isolated, so that a slow or failing listener can neither hold up the
     * mutation executor nor break the notification of the other listeners.
     */
    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory("bookie-registration-listener"));

    private final AtomicBoolean closed = new AtomicBoolean(false);

    PulsarRegistrationManager(MetadataStoreExtended store, String ledgersRootPath, AbstractConfiguration<?> conf) {
        this.store = store;
        this.conf = conf;
        this.coordinationService = new CoordinationServiceImpl(store);
        this.lockManager = coordinationService.getLockManager(BookieServiceInfoSerde.INSTANCE);
        this.ledgersRootPath = ledgersRootPath;
        this.cookiePath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Stop accepting mutations. Store operations that were already in flight may still
        // complete after this: their results are discarded silently by the completion handlers.
        mutationExecutor.shutdownNow();
        listenerExecutor.shutdown();
        try {
            mutationExecutor.awaitTermination(MUTATION_EXECUTOR_SHUTDOWN_TIMEOUT_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (Map<BookieId, ResourceLock<BookieServiceInfo>> registrations :
                List.of(bookieRegistration, bookieRegistrationReadOnly)) {
            for (ResourceLock<BookieServiceInfo> lock : registrations.values()) {
                try {
                    lock.release().get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    log.error().attr("lock", lock).exception(e.getCause()).log("Cannot release correctly");
                    try {
                        removeOwnRegistrationRecord(lock.getPath());
                    } catch (BookieException cleanupFailure) {
                        log.warn().attr("lock", lock).exception(cleanupFailure)
                                .log("Cannot remove the registration record directly");
                    }
                    discardUnreleasableLock(lock);
                } catch (InterruptedException ignore) {
                    log.error().attr("lock", lock).exception(ignore).log("Cannot release correctly");
                    Thread.currentThread().interrupt();
                }
            }
        }
        try {
            coordinationService.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        try {
            return store.get(ledgersRootPath + "/" + INSTANCEID)
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)
                    .map(res -> new String(res.getValue(), UTF_8))
                    .orElseThrow(
                            () -> new BookieException.MetadataStoreException("BookKeeper cluster not initialized"));
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new BookieException.MetadataStoreException("Failed to get cluster instance id", e);
        }
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)
            throws BookieException {
        log.info().attr("bookieId", bookieId).attr("readOnly", readOnly).attr("info", bookieServiceInfo)
                .log("RegisterBookie");
        runBlockingMutation(() -> doRegisterBookie(bookieId, readOnly, bookieServiceInfo));
    }

    /**
     * Internal registration path, to be run only on the mutation executor. It is only ever
     * called through the public {@link #registerBookie(BookieId, boolean, BookieServiceInfo)}
     * adapter, which blocks on the same single-thread executor.
     */
    private void doRegisterBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)
            throws BookieException {
        try {
            if (readOnly) {
                unregisterTrackedRegistration(bookieId, false);

                bookieRegistrationReadOnly.put(bookieId,
                        acquireRegistrationLock(bookieId, true, bookieServiceInfo));
            } else {
                unregisterTrackedRegistration(bookieId, true);

                bookieRegistration.put(bookieId,
                        acquireRegistrationLock(bookieId, false, bookieServiceInfo));
            }
        } catch (ExecutionException | TimeoutException ee) {
            log.error().exception(ee).log("Exception registering ephemeral node for Bookie");
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new BookieException.MetadataStoreException(ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error().exception(ie).log("Interrupted exception while registering Bookie");
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new BookieException.MetadataStoreException(ie);
        }
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        runBlockingMutation(() -> doUnregisterBookie(bookieId, readOnly));
    }

    private void doUnregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        unregisterTrackedRegistration(bookieId, readOnly);
    }

    /**
     * Releases the tracked registration of the bookie. The handle stays tracked until its
     * release succeeded, so that a failed unregister, for example on a transient store
     * failure, can be retried: a retry that finds no handle would silently succeed without
     * deleting anything. Must run on the mutation executor.
     */
    private void unregisterTrackedRegistration(BookieId bookieId, boolean readOnly) throws BookieException {
        ResourceLock<BookieServiceInfo> registration = registrationMap(readOnly).get(bookieId);
        if (registration == null) {
            return;
        }
        releaseRegistrationLock(bookieId, readOnly, registration);
        registrationMap(readOnly).remove(bookieId, registration);
    }

    /**
     * Acquires the registration lock and watches its expiry: the expiry of a tracked
     * registration, the point where the lock layer concludes the registration cannot be held,
     * is what drives the bookkeeper registration-expired contract.
     */
    private ResourceLock<BookieServiceInfo> acquireRegistrationLock(
            BookieId bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)
            throws InterruptedException, ExecutionException, TimeoutException {
        ResourceLock<BookieServiceInfo> lock = lockManager
                .acquireLock(registrationPath(bookieId, readOnly), bookieServiceInfo)
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        lock.getLockExpiredFuture().thenRun(() -> scheduleOnMutationExecutor(
                () -> handleRegistrationExpired(bookieId, readOnly, lock)));
        return lock;
    }

    /**
     * The lock of a tracked registration expired. Only the still-tracked handle owns the
     * notification: a voluntary unregister removes the handle only after its release
     * succeeded, and the completion of that release must not drive the bookie into a
     * re-registration of what was just torn down.
     */
    private void handleRegistrationExpired(BookieId bookieId, boolean readOnly,
            ResourceLock<BookieServiceInfo> lock) {
        if (closed.get()) {
            return;
        }
        if (registrationMap(readOnly).remove(bookieId, lock)) {
            log.warn().attr("bookieId", bookieId)
                    .log("The bookie registration expired, notifying the listeners");
            notifyRegistrationExpired();
        }
    }

    /**
     * Releases a registration lock, tolerating a BadVersion caused by a revalidation of the
     * same lock still in flight (its version expectation was reset): in that case the record
     * is removed directly, when it belongs to this store identity, and the unreleasable
     * handle is discarded, because the registration it represented is torn down anyway.
     */
    private void releaseRegistrationLock(BookieId bookieId, boolean readOnly,
            ResourceLock<BookieServiceInfo> registration) throws BookieException {
        try {
            registration.release().get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(ie);
        } catch (ExecutionException | TimeoutException e) {
            if (e instanceof ExecutionException
                    && e.getCause() instanceof MetadataStoreException.BadVersionException) {
                log.warn().attr("bookieId", bookieId)
                        .log("Cannot release the registration lock through its handle,"
                                + " removing the registration record directly");
                removeOwnRegistrationRecord(registrationPath(bookieId, readOnly));
                discardUnreleasableLock(registration);
                return;
            }
            throw new BookieException.MetadataStoreException(e);
        }
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String readonlyRegPath = bookieReadonlyRegistrationPath + "/" + bookieId;

        try {
            return (store.exists(regPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)
                    || store.exists(readonlyRegPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS));
        } catch (ExecutionException | TimeoutException e) {
            log.error().attr("bookieId", bookieId).exception(e)
                    .log("Exception while checking registration ephemeral nodes");
            throw new BookieException.MetadataStoreException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error().attr("bookieId", bookieId).exception(e)
                    .log("InterruptedException while checking registration ephemeral nodes");
            throw new BookieException.MetadataStoreException(e);
        }
    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {
        String path = this.cookiePath + "/" + bookieId;
        try {
            long version;
            if (Version.NEW == cookieData.getVersion()) {
                version = -1L;
            } else {
                if (!(cookieData.getVersion() instanceof LongVersion)) {
                    throw new BookieException.BookieIllegalOpException(
                            "Invalid version type, expected it to be LongVersion");
                }
                version = ((LongVersion) cookieData.getVersion()).getLongVersion();
            }

            store.put(path, cookieData.getValue(), Optional.of(version))
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException("Interrupted writing cookie for bookie " + bookieId, ie);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MetadataStoreException.BadVersionException) {
                throw new BookieException.CookieExistException(bookieId.toString());
            } else {
                throw new BookieException.MetadataStoreException("Failed to write cookie for bookie " + bookieId);
            }
        } catch (TimeoutException ex) {
            throw new BookieException.MetadataStoreException("Failed to write cookie for bookie " + bookieId, ex);
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        String path = this.cookiePath + "/" + bookieId;
        try {
            Optional<GetResult> res = store.get(path).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
            if (!res.isPresent()) {
                throw new BookieException.CookieNotFoundException(bookieId.toString());
            }

            // sets stat version from MetadataStore
            LongVersion version = new LongVersion(res.get().getStat().getVersion());
            return new Versioned<>(res.get().getValue(), version);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(ie);
        } catch (ExecutionException | TimeoutException e) {
            throw new BookieException.MetadataStoreException(e);
        }
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        String path = this.cookiePath + "/" + bookieId;
        try {
            store.delete(path, Optional.of(((LongVersion) version).getLongVersion()))
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException("Interrupted deleting cookie for bookie " + bookieId, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                throw new BookieException.CookieNotFoundException(bookieId.toString());
            } else {
                throw new BookieException.MetadataStoreException("Failed to delete cookie for bookie " + bookieId);
            }
        } catch (TimeoutException ex) {
            throw new BookieException.MetadataStoreException("Failed to delete cookie for bookie " + bookieId);
        }

        log.info().attr("cookiePath", cookiePath).attr("bookieId", bookieId).log("Removed cookie for bookie");
    }

    @Override
    public boolean prepareFormat() throws Exception {
        boolean ledgerRootExists = store.exists(ledgersRootPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        boolean availableNodeExists = store.exists(bookieRegistrationPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        // Create ledgers root node if not exists
        if (!ledgerRootExists) {
            store.put(ledgersRootPath, new byte[0], Optional.empty())
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        }
        // create available bookies node if not exists
        if (!availableNodeExists) {
            store.put(bookieRegistrationPath, new byte[0], Optional.empty())
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        }

        // create readonly bookies node if not exists
        if (!store.exists(bookieReadonlyRegistrationPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)) {
            store.put(bookieReadonlyRegistrationPath, new byte[0], Optional.empty())
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        }

        return ledgerRootExists;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        String instanceIdPath = ledgersRootPath + "/" + INSTANCEID;
        log.info().attr("ledgersRootPath", ledgersRootPath).log("Initializing metadata for new cluster");

        if (store.exists(instanceIdPath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)) {
            log.error().attr("ledgersRootPath", ledgersRootPath).log("Ledger root path already exists");
            return false;
        }

        store.put(ledgersRootPath, new byte[0], Optional.empty())
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        store.put(instanceIdPath, instanceId.getBytes(UTF_8), Optional.of(-1L))
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        log.info().attr("ledgersRootPath", ledgersRootPath).attr("instanceId", instanceId)
                .log("Successfully initiated cluster");
        return true;
    }

    @Override
    public boolean format() throws Exception {
        // Clear underreplicated ledgers
        store.deleteRecursive(PulsarLedgerUnderreplicationManager.getBasePath(ledgersRootPath)
                              + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH)
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        // Clear underreplicatedledger locks
        store.deleteRecursive(PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRootPath))
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        // Clear the cookies
        store.deleteRecursive(cookiePath).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        // Clear the INSTANCEID
        if (store.exists(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID)
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)) {
            store.delete(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID, Optional.empty())
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        }

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        store.put(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID,
                instanceId.getBytes(StandardCharsets.UTF_8), Optional.of(-1L))
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);

        log.info().log("Successfully formatted BookKeeper metadata");
        return true;
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        log.info().attr("ledgersRootPath", ledgersRootPath).log("Nuking metadata of existing cluster");

        if (!store.exists(ledgersRootPath + "/" + INSTANCEID).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS)) {
            log.info().attr("ledgersRootPath", ledgersRootPath)
                    .log("There is no existing cluster, so exiting nuke operation");
            return true;
        }

        @Cleanup
        RegistrationClient registrationClient = new PulsarRegistrationClient(store, ledgersRootPath);

        Collection<BookieId> rwBookies = registrationClient.getWritableBookies()
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS).getValue();
        if (rwBookies != null && !rwBookies.isEmpty()) {
            log.error("Bookies are still up and connected to this cluster, stop all bookies before nuking the cluster");
            return false;
        }

        Collection<BookieId> roBookies = registrationClient.getReadOnlyBookies()
                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS).getValue();
        if (roBookies != null && !roBookies.isEmpty()) {
            log.error("Readonly Bookies are still up and connected to this cluster,"
                    + " stop all bookies before nuking the cluster");
            return false;
        }

        LayoutManager layoutManager = new PulsarLayoutManager(store, ledgersRootPath);
        LedgerManagerFactory ledgerManagerFactory = new PulsarLedgerManagerFactory();
        ledgerManagerFactory.initialize(conf, layoutManager, LegacyHierarchicalLedgerManagerFactory.CUR_VERSION);
        return ledgerManagerFactory.validateAndNukeExistingCluster(conf, layoutManager);
    }

    @Override
    public void addRegistrationListener(RegistrationListener listener) {
        listeners.add(listener);
    }

    /**
     * Runs a registration mutation on the mutation executor, blocking the calling thread until
     * the mutation completes and propagating its failure, to preserve the synchronous contract
     * of the bookkeeper registration calls.
     */
    private void runBlockingMutation(RegistrationMutation mutation) throws BookieException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // The budget covers the worst-case internal blocking sequence and the time the mutation
        // may spend queued behind other work on the single-thread executor.
        long deadline = System.currentTimeMillis() + 2 * BLOCKING_CALL_TIMEOUT + 5_000;
        try {
            mutationExecutor.execute(() -> {
                try {
                    mutation.run();
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            throw new BookieException.MetadataStoreException(e);
        }

        try {
            future.get(deadline - System.currentTimeMillis(), MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof BookieException) {
                throw (BookieException) e.getCause();
            }
            throw new BookieException.MetadataStoreException(e.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(ie);
        } catch (TimeoutException te) {
            throw new BookieException.MetadataStoreException(te);
        }
    }

    private interface RegistrationMutation {

        void run() throws BookieException;
    }

    private void scheduleOnMutationExecutor(Runnable task) {
        try {
            mutationExecutor.execute(task);
        } catch (RejectedExecutionException ignore) {
            // The executor rejects only after close. Run the continuation inline: after close
            // there is no mutation left to race with, and the continuations only do terminal
            // bookkeeping, like the cleanup of a stale in-flight registration write. A task
            // accepted between close()'s closed-CAS and the shutdownNow() that follows is
            // dropped instead; what it could have cleaned up is a leftover bound to the dying
            // session, which the store's own shutdown clears.
            if (closed.get()) {
                task.run();
            }
        }
    }

    private void notifyRegistrationExpired() {
        try {
            listenerExecutor.execute(() -> {
                for (RegistrationListener listener : listeners) {
                    try {
                        listener.onRegistrationExpired();
                    } catch (Throwable t) {
                        log.error().exception(t).log("Failed to notify the registration listener");
                    }
                }
            });
        } catch (RejectedExecutionException ignore) {
            // The registration manager was closed
        }
    }
    /**
     * Removes the registration record directly, completing inside the current mutation: a
     * cleanup that only issues the get/delete and returns could race a subsequent registration
     * that adopted the record — an adoption does not change the version — and delete the
     * fresh record with the version this cleanup read. Only a record created by this store
     * identity is removed.
     */
    private void removeOwnRegistrationRecord(String path) throws BookieException {
        Optional<GetResult> result;
        try {
            result = store.get(path).get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new BookieException.MetadataStoreException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(e);
        }
        if (result.isEmpty() || !result.get().getStat().isCreatedBySelf()) {
            return;
        }
        try {
            store.delete(path, Optional.of(result.get().getStat().getVersion()))
                    .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof MetadataStoreException.NotFoundException)
                    && !(e.getCause() instanceof MetadataStoreException.BadVersionException)) {
                throw new BookieException.MetadataStoreException(e);
            }
            // The record is already gone, or was rewritten in the meantime by a writer we
            // cannot attribute to this registration: nothing of it remains to clean up.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(e);
        } catch (TimeoutException e) {
            throw new BookieException.MetadataStoreException(e);
        }
    }

    /**
     * Marks a lock that could not be released, for example because it was lost to another
     * owner, as expired, so that it is discarded by the lock manager instead of failing its
     * own close path.
     */
    private void discardUnreleasableLock(ResourceLock<BookieServiceInfo> lock) {
        lock.getLockExpiredFuture().complete(null);
    }

    private Map<BookieId, ResourceLock<BookieServiceInfo>> registrationMap(boolean readOnly) {
        return readOnly ? bookieRegistrationReadOnly : bookieRegistration;
    }

    private String registrationPath(BookieId bookieId, boolean readOnly) {
        return readOnly
                ? bookieReadonlyRegistrationPath + "/" + bookieId
                : bookieRegistrationPath + "/" + bookieId;
    }
}
