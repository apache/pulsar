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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;

/**
 * Registration manager for bookies on top of the pulsar metadata store.
 *
 * <p>Registrations are held as ephemeral resource locks, which silently disappear when the
 * metadata store session is lost. On {@link SessionEvent#SessionLost} this manager re-validates
 * every tracked registration with its own read / disambiguate / write loop until the record is
 * re-established on a live session, escalating through
 * {@link RegistrationListener#onRegistrationExpired()} only after repeated confirmation of a
 * genuine foreign owner.
 *
 * <p>The write is a version-compared put (CAS), not an unconditional put: a writer landing
 * between the read and the write fails with {@code BadVersion} and routes back to
 * disambiguation, so a different-value holder is never silently trampled. Every successful write
 * is followed by a lock-handle reacquire: on stores where a put does not transfer the ephemeral
 * ownership (zk {@code setData}), it is the reacquire's delete-and-recreate that moves the record
 * onto the live session, so the loop only stops after the reacquire completes. A registration
 * path occupied by a non-ephemeral node is never written: a put would silently convert the
 * persistent node into an ephemeral one.
 *
 * <p>The bookkeeper {@code ZKRegistrationManager} reports a registration expiry on every session
 * loss and relies on the surrounding zk client stack to keep re-issuing its operations against
 * the rebuilt session. The metadata store stack used here does not retry store operations across
 * a session loss, so this manager carries the re-registration itself instead: a same-value
 * foreign holder (a stale copy of this bookie) is trampled and re-acquired rather than waited
 * out, and the registration-expired notification fires only for a different-value holder
 * confirmed over consecutive attempts. A genuine conflict still ends in the same terminal state:
 * the consumer's re-registration fails and the bookie exits.
 */
@CustomLog
public class PulsarRegistrationManager implements RegistrationManager {

    /**
     * Number of consecutive observations of a foreign registration holding a different value
     * before escalating through {@link RegistrationListener#onRegistrationExpired()}.
     */
    private static final int FOREIGN_HOLDER_CONFIRMATIONS = 3;

    private static final long MUTATION_EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5000;

    private static final Set<Option> EPHEMERAL_PUT = Set.of(Option.Ephemeral.INSTANCE);

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

    // Revalidation loop bookkeeping, keyed by registration path. These fields are only accessed
    // from the mutationExecutor (after close, completion continuations may also run inline on
    // store callback threads). The loop is active while the map is non-empty.
    private final Map<String, RegistrationRevalidation> activeRevalidations = new ConcurrentHashMap<>();

    PulsarRegistrationManager(MetadataStoreExtended store, String ledgersRootPath, AbstractConfiguration<?> conf) {
        this.store = store;
        this.conf = conf;
        this.coordinationService = new CoordinationServiceImpl(store);
        this.lockManager = coordinationService.getLockManager(BookieServiceInfoSerde.INSTANCE);
        this.ledgersRootPath = ledgersRootPath;
        this.cookiePath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
        // Session-loss driven re-registration: the session events are only produced when the
        // store was created with the session watcher enabled.
        store.registerSessionListener(this::handleSessionEvent);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Stop the revalidation loop and drop its pending ticks. Store operations that were
        // already in flight may still complete after this: their results are discarded
        // silently by the loop completion handlers.
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
                } catch (ExecutionException | TimeoutException ignore) {
                    log.error().attr("lock", lock).exception(ignore.getCause()).log("Cannot release correctly");
                    removeOwnRegistrationRecord(lock.getPath());
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
     * Internal registration path, to be run only on the mutation executor. The revalidation
     * loop must never call this through the public {@link #registerBookie(BookieId, boolean,
     * BookieServiceInfo)} adapter, because that blocks on the same single-thread executor.
     */
    private void doRegisterBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)
            throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String regPathReadOnly = bookieReadonlyRegistrationPath + "/" + bookieId;

        try {
            if (readOnly) {
                ResourceLock<BookieServiceInfo> rwRegistration = bookieRegistration.remove(bookieId);
                if (rwRegistration != null) {
                    log.info().attr("bookieId", bookieId)
                            .log("Bookie was already registered as writable, unregistering");
                    releaseRegistrationLock(bookieId, false, rwRegistration);
                }

                bookieRegistrationReadOnly.put(bookieId,
                        lockManager.acquireLock(regPathReadOnly, bookieServiceInfo)
                                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS));
            } else {
                ResourceLock<BookieServiceInfo> roRegistration = bookieRegistrationReadOnly.remove(bookieId);
                if (roRegistration != null) {
                    log.info().attr("bookieId", bookieId)
                            .log("Bookie was already registered as read-only, unregistering");
                    releaseRegistrationLock(bookieId, true, roRegistration);
                }

                bookieRegistration.put(bookieId,
                        lockManager.acquireLock(regPath, bookieServiceInfo)
                                .get(BLOCKING_CALL_TIMEOUT, MILLISECONDS));
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
        if (readOnly) {
            ResourceLock<BookieServiceInfo> roRegistration = bookieRegistrationReadOnly.remove(bookieId);
            if (roRegistration != null) {
                releaseRegistrationLock(bookieId, true, roRegistration);
            }
        } else {
            ResourceLock<BookieServiceInfo> rwRegistration = bookieRegistration.remove(bookieId);
            if (rwRegistration != null) {
                releaseRegistrationLock(bookieId, false, rwRegistration);
            }
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

    private void handleSessionEvent(SessionEvent event) {
        if (event == SessionEvent.SessionLost) {
            // Keep this callback free of any I/O: it must never block the store session event
            // thread. It only makes sure, at most once, that the revalidation loop is running.
            scheduleOnMutationExecutor(this::startRevalidationLoop);
        }
    }

    /**
     * Re-validates all the currently tracked registrations, at most one target per registration
     * path. A registration already under revalidation keeps its in-flight target, while a
     * registration whose tracked handle was replaced or re-created since its target was created
     * gets a fresh target — the replaced target stops itself through the identity checks on its
     * next tick. This way a session lost while a previous loop is still draining is never
     * swallowed. Must run on the mutation executor.
     */
    private void startRevalidationLoop() {
        if (closed.get()) {
            return;
        }

        List<RegistrationRevalidation> newTargets = new ArrayList<>();
        bookieRegistration.forEach(
                (bookieId, lock) -> addRevalidationTarget(newTargets, bookieId, false, lock));
        bookieRegistrationReadOnly.forEach(
                (bookieId, lock) -> addRevalidationTarget(newTargets, bookieId, true, lock));

        if (newTargets.isEmpty()) {
            // The session was lost before any registration happened, or every registration is
            // already under revalidation.
            return;
        }

        log.info().attr("registrations", newTargets.size())
                .log("Metadata store session was lost, re-validating bookie registrations");
        newTargets.forEach(target -> {
            activeRevalidations.put(registrationPath(target.bookieId, target.readOnly), target);
            scheduleRevalidationTick(target, 0);
        });
    }

    private void addRevalidationTarget(List<RegistrationRevalidation> newTargets,
            BookieId bookieId, boolean readOnly, ResourceLock<BookieServiceInfo> lock) {
        RegistrationRevalidation existing = activeRevalidations.get(registrationPath(bookieId, readOnly));
        if (existing == null || existing.lock != lock) {
            newTargets.add(new RegistrationRevalidation(bookieId, readOnly, lock));
        }
    }

    private void scheduleRevalidationTick(RegistrationRevalidation target, long delayMillis) {
        try {
            mutationExecutor.schedule(() -> runRevalidationTickSafely(target), delayMillis, MILLISECONDS);
        } catch (RejectedExecutionException ignore) {
            // The registration manager was closed
        }
    }

    /**
     * Guards the tick against an unexpected failure: the target must never be left pinned in
     * the loop because a single throw escaped the task body.
     */
    private void runRevalidationTickSafely(RegistrationRevalidation target) {
        try {
            runRevalidationTick(target);
        } catch (Throwable t) {
            log.warn().attr("bookieId", target.bookieId).exception(t)
                    .log("Unexpected failure re-validating the registration, retrying");
            if (activeRevalidations.get(registrationPath(target.bookieId, target.readOnly)) == target) {
                scheduleRevalidationTick(target, target.backoff.next().toMillis());
            }
        }
    }

    /**
     * One revalidation attempt for one registration: read the registration record, route it
     * through the disambiguation, then write it with a version-compared put. Must run on the
     * mutation executor.
     */
    private void runRevalidationTick(RegistrationRevalidation target) {
        if (closed.get()
                || activeRevalidations.get(registrationPath(target.bookieId, target.readOnly)) != target) {
            stopRevalidation(target);
            return;
        }

        if (!isTrackedRegistrationCurrent(target)) {
            // The registration entry is gone or was replaced in the meantime, for example by a
            // readonly conversion or by an explicit new registration: never resurrect it.
            stopRevalidation(target);
            return;
        }

        log.info().attr("bookieId", target.bookieId)
                .log("Re-validating bookie registration after metadata session loss");
        store.get(registrationPath(target.bookieId, target.readOnly))
                .whenComplete((result, ex) ->
                        scheduleOnMutationExecutor(() -> onRegistrationReadCompleted(target, result, ex)));
    }

    private void onRegistrationReadCompleted(RegistrationRevalidation target,
            Optional<GetResult> result, Throwable ex) {
        if (closed.get() || !isTrackedRegistrationCurrent(target)) {
            stopRevalidation(target);
            return;
        }

        if (ex != null) {
            // The store could not be observed: retry without counting any observation.
            log.warn().attr("bookieId", target.bookieId).exception(ex)
                    .log("Failed to read the registration, retrying");
            scheduleRevalidationTick(target, target.backoff.next().toMillis());
            return;
        }

        if (result.isEmpty()) {
            // No record: re-create it. The -1 expectation enforces must-not-exist, so a
            // concurrent creator turns our put into a BadVersion back to disambiguation.
            target.foreignHolderObservations = 0;
            startRegistrationCasWrite(target, Optional.empty());
            return;
        }

        GetResult getResult = result.get();
        if (!getResult.getStat().isEphemeral()) {
            // A non-ephemeral node occupies the registration path: never write it, because a
            // put would silently convert the persistent node into an ephemeral one. Treat it
            // as a registration conflict.
            if (!target.nonEphemeralWarned) {
                target.nonEphemeralWarned = true;
                log.warn().attr("bookieId", target.bookieId)
                        .log("Registration path is occupied by a non-ephemeral node");
            }
            observeForeignConflict(target);
            return;
        }

        // Either our own record or a stale same-value copy of it holds the path: rewrite it
        // with the versioned write, which rebinds it to the current session, never escalating.
        boolean createdBySelf = getResult.getStat().isCreatedBySelf();
        if (createdBySelf || hasSameRegistrationValue(target, getResult)) {
            target.foreignHolderObservations = 0;
            if (!createdBySelf && !target.staleHolderWarned) {
                target.staleHolderWarned = true;
                log.warn().attr("bookieId", target.bookieId)
                        .log("Registration is currently owned by a stale session with the same value,"
                                + " trampling it to re-acquire it");
            }
            startRegistrationCasWrite(target, Optional.of(getResult));
            return;
        }

        // A different owner with a different value: only repeated consecutive confirmations
        // escalate, otherwise wait and re-read.
        observeForeignConflict(target);
    }

    /**
     * Writes the registration record with the version just read as the expectation. An
     * unconditional put is never used: a writer landing inside the read-to-write window
     * produces a BadVersion that routes back to disambiguation instead of being silently
     * trampled, and the lock layer's adopt branch, which can report success without writing,
     * is never entered.
     */
    private void startRegistrationCasWrite(RegistrationRevalidation target, Optional<GetResult> read) {
        String path = registrationPath(target.bookieId, target.readOnly);
        byte[] payload;
        try {
            payload = BookieServiceInfoSerde.INSTANCE.serialize(path, target.cachedValue);
        } catch (Throwable t) {
            log.warn().attr("bookieId", target.bookieId).exception(t)
                    .log("Failed to serialize the registration, retrying");
            scheduleRevalidationTick(target, target.backoff.next().toMillis());
            return;
        }

        Optional<Long> expectedVersion = read.isPresent()
                ? Optional.of(read.get().getStat().getVersion())
                : Optional.of(-1L);
        store.put(path, payload, expectedVersion, EPHEMERAL_PUT)
                .whenComplete((stat, ex) ->
                        scheduleOnMutationExecutor(() -> onRegistrationCasWriteCompleted(target, ex)));
    }

    private void onRegistrationCasWriteCompleted(RegistrationRevalidation target, Throwable ex) {
        if (closed.get()) {
            // A completion arriving after close(): no logs, no further work, but a write that
            // landed after close() released the registrations must not be left behind.
            // Nothing else can mutate anymore, so the cleanup runs inline.
            stopRevalidation(target);
            cleanupStaleRegistrationWrite(target, true);
            return;
        }

        if (!isTrackedRegistrationCurrent(target)) {
            // The registration entry is gone or was replaced while the write was in flight
            // (readonly conversion, unregister or a fresh registration). If the in-flight
            // write still landed and resurrected the torn-down record, remove it again.
            stopRevalidation(target);
            cleanupStaleRegistrationWrite(target, false);
            return;
        }

        if (ex == null) {
            // The record was really written and, on stores where a put transfers the
            // ephemeral ownership, rebound to the current session. Swap the lock handle
            // before declaring success: on stores where a put does not rebind (zk setData),
            // it is the reacquire leg that moves the record onto the live session.
            reacquireRegistrationLock(target);
            return;
        }

        Throwable cause = FutureUtil.unwrapCompletionException(ex);
        if (cause instanceof MetadataStoreException.BadVersionException) {
            // Someone wrote between the read and this put: back to the read / disambiguation
            // loop. This is exactly why the write is version-compared.
            scheduleRevalidationTick(target, target.backoff.next().toMillis());
            return;
        }

        // Transient failure, for example the store being unavailable: keep retrying.
        log.warn().attr("bookieId", target.bookieId).exception(cause)
                .log("Failed to write the registration, retrying");
        scheduleRevalidationTick(target, target.backoff.next().toMillis());
    }

    /**
     * Records one observation of a foreign, different-value holder of the registration (also
     * used for a non-ephemeral occupant of the path): only repeated consecutive confirmations
     * escalate, any other observation resets the counter.
     */
    private void observeForeignConflict(RegistrationRevalidation target) {
        target.staleHolderWarned = false;
        target.foreignHolderObservations++;
        if (target.foreignHolderObservations >= FOREIGN_HOLDER_CONFIRMATIONS) {
            // A different owner with a different value was confirmed over consecutive
            // attempts: report the conflict truthfully, the listener will react to it.
            log.error().attr("bookieId", target.bookieId).log("Bookie registration is held by another owner");
            stopRevalidation(target);
            notifyRegistrationExpired();
            return;
        }

        scheduleRevalidationTick(target, target.backoff.next().toMillis());
    }

    /**
     * Swaps the lock handle after a successful registration write: on stores where the put
     * does not transfer the ephemeral ownership, it is this reacquire leg, through its
     * internal delete-and-recreate, that moves the record onto the live session. Must run on
     * the mutation executor.
     */
    private void reacquireRegistrationLock(RegistrationRevalidation target) {
        lockManager.acquireLock(registrationPath(target.bookieId, target.readOnly), target.cachedValue)
                .whenComplete((lock, ex) ->
                        scheduleOnMutationExecutor(() -> onReacquireCompleted(target, lock, ex)));
    }

    private void onReacquireCompleted(RegistrationRevalidation target,
            ResourceLock<BookieServiceInfo> newLock, Throwable ex) {
        if (closed.get() || !isTrackedRegistrationCurrent(target)) {
            // The registration was closed, removed or replaced while re-acquiring: discard the
            // newly acquired lock instead of resurrecting the registration, and clean up the
            // registration write that preceded the reacquire if it resurrected the record.
            if (newLock != null) {
                newLock.release();
            }
            stopRevalidation(target);
            cleanupStaleRegistrationWrite(target, closed.get());
            return;
        }

        if (ex != null) {
            // The reacquire failed, for example a conflict or a transient failure: go back to
            // the read / disambiguation / write loop, which starts with a fresh read.
            log.warn().attr("bookieId", target.bookieId).exception(ex)
                    .log("Failed to re-acquire the registration, retrying");
            scheduleRevalidationTick(target, target.backoff.next().toMillis());
            return;
        }

        if (target.lock.getLockExpiredFuture().isDone()) {
            log.info().attr("bookieId", target.bookieId)
                    .log("Registration lock is no longer valid, re-acquiring it");
        }
        registrationMap(target.readOnly).put(target.bookieId, newLock);
        target.lock = newLock;

        // The loop only stops here, after the reacquire completed: the write alone does not
        // prove the registration is on a live session on every backend.
        log.info().attr("bookieId", target.bookieId).log("Bookie registration re-validated");
        stopRevalidation(target);
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

    private boolean hasSameRegistrationValue(RegistrationRevalidation target, GetResult result) {
        try {
            BookieServiceInfo existing = BookieServiceInfoSerde.INSTANCE.deserialize(
                    registrationPath(target.bookieId, target.readOnly), result.getValue(), result.getStat());
            return Objects.equals(existing, target.cachedValue);
        } catch (Throwable t) {
            // Not parseable as a BookieServiceInfo: consider it a different value.
            return false;
        }
    }

    /**
     * Best-effort removal of a registration record that a revalidation write, already in
     * flight when the registration was torn down, resurrected after the teardown. Only a
     * record of our own identity with our own value is removed, so a registration that was
     * legitimately re-created in the meantime is never touched. On stores where
     * {@code createdBySelf} is scoped to the current session rather than to the client
     * identity, a record this loop rebound through a plain write may not qualify and the
     * cleanup is skipped; the leftover is then bounded by the lifetime of the session that
     * owns it. The {@code afterClose} parameter is a call-site snapshot of {@code closed} and
     * must stay: when true, the cleanup always runs inline, because a task accepted between
     * close()'s closed-CAS and mutationExecutor.shutdownNow() would be discarded by the
     * shutdown; and the re-creation guard is skipped, because after close there is no
     * legitimate re-creator left.
     */
    private void cleanupStaleRegistrationWrite(RegistrationRevalidation target, boolean afterClose) {
        String path = registrationPath(target.bookieId, target.readOnly);
        store.get(path).whenComplete((result, ex) -> {
            Runnable cleaner = () -> removeResurrectedRegistration(target, path, result, ex, afterClose);
            if (afterClose) {
                cleaner.run();
            } else {
                scheduleOnMutationExecutor(cleaner);
            }
        });
    }

    private void removeResurrectedRegistration(RegistrationRevalidation target, String path,
            Optional<GetResult> result, Throwable ex, boolean afterClose) {
        if (ex != null || result.isEmpty()) {
            return;
        }
        GetResult getResult = result.get();
        if (!getResult.getStat().isCreatedBySelf() || !hasSameRegistrationValue(target, getResult)) {
            return;
        }
        if (!afterClose && registrationMap(target.readOnly).containsKey(target.bookieId)) {
            // The registration was legitimately re-created in the meantime: leave it alone.
            return;
        }
        log.info().attr("bookieId", target.bookieId).log("Cleaning up a stale registration write");
        store.delete(path, Optional.of(getResult.getStat().getVersion()));
    }

    /**
     * Best-effort removal of a registration record that could not be released through its
     * lock, for example when the lock version was reset to -1 by a revalidation still in
     * flight at close time. Removing the record also lets the coordination service's own
     * release at close converge, because it tolerates a NotFound. Only a record created by
     * this store identity is removed.
     */
    private void removeOwnRegistrationRecord(String path) {
        store.get(path).whenComplete((result, ex) -> {
            if (ex != null || result.isEmpty() || !result.get().getStat().isCreatedBySelf()) {
                return;
            }
            store.delete(path, Optional.of(result.get().getStat().getVersion()));
        });
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

    private boolean isTrackedRegistrationCurrent(RegistrationRevalidation target) {
        return registrationMap(target.readOnly).get(target.bookieId) == target.lock;
    }

    private void stopRevalidation(RegistrationRevalidation target) {
        activeRevalidations.remove(registrationPath(target.bookieId, target.readOnly), target);
    }

    /**
     * The revalidation state of a single registration after a metadata session loss.
     */
    private static class RegistrationRevalidation {

        final BookieId bookieId;
        final boolean readOnly;
        final BookieServiceInfo cachedValue;
        final Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(5))
                .build();
        ResourceLock<BookieServiceInfo> lock;
        int foreignHolderObservations = 0;
        boolean staleHolderWarned = false;
        boolean nonEphemeralWarned = false;

        RegistrationRevalidation(BookieId bookieId, boolean readOnly, ResourceLock<BookieServiceInfo> lock) {
            this.bookieId = bookieId;
            this.readOnly = readOnly;
            this.lock = lock;
            this.cachedValue = lock.getValue();
        }
    }
}
