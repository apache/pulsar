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
package org.apache.pulsar.metadata.bookkeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.INSTANCEID;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class PulsarRegistrationManager implements RegistrationManager {

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
    @SneakyThrows
    public void close() {
        for (ResourceLock<BookieServiceInfo> rwBookie : bookieRegistration.values()) {
            try {
                rwBookie.release().get();
            } catch (ExecutionException ignore) {
                log.error("Cannot release correctly {}", rwBookie, ignore.getCause());
            } catch (InterruptedException ignore) {
                log.error("Cannot release correctly {}", rwBookie, ignore);
                Thread.currentThread().interrupt();
            }
        }

        for (ResourceLock<BookieServiceInfo> roBookie : bookieRegistrationReadOnly.values()) {
            try {
                roBookie.release().get();
            } catch (ExecutionException ignore) {
                log.error("Cannot release correctly {}", roBookie, ignore.getCause());
            } catch (InterruptedException ignore) {
                log.error("Cannot release correctly {}", roBookie, ignore);
                Thread.currentThread().interrupt();
            }
        }
        coordinationService.close();
    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        try {
            return store.get(ledgersRootPath + "/" + INSTANCEID)
                    .get()
                    .map(res -> new String(res.getValue(), UTF_8))
                    .orElseThrow(
                            () -> new BookieException.MetadataStoreException("BookKeeper cluster not initialized"));
        } catch (ExecutionException | InterruptedException e) {
            throw new BookieException.MetadataStoreException("Failed to get cluster instance id", e);
        }
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)
            throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String regPathReadOnly = bookieReadonlyRegistrationPath + "/" + bookieId;
        log.info("RegisterBookie {} readOnly {} info {}", bookieId, readOnly, bookieServiceInfo);

        try {
            if (readOnly) {
                ResourceLock<BookieServiceInfo> rwRegistration = bookieRegistration.remove(bookieId);
                if (rwRegistration != null) {
                    log.info("Bookie {} was already registered as writable, unregistering", bookieId);
                    rwRegistration.release().get();
                }

                bookieRegistrationReadOnly.put(bookieId,
                        lockManager.acquireLock(regPathReadOnly, bookieServiceInfo).get());
            } else {
                ResourceLock<BookieServiceInfo> roRegistration = bookieRegistrationReadOnly.remove(bookieId);
                if (roRegistration != null) {
                    log.info("Bookie {} was already registered as read-only, unregistering", bookieId);
                    roRegistration.release().get();
                }

                bookieRegistration.put(bookieId,
                        lockManager.acquireLock(regPath, bookieServiceInfo).get());
            }
        } catch (ExecutionException ee) {
            log.error("Exception registering ephemeral node for Bookie!", ee);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new BookieException.MetadataStoreException(ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Interrupted exception while registering Bookie!", ie);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new BookieException.MetadataStoreException(ie);
        }
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        try {
            if (readOnly) {
                ResourceLock<BookieServiceInfo> roRegistration = bookieRegistrationReadOnly.get(bookieId);
                if (roRegistration != null) {
                    roRegistration.release().get();
                }
            } else {
                ResourceLock<BookieServiceInfo> rwRegistration = bookieRegistration.get(bookieId);
                if (rwRegistration != null) {
                    rwRegistration.release().get();
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(ie);
        } catch (ExecutionException e) {
            throw new BookieException.MetadataStoreException(e);
        }
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String readonlyRegPath = bookieReadonlyRegistrationPath + "/" + bookieId;

        try {
            return (store.exists(regPath).get() || store.exists(readonlyRegPath).get());
        } catch (ExecutionException e) {
            log.error("Exception while checking registration ephemeral nodes for BookieId: {}", bookieId, e);
            throw new BookieException.MetadataStoreException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("InterruptedException while checking registration ephemeral nodes for BookieId: {}", bookieId,
                    e);
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

            store.put(path, cookieData.getValue(), Optional.of(version)).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException("Interrupted writing cookie for bookie " + bookieId, ie);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MetadataStoreException.BadVersionException) {
                throw new BookieException.CookieExistException(bookieId.toString());
            } else {
                throw new BookieException.MetadataStoreException("Failed to write cookie for bookie " + bookieId);
            }
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        String path = this.cookiePath + "/" + bookieId;
        try {
            Optional<GetResult> res = store.get(path).get();
            if (!res.isPresent()) {
                throw new BookieException.CookieNotFoundException(bookieId.toString());
            }

            // sets stat version from MetadataStore
            LongVersion version = new LongVersion(res.get().getStat().getVersion());
            return new Versioned<>(res.get().getValue(), version);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException(ie);
        } catch (ExecutionException e) {
            throw new BookieException.MetadataStoreException(e);
        }
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        String path = this.cookiePath + "/" + bookieId;
        try {
            store.delete(path, Optional.of(((LongVersion) version).getLongVersion())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BookieException.MetadataStoreException("Interrupted deleting cookie for bookie " + bookieId, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                throw new BookieException.CookieNotFoundException(bookieId.toString());
            } else {
                throw new BookieException.MetadataStoreException("Failed to delete cookie for bookie " + bookieId);
            }
        }

        log.info("Removed cookie from {} for bookie {}.", cookiePath, bookieId);
    }

    @Override
    public boolean prepareFormat() throws Exception {
        boolean ledgerRootExists = store.exists(ledgersRootPath).get();
        boolean availableNodeExists = store.exists(bookieRegistrationPath).get();
        // Create ledgers root node if not exists
        if (!ledgerRootExists) {
            store.put(ledgersRootPath, new byte[0], Optional.empty()).get();
        }
        // create available bookies node if not exists
        if (!availableNodeExists) {
            store.put(bookieRegistrationPath, new byte[0], Optional.empty()).get();
        }

        // create readonly bookies node if not exists
        if (!store.exists(bookieReadonlyRegistrationPath).get()) {
            store.put(bookieReadonlyRegistrationPath, new byte[0], Optional.empty()).get();
        }

        return ledgerRootExists;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        String instanceIdPath = ledgersRootPath + "/" + INSTANCEID;
        log.info("Initializing metadata for new cluster, ledger root path: {}",
                ledgersRootPath);

        if (store.exists(instanceIdPath).get()) {
            log.error("Ledger root path: {} already exists", ledgersRootPath);
            return false;
        }

        store.put(ledgersRootPath, new byte[0], Optional.empty()).get();

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        store.put(instanceIdPath, instanceId.getBytes(UTF_8), Optional.of(-1L)).join();

        log.info("Successfully initiated cluster. ledger root path: {} instanceId: {}",
                ledgersRootPath, instanceId);
        return true;
    }

    @Override
    public boolean format() throws Exception {
        // Clear underreplicated ledgers
        store.deleteRecursive(PulsarLedgerUnderreplicationManager.getBasePath(ledgersRootPath)
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH).get();

        // Clear underreplicatedledger locks
        store.deleteRecursive(PulsarLedgerUnderreplicationManager.getUrLockPath(ledgersRootPath)).get();

        // Clear the cookies
        store.deleteRecursive(cookiePath).get();

        // Clear the INSTANCEID
        if (store.exists(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID).get()) {
            store.delete(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID, Optional.empty()).get();
        }

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        store.put(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID,
                instanceId.getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();

        log.info("Successfully formatted BookKeeper metadata");
        return true;
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        log.info("Nuking metadata of existing cluster, ledger root path: {}", ledgersRootPath);

        if (!store.exists(ledgersRootPath + "/" + INSTANCEID).join()) {
            log.info("There is no existing cluster with ledgersRootPath: {}, so exiting nuke operation",
                    ledgersRootPath);
            return true;
        }

        @Cleanup
        RegistrationClient registrationClient = new PulsarRegistrationClient(store, ledgersRootPath);

        Collection<BookieId> rwBookies = registrationClient.getWritableBookies().join().getValue();
        if (rwBookies != null && !rwBookies.isEmpty()) {
            log.error("Bookies are still up and connected to this cluster, "
                    + "stop all bookies before nuking the cluster");
            return false;
        }

        Collection<BookieId> roBookies = registrationClient.getReadOnlyBookies().join().getValue();
        if (roBookies != null && !roBookies.isEmpty()) {
            log.error("Readonly Bookies are still up and connected to this cluster, "
                    + "stop all bookies before nuking the cluster");
            return false;
        }

        LayoutManager layoutManager = new PulsarLayoutManager(store, ledgersRootPath);
        LedgerManagerFactory ledgerManagerFactory = new PulsarLedgerManagerFactory();
        ledgerManagerFactory.initialize(conf, layoutManager, LegacyHierarchicalLedgerManagerFactory.CUR_VERSION);
        return ledgerManagerFactory.validateAndNukeExistingCluster(conf, layoutManager);
    }
}
