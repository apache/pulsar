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
import static org.apache.bookkeeper.proto.DataFormats.CheckAllLedgersFormat;
import static org.apache.bookkeeper.proto.DataFormats.LedgerRereplicationLayoutFormat;
import static org.apache.bookkeeper.proto.DataFormats.LockDataFormat;
import static org.apache.bookkeeper.proto.DataFormats.PlacementPolicyCheckFormat;
import static org.apache.bookkeeper.proto.DataFormats.ReplicasCheckFormat;
import static org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.replication.ReplicationEnableCb;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class PulsarLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
    static final String LAYOUT = "BASIC";

    static final int LAYOUT_VERSION = 1;

    private static final byte[] LOCK_DATA = getLockData();

    private static class Lock {
        private final String lockPath;
        private final long ledgerNodeVersion;

        Lock(String lockPath, long ledgerNodeVersion) {
            this.lockPath = lockPath;
            this.ledgerNodeVersion = ledgerNodeVersion;
        }

        String getLockPath() {
            return lockPath;
        }

        long getLedgerNodeVersion() {
            return ledgerNodeVersion;
        }
    }

    private final Map<Long, Lock> heldLocks = new ConcurrentHashMap<>();

    private static final Pattern ID_EXTRACTION_PATTERN = Pattern.compile("urL(\\d+)$");

    private final AbstractConfiguration conf;
    private final String basePath;
    private final String urLedgerPath;
    private final String urLockPath;
    private final String layoutPath;
    private final String lostBookieRecoveryDelayPath;
    private final String checkAllLedgersCtimePath;
    private final String placementPolicyCheckCtimePath;
    private final String replicasCheckCtimePath;

    private final MetadataStoreExtended store;

    private BookkeeperInternalCallbacks.GenericCallback<Void> replicationEnabledListener;
    private BookkeeperInternalCallbacks.GenericCallback<Void> lostBookieRecoveryDelayListener;

    private static class PulsarUnderreplicatedLedger extends UnderreplicatedLedger {
        PulsarUnderreplicatedLedger(long ledgerId) {
            super(ledgerId);
        }

        @Override
        protected void setCtime(long ctime) {
            super.setCtime(ctime);
        }

        @Override
        protected void setReplicaList(List<String> replicaList) {
            super.setReplicaList(replicaList);
        }
    }

    public PulsarLedgerUnderreplicationManager(AbstractConfiguration<?> conf, MetadataStoreExtended store,
                                               String ledgerRootPath)
            throws ReplicationException.CompatibilityException {
        this.conf = conf;
        this.basePath = getBasePath(ledgerRootPath);
        layoutPath = basePath + '/' + BookKeeperConstants.LAYOUT_ZNODE;
        urLedgerPath = basePath + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        urLockPath = basePath + '/' + BookKeeperConstants.UNDER_REPLICATION_LOCK;
        lostBookieRecoveryDelayPath = basePath + '/' + BookKeeperConstants.LOSTBOOKIERECOVERYDELAY_NODE;
        checkAllLedgersCtimePath = basePath + '/' + BookKeeperConstants.CHECK_ALL_LEDGERS_CTIME;
        placementPolicyCheckCtimePath = basePath + '/' + BookKeeperConstants.PLACEMENT_POLICY_CHECK_CTIME;
        replicasCheckCtimePath = basePath + '/' + BookKeeperConstants.REPLICAS_CHECK_CTIME;

        this.store = store;
        store.registerListener(this::handleNotification);

        checkLayout();
    }

    static String getBasePath(String rootPath) {
        return String.format("%s/%s", rootPath, BookKeeperConstants.UNDER_REPLICATION_NODE);
    }

    static String getUrLockPath(String rootPath) {
        return String.format("%s/%s", getBasePath(rootPath), BookKeeperConstants.UNDER_REPLICATION_LOCK);
    }

    public static byte[] getLockData() {
        DataFormats.LockDataFormat.Builder lockDataBuilder = DataFormats.LockDataFormat.newBuilder();
        try {
            lockDataBuilder.setBookieId(DNS.getDefaultHost("default"));
        } catch (UnknownHostException uhe) {
            // if we cant get the address, ignore. it's optional
            // in the data structure in any case
        }
        return lockDataBuilder.build().toString().getBytes(UTF_8);
    }

    private void checkLayout() throws ReplicationException.CompatibilityException {
        while (true) {
            if (!store.exists(layoutPath).join()) {
                LedgerRereplicationLayoutFormat.Builder builder = LedgerRereplicationLayoutFormat.newBuilder();
                builder.setType(LAYOUT).setVersion(LAYOUT_VERSION);
                store.put(layoutPath, builder.build().toString().getBytes(UTF_8), Optional.of(-1L)).join();
            } else {
                byte[] layoutData = store.get(layoutPath).join().get().getValue();

                LedgerRereplicationLayoutFormat.Builder builder = LedgerRereplicationLayoutFormat.newBuilder();

                try {
                    TextFormat.merge(new String(layoutData, UTF_8), builder);
                    LedgerRereplicationLayoutFormat layout = builder.build();
                    if (!layout.getType().equals(LAYOUT)
                            || layout.getVersion() != LAYOUT_VERSION) {
                        throw new ReplicationException.CompatibilityException(
                                "Incompatible layout found (" + LAYOUT + ":" + LAYOUT_VERSION + ")");
                    }
                } catch (TextFormat.ParseException pe) {
                    throw new ReplicationException.CompatibilityException(
                            "Invalid data found", pe);
                }
                break;
            }
        }
    }

    private long getLedgerId(String path) throws NumberFormatException {
        Matcher m = ID_EXTRACTION_PATTERN.matcher(path);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        } else {
            throw new NumberFormatException("Couldn't find ledgerid in path");
        }
    }

    private static String getParentPath(String base, long ledgerId) {
        String subdir1 = String.format("%04x", ledgerId >> 48 & 0xffff);
        String subdir2 = String.format("%04x", ledgerId >> 32 & 0xffff);
        String subdir3 = String.format("%04x", ledgerId >> 16 & 0xffff);
        String subdir4 = String.format("%04x", ledgerId & 0xffff);

        return String.format("%s/%s/%s/%s/%s",
                base, subdir1, subdir2, subdir3, subdir4);
    }

    public static String getUrLedgerPath(String base, long ledgerId) {
        return String.format("%s/urL%010d", getParentPath(base, ledgerId), ledgerId);
    }

    public static String getUrLedgerLockPath(String base, long ledgerId) {
        return String.format("%s/urL%010d", base, ledgerId);
    }

    private String getUrLedgerPath(long ledgerId) {
        return getUrLedgerPath(urLedgerPath, ledgerId);
    }

    private void handleNotification(Notification n) {
        if (n.getPath().startsWith(basePath)) {
            synchronized (this) {
                // Notify that there were some changes on the under-replicated z-nodes
                notifyAll();

                if (n.getType() == NotificationType.Deleted) {
                    if (n.getPath().equals(basePath + '/' + BookKeeperConstants.DISABLE_NODE)) {
                        log.info("LedgerReplication is enabled externally through MetadataStore, "
                                + "since DISABLE_NODE ZNode is deleted");
                        if (replicationEnabledListener != null) {
                            replicationEnabledListener.operationComplete(0, null);
                        }
                    } else if (n.getPath().equals(lostBookieRecoveryDelayPath)) {
                        if (lostBookieRecoveryDelayListener != null) {
                            lostBookieRecoveryDelayListener.operationComplete(0, null);
                        }
                    }
                }
            }
        }
    }

    @Override
    public UnderreplicatedLedger getLedgerUnreplicationInfo(long ledgerId)
            throws ReplicationException.UnavailableException {
        try {
            String path = getUrLedgerPath(ledgerId);

            Optional<GetResult> optRes = store.get(path).get();
            if (!optRes.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Ledger: {} is not marked underreplicated", ledgerId);
                }
                return null;
            }

            byte[] data = optRes.get().getValue();

            UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();

            TextFormat.merge(new String(data, UTF_8), builder);
            UnderreplicatedLedgerFormat underreplicatedLedgerFormat = builder.build();
            PulsarUnderreplicatedLedger underreplicatedLedger = new PulsarUnderreplicatedLedger(ledgerId);
            List<String> replicaList = underreplicatedLedgerFormat.getReplicaList();
            long ctime = (underreplicatedLedgerFormat.hasCtime() ? underreplicatedLedgerFormat.getCtime()
                    : UnderreplicatedLedger.UNASSIGNED_CTIME);
            underreplicatedLedger.setCtime(ctime);
            underreplicatedLedger.setReplicaList(replicaList);
            return underreplicatedLedger;
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting with metadata store", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting metadata store", ie);
        } catch (TextFormat.ParseException pe) {
            throw new ReplicationException.UnavailableException("Error parsing proto message", pe);
        }
    }

    @Override
    public CompletableFuture<Void> markLedgerUnderreplicatedAsync(long ledgerId, Collection<String> missingReplicas) {
        if (log.isDebugEnabled()) {
            log.debug("markLedgerUnderreplicated(ledgerId={}, missingReplica={})", ledgerId, missingReplicas);
        }
        final String path = getUrLedgerPath(ledgerId);
        final CompletableFuture<Void> createFuture = new CompletableFuture<>();
        tryMarkLedgerUnderreplicatedAsync(path, missingReplicas, createFuture);
        return createFuture;
    }

    private void tryMarkLedgerUnderreplicatedAsync(final String path,
                                                   final Collection<String> missingReplicas,
                                                   final CompletableFuture<Void> finalFuture) {
        final UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
        if (conf.getStoreSystemTimeAsLedgerUnderreplicatedMarkTime()) {
            builder.setCtime(System.currentTimeMillis());
        }
        missingReplicas.forEach(builder::addReplica);
        final byte[] urLedgerData = builder.build().toString().getBytes(UTF_8);
        store.put(path, urLedgerData, Optional.of(-1L))
                .thenRun(() -> {
                    FutureUtils.complete(finalFuture, null);
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof MetadataStoreException.BadVersionException) {
                        // we need to handle the case where the ledger has been marked as underreplicated
                        handleLedgerUnderreplicatedAlreadyMarked(path, missingReplicas, finalFuture);
                    } else {
                        FutureUtils.completeExceptionally(finalFuture, ex);
                    }
                    return null;
                });
    }


    private void handleLedgerUnderreplicatedAlreadyMarked(final String path,
                                                          final Collection<String> missingReplicas,
                                                          final CompletableFuture<Void> finalFuture) {
        // get the existing underreplicated ledger data
        store.get(path).thenAccept(optRes -> {
            if (!optRes.isPresent()) {
                tryMarkLedgerUnderreplicatedAsync(path, missingReplicas, finalFuture);
                return;
            }

            byte[] existingUrLedgerData = optRes.get().getValue();

            // deserialize existing underreplicated ledger data
            final UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
            try {
                TextFormat.merge(new String(existingUrLedgerData, UTF_8), builder);
            } catch (TextFormat.ParseException e) {
                // corrupted metadata in zookeeper
                FutureUtils.completeExceptionally(finalFuture,
                        new ReplicationException.UnavailableException(
                                "Invalid underreplicated ledger data for ledger " + path, e));
                return;
            }
            UnderreplicatedLedgerFormat existingUrLedgerFormat = builder.build();
            boolean replicaAdded = false;
            for (String missingReplica : missingReplicas) {
                if (existingUrLedgerFormat.getReplicaList().contains(missingReplica)) {
                    continue;
                } else {
                    builder.addReplica(missingReplica);
                    replicaAdded = true;
                }
            }
            if (!replicaAdded) { // no new missing replica is added
                FutureUtils.complete(finalFuture, null);
                return;
            }
            if (conf.getStoreSystemTimeAsLedgerUnderreplicatedMarkTime()) {
                builder.setCtime(System.currentTimeMillis());
            }
            final byte[] newUrLedgerData = builder.build().toString().getBytes(UTF_8);

            store.put(path, newUrLedgerData, Optional.of(optRes.get().getStat().getVersion()))
                    .thenRun(() -> {
                        FutureUtils.complete(finalFuture, null);
                    }).exceptionally(ex -> {
                        FutureUtils.completeExceptionally(finalFuture, ex);
                        return null;
                    });
        }).exceptionally(ex -> {
            FutureUtils.completeExceptionally(finalFuture, ex);
            return null;
        });
    }

    @Override
    public void acquireUnderreplicatedLedger(long ledgerId) throws ReplicationException {
        try {
            internalAcquireUnderreplicatedLedger(ledgerId);
        } catch (ExecutionException | InterruptedException e) {
            throw new ReplicationException.UnavailableException("Failed to acuire under-replicated ledger", e);
        }
    }

    private void internalAcquireUnderreplicatedLedger(long ledgerId) throws ExecutionException, InterruptedException {
        String lockPath = getUrLedgerLockPath(urLockPath, ledgerId);
        store.put(lockPath, LOCK_DATA, Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).get();
    }

    @Override
    public void markLedgerReplicated(long ledgerId) throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("markLedgerReplicated(ledgerId={})", ledgerId);
        }
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                store.delete(getUrLedgerPath(ledgerId), Optional.of(l.getLedgerNodeVersion())).get();
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof MetadataStoreException.NotFoundException) {
                // this is ok
            } else if (ee.getCause() instanceof MetadataStoreException.BadVersionException) {
                // if this is the case, some has marked the ledger
                // for rereplication again. Leave the underreplicated
                // znode in place, so the ledger is checked.
            } else {
                log.error("Error deleting underreplicated ledger node", ee);
                throw new ReplicationException.UnavailableException("Error contacting metadata store", ee);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting metadata store", ie);
        } finally {
            releaseUnderreplicatedLedger(ledgerId);
        }
    }

    /**
     * Get a list of all the underreplicated ledgers which have been
     * marked for rereplication, filtered by the predicate on the replicas list.
     *
     * <p>Replicas list of an underreplicated ledger is the list of the bookies which are part of
     * the ensemble of this ledger and are currently unavailable/down.
     *
     * @param predicate filter to use while listing under replicated ledgers. 'null' if filtering is not required.
     * @return an iterator which returns underreplicated ledgers.
     */
    @Override
    public Iterator<UnderreplicatedLedger> listLedgersToRereplicate(final Predicate<List<String>> predicate) {
        final Queue<String> queue = new LinkedList<>();
        queue.add(urLedgerPath);

        return new Iterator<UnderreplicatedLedger>() {
            final Queue<UnderreplicatedLedger> curBatch = new LinkedList<>();

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                if (curBatch.size() > 0) {
                    return true;
                }

                while (queue.size() > 0 && curBatch.size() == 0) {
                    String parent = queue.remove();
                    try {
                        for (String c : store.getChildren(parent).get()) {
                            String child = parent + "/" + c;
                            if (c.startsWith("urL")) {
                                long ledgerId = getLedgerId(child);
                                UnderreplicatedLedger underreplicatedLedger = getLedgerUnreplicationInfo(ledgerId);
                                if (underreplicatedLedger != null) {
                                    List<String> replicaList = underreplicatedLedger.getReplicaList();
                                    if ((predicate == null) || predicate.test(replicaList)) {
                                        curBatch.add(underreplicatedLedger);
                                    }
                                }
                            } else {
                                queue.add(child);
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    } catch (Exception e) {
                        throw new RuntimeException("Error reading list", e);
                    }
                }
                return curBatch.size() > 0;
            }

            @Override
            public UnderreplicatedLedger next() {
                assert curBatch.size() > 0;
                return curBatch.remove();
            }
        };
    }

    private long getLedgerToRereplicateFromHierarchy(String parent, long depth)
            throws ExecutionException, InterruptedException {
        if (depth == 4) {
            List<String> children = new ArrayList<>(store.getChildren(parent).get());
            Collections.shuffle(children);

            while (!children.isEmpty()) {
                String tryChild = children.get(0);
                try {
                    List<String> locks = store.getChildren(urLockPath).get();
                    if (locks.contains(tryChild)) {
                        children.remove(tryChild);
                        continue;
                    }

                    Optional<GetResult> optRes = store.get(parent + "/" + tryChild).get();
                    if (!optRes.isPresent()) {
                        if (log.isDebugEnabled()) {
                            log.debug("{}/{} doesn't exist", parent, tryChild);
                        }
                        children.remove(tryChild);
                        continue;
                    }

                    long ledgerId = getLedgerId(tryChild);
                    internalAcquireUnderreplicatedLedger(ledgerId);
                    String lockPath = getUrLedgerLockPath(urLockPath, ledgerId);
                    heldLocks.put(ledgerId, new Lock(lockPath, optRes.get().getStat().getVersion()));
                    return ledgerId;
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof MetadataStoreException.BadVersionException) {
                        // If we fail to acquire the lock because it's already taken, we should simply try with
                        // another ledger
                        children.remove(tryChild);
                    } else {
                        throw ee;
                    }
                } catch (NumberFormatException nfe) {
                    children.remove(tryChild);
                }
            }
            return -1;
        }

        List<String> children = new ArrayList<>(store.getChildren(parent).join());
        Collections.shuffle(children);

        while (children.size() > 0) {
            String tryChild = children.get(0);
            String tryPath = parent + "/" + tryChild;
            long ledger = getLedgerToRereplicateFromHierarchy(tryPath, depth + 1);
            if (ledger != -1) {
                return ledger;
            }
            children.remove(tryChild);
        }
        return -1;
    }


    @Override
    public long pollLedgerToRereplicate() throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("pollLedgerToRereplicate()");
        }
        try {
            return getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting metadata store", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting metadata store", ie);
        }
    }

    @Override
    public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("getLedgerToRereplicate()");
        }
        while (true) {
            try {
                waitIfLedgerReplicationDisabled();

                long ledger = getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
                if (ledger != -1) {
                    return ledger;
                }

                synchronized (this) {
                    // nothing found, wait for a watcher to trigger
                    this.wait(1000);
                }
            } catch (ExecutionException ee) {
                throw new ReplicationException.UnavailableException("Error contacting metadata store", ee);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new ReplicationException.UnavailableException("Interrupted while connecting metadata store", ie);
            }
        }
    }

    private void waitIfLedgerReplicationDisabled() throws ReplicationException.UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!this.isLedgerReplicationEnabled()) {
            this.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    @Override
    public void releaseUnderreplicatedLedger(long ledgerId) throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("releaseLedger(ledgerId={})", ledgerId);
        }
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                store.delete(l.getLockPath(), Optional.empty()).get();
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof MetadataStoreException.NotFoundException) {
                // this is ok
            } else {
                log.error("Error deleting underreplicated ledger lock", ee);
                throw new ReplicationException.UnavailableException("Error contacting metadata store", ee);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting metadata store", ie);
        }
        heldLocks.remove(ledgerId);
    }

    @Override
    public void close() throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("close()");
        }
        try {
            for (Map.Entry<Long, Lock> e : heldLocks.entrySet()) {
                store.delete(e.getValue().getLockPath(), Optional.empty()).get();
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof MetadataStoreException.NotFoundException) {
                // this is ok
            } else {
                log.error("Error deleting underreplicated ledger lock", ee);
                throw new ReplicationException.UnavailableException("Error contacting metadata store", ee);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting metadata store", ie);
        }
    }

    @Override
    public void disableLedgerReplication()
            throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("disableLedegerReplication()");
        }
        try {
            String path = basePath + '/' + BookKeeperConstants.DISABLE_NODE;
            store.put(path, "".getBytes(UTF_8), Optional.of(-1L)).get();
            log.info("Auto ledger re-replication is disabled!");
        } catch (ExecutionException ee) {
            log.error("Exception while stopping auto ledger re-replication", ee);
            throw new ReplicationException.UnavailableException(
                    "Exception while stopping auto ledger re-replication", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while stopping auto ledger re-replication", ie);
        }
    }

    @Override
    public void enableLedgerReplication()
            throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("enableLedegerReplication()");
        }
        try {
            store.delete(basePath + '/' + BookKeeperConstants.DISABLE_NODE, Optional.empty()).get();
            log.info("Resuming automatic ledger re-replication");
        } catch (ExecutionException ee) {
            log.error("Exception while resuming ledger replication", ee);
            throw new ReplicationException.UnavailableException(
                    "Exception while resuming auto ledger re-replication", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while resuming auto ledger re-replication", ie);
        }
    }

    @Override
    public boolean isLedgerReplicationEnabled()
            throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("isLedgerReplicationEnabled()");
        }
        try {
            return !store.exists(basePath + '/' + BookKeeperConstants.DISABLE_NODE).get();
        } catch (ExecutionException ee) {
            log.error("Error while checking the state of "
                    + "ledger re-replication", ee);
            throw new ReplicationException.UnavailableException(
                    "Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLedgerReplicationEnabled(final BookkeeperInternalCallbacks.GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("notifyLedgerReplicationEnabled()");
        }

        synchronized (this) {
            replicationEnabledListener = cb;
        }

        try {
            if (!store.exists(basePath + '/' + BookKeeperConstants.DISABLE_NODE).get()) {
                log.info("LedgerReplication is enabled externally through metadata store, "
                        + "since DISABLE_NODE node is deleted");
                cb.operationComplete(0, null);
                return;
            }
        } catch (ExecutionException ee) {
            log.error("Error while checking the state of "
                    + "ledger re-replication", ee);
            throw new ReplicationException.UnavailableException(
                    "Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    /**
     * Check whether the ledger is being replicated by any bookie.
     */
    @Override
    public boolean isLedgerBeingReplicated(long ledgerId) throws ReplicationException {
        try {
            return store.exists(getUrLedgerLockPath(urLockPath, ledgerId)).get();
        } catch (Exception e) {
            throw new ReplicationException.UnavailableException("Failed to check if ledger is beinge replicated", e);
        }
    }

    @Override
    public boolean initializeLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws
            ReplicationException.UnavailableException {
        log.debug("initializeLostBookieRecoveryDelay()");
        try {
            store.put(lostBookieRecoveryDelayPath, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                    Optional.of(-1L)).get();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof MetadataStoreException.BadVersionException) {
                log.info("lostBookieRecoveryDelay node is already present, so using "
                        + "existing lostBookieRecoveryDelay node value");
                return false;
            } else {
                log.error("Error while initializing LostBookieRecoveryDelay", ee);
                throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
        return true;
    }

    @Override
    public void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws
            ReplicationException.UnavailableException {
        log.debug("setLostBookieRecoveryDelay()");
        try {
            store.put(lostBookieRecoveryDelayPath, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                    Optional.empty()).get();

        } catch (ExecutionException ee) {
            log.error("Error while setting LostBookieRecoveryDelay ", ee);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public int getLostBookieRecoveryDelay() throws ReplicationException.UnavailableException {
        log.debug("getLostBookieRecoveryDelay()");
        try {
            byte[] data = store.get(lostBookieRecoveryDelayPath).get().get().getValue();
            return Integer.parseInt(new String(data, UTF_8));
        } catch (ExecutionException ee) {
            log.error("Error while getting LostBookieRecoveryDelay ", ee);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLostBookieRecoveryDelayChanged(BookkeeperInternalCallbacks.GenericCallback<Void> cb) throws
            ReplicationException.UnavailableException {
        log.debug("notifyLostBookieRecoveryDelayChanged()");
        synchronized (this) {
            lostBookieRecoveryDelayListener = cb;
        }
        try {
            if (!store.exists(lostBookieRecoveryDelayPath).get()) {
                cb.operationComplete(0, null);
                return;
            }

        } catch (ExecutionException ee) {
            log.error("Error while checking the state of lostBookieRecoveryDelay", ee);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public String getReplicationWorkerIdRereplicatingLedger(long ledgerId)
            throws ReplicationException.UnavailableException {

        try {
            Optional<GetResult> optRes = store.get(getUrLedgerLockPath(urLockPath, ledgerId)).get();
            if (!optRes.isPresent()) {
                // this is ok.
                return null;
            }

            byte[] lockData = optRes.get().getValue();
            LockDataFormat.Builder lockDataBuilder = LockDataFormat.newBuilder();
            TextFormat.merge(new String(lockData, UTF_8), lockDataBuilder);
            LockDataFormat lock = lockDataBuilder.build();
            return lock.getBookieId();
        } catch (ExecutionException e) {
            log.error("Error while getting ReplicationWorkerId rereplicating Ledger", e);
            throw new ReplicationException.UnavailableException(
                    "Error while getting ReplicationWorkerId rereplicating Ledger", e);
        } catch (InterruptedException e) {
            log.error("Got interrupted while getting ReplicationWorkerId rereplicating Ledger", e);
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", e);
        } catch (TextFormat.ParseException e) {
            log.error("Error while parsing ZK data of lock", e);
            throw new ReplicationException.UnavailableException("Error while parsing ZK data of lock", e);
        }
    }

    @Override
    public void setCheckAllLedgersCTime(long checkAllLedgersCTime) throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("setCheckAllLedgersCTime");
        }
        try {
            CheckAllLedgersFormat.Builder builder = CheckAllLedgersFormat.newBuilder();
            builder.setCheckAllLedgersCTime(checkAllLedgersCTime);
            byte[] checkAllLedgersFormatByteArray = builder.build().toByteArray();

            store.put(checkAllLedgersCtimePath, checkAllLedgersFormatByteArray, Optional.empty()).get();
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getCheckAllLedgersCTime() throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("setCheckAllLedgersCTime");
        }
        try {
            Optional<GetResult> optRes = store.get(checkAllLedgersCtimePath).get();
            if (!optRes.isPresent()) {
                log.warn("checkAllLedgersCtimeZnode is not yet available");
                return -1;
            }
            byte[] data = optRes.get().getValue();
            CheckAllLedgersFormat checkAllLedgersFormat = CheckAllLedgersFormat.parseFrom(data);
            return checkAllLedgersFormat.hasCheckAllLedgersCTime() ? checkAllLedgersFormat.getCheckAllLedgersCTime()
                    : -1;
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }

    @Override
    public void setPlacementPolicyCheckCTime(long placementPolicyCheckCTime) throws
            ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("setPlacementPolicyCheckCTime");
        }
        try {
            PlacementPolicyCheckFormat.Builder builder = PlacementPolicyCheckFormat.newBuilder();
            builder.setPlacementPolicyCheckCTime(placementPolicyCheckCTime);
            byte[] placementPolicyCheckFormatByteArray = builder.build().toByteArray();
            store.put(placementPolicyCheckCtimePath, placementPolicyCheckFormatByteArray, Optional.empty()).get();
        } catch (ExecutionException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getPlacementPolicyCheckCTime() throws ReplicationException.UnavailableException {
        if (log.isDebugEnabled()) {
            log.debug("getPlacementPolicyCheckCTime");
        }
        try {
            Optional<GetResult> optRes = store.get(placementPolicyCheckCtimePath).get();
            if (!optRes.isPresent()) {
                log.warn("placementPolicyCheckCtimeZnode is not yet available");
                return -1;
            }
            byte[] data = optRes.get().getValue();
            PlacementPolicyCheckFormat placementPolicyCheckFormat = PlacementPolicyCheckFormat.parseFrom(data);
            return placementPolicyCheckFormat.hasPlacementPolicyCheckCTime()
                    ? placementPolicyCheckFormat.getPlacementPolicyCheckCTime() : -1;
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }

    @Override
    public void setReplicasCheckCTime(long replicasCheckCTime) throws ReplicationException.UnavailableException {
        try {
            ReplicasCheckFormat.Builder builder = ReplicasCheckFormat.newBuilder();
            builder.setReplicasCheckCTime(replicasCheckCTime);
            byte[] replicasCheckFormatByteArray = builder.build().toByteArray();
            store.put(replicasCheckCtimePath, replicasCheckFormatByteArray, Optional.empty()).get();
            if (log.isDebugEnabled()) {
                log.debug("setReplicasCheckCTime completed successfully");
            }
        } catch (ExecutionException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getReplicasCheckCTime() throws ReplicationException.UnavailableException {
        try {
            Optional<GetResult> optRes = store.get(replicasCheckCtimePath).get();
            if (!optRes.isPresent()) {
                log.warn("placementPolicyCheckCtimeZnode is not yet available");
                return -1;
            }
            byte[] data = optRes.get().getValue();
            ReplicasCheckFormat replicasCheckFormat = ReplicasCheckFormat.parseFrom(data);
            if (log.isDebugEnabled()) {
                log.debug("getReplicasCheckCTime completed successfully");
            }
            return replicasCheckFormat.hasReplicasCheckCTime() ? replicasCheckFormat.getReplicasCheckCTime() : -1;
        } catch (ExecutionException ee) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }
}
