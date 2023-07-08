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
import static org.apache.bookkeeper.util.BookKeeperConstants.EMPTY_BYTE_ARRAY;
import static org.apache.bookkeeper.util.BookKeeperConstants.INSTANCEID;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.BookieIllegalOpException;
import org.apache.bookkeeper.bookie.BookieException.CookieExistException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats.BookieServiceInfoFormat;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Fault injectable ZK registration manager.
 * Copy from #{@link org.apache.bookkeeper.discover.ZKRegistrationManager}.
 */
@Slf4j
public class FaultInjectableZKRegistrationManager implements RegistrationManager {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            return new MetaStoreException();
        }
    };

    private final ServerConfiguration conf;
    private final ZooKeeper zk;
    private final List<ACL> zkAcls;
    private final LayoutManager layoutManager;

    private volatile boolean zkRegManagerInitialized = false;

    // ledgers root path
    private final String ledgersRootPath;
    // cookie path
    private final String cookiePath;
    // registration paths
    protected final String bookieRegistrationPath;
    protected final String bookieReadonlyRegistrationPath;
    // session timeout in milliseconds
    private final int zkTimeoutMs;
    private final List<RegistrationListener> listeners = new ArrayList<>();
    private Function<Void, Void> hookOnRegisterReadOnly;

    public FaultInjectableZKRegistrationManager(ServerConfiguration conf,
                                                ZooKeeper zk) {
        this(conf, zk, ZKMetadataDriverBase.resolveZkLedgersRootPath(conf));
    }

    public FaultInjectableZKRegistrationManager(ServerConfiguration conf,
                                                ZooKeeper zk,
                                                String ledgersRootPath) {
        this.conf = conf;
        this.zk = zk;
        this.zkAcls = ZkUtils.getACLs(conf);
        this.ledgersRootPath = ledgersRootPath;
        this.cookiePath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
        this.zkTimeoutMs = conf.getZkTimeout();

        this.layoutManager = new ZkLayoutManager(
                zk,
                ledgersRootPath,
                zkAcls);

        this.zk.register(event -> {
            if (!zkRegManagerInitialized) {
                // do nothing until first registration
                return;
            }
            // Check for expired connection.
            if (event.getType().equals(EventType.None)
                    && event.getState().equals(KeeperState.Expired)) {
                listeners.forEach(RegistrationListener::onRegistrationExpired);
            }
        });
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Returns the CookiePath of the bookie in the ZooKeeper.
     *
     * @param bookieId bookie id
     * @return
     */
    public String getCookiePath(BookieId bookieId) {
        return this.cookiePath + "/" + bookieId;
    }

    //
    // Registration Management
    //

    /**
     * Check existence of <i>regPath</i> and wait it expired if possible.
     *
     * @param regPath reg node path.
     * @return true if regPath exists, otherwise return false
     * @throws IOException if can't create reg path
     */
    protected boolean checkRegNodeAndWaitExpired(String regPath) throws IOException {
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);
        Watcher zkPrevRegNodewatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Check for prev znode deletion. Connection expiration is
                // not handling, since bookie has logic to shutdown.
                if (EventType.NodeDeleted == event.getType()) {
                    prevNodeLatch.countDown();
                }
            }
        };
        try {
            Stat stat = zk.exists(regPath, zkPrevRegNodewatcher);
            if (null != stat) {
                // if the ephemeral owner isn't current zookeeper client
                // wait for it to be expired.
                if (stat.getEphemeralOwner() != zk.getSessionId()) {
                    log.info("Previous bookie registration znode: {} exists, so waiting zk sessiontimeout:"
                            + " {} ms for znode deletion", regPath, zkTimeoutMs);
                    // waiting for the previous bookie reg znode deletion
                    if (!prevNodeLatch.await(zkTimeoutMs, TimeUnit.MILLISECONDS)) {
                        throw new NodeExistsException(regPath);
                    } else {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (KeeperException ke) {
            log.error("ZK exception checking and wait ephemeral znode {} expired : ", regPath, ke);
            throw new IOException("ZK exception checking and wait ephemeral znode "
                    + regPath + " expired", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Interrupted checking and wait ephemeral znode {} expired : ", regPath, ie);
            throw new IOException("Interrupted checking and wait ephemeral znode "
                    + regPath + " expired", ie);
        }
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly,
                               BookieServiceInfo bookieServiceInfo) throws BookieException {
        if (!readOnly) {
            String regPath = bookieRegistrationPath + "/" + bookieId;
            doRegisterBookie(regPath, bookieServiceInfo);
        } else {
            doRegisterReadOnlyBookie(bookieId, bookieServiceInfo);
        }
    }

    @VisibleForTesting
    static byte[] serializeBookieServiceInfo(BookieServiceInfo bookieServiceInfo) {
        if (log.isDebugEnabled()) {
            log.debug("serialize BookieServiceInfo {}", bookieServiceInfo);
        }
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BookieServiceInfoFormat.Builder builder = BookieServiceInfoFormat.newBuilder();
            List<BookieServiceInfoFormat.Endpoint> bsiEndpoints = bookieServiceInfo.getEndpoints().stream()
                    .map(e -> {
                        return BookieServiceInfoFormat.Endpoint.newBuilder()
                                .setId(e.getId())
                                .setPort(e.getPort())
                                .setHost(e.getHost())
                                .setProtocol(e.getProtocol())
                                .addAllAuth(e.getAuth())
                                .addAllExtensions(e.getExtensions())
                                .build();
                    })
                    .collect(Collectors.toList());

            builder.addAllEndpoints(bsiEndpoints);
            builder.putAllProperties(bookieServiceInfo.getProperties());

            builder.build().writeTo(os);
            return os.toByteArray();
        } catch (IOException err) {
            log.error("Cannot serialize bookieServiceInfo from " + bookieServiceInfo);
            throw new RuntimeException(err);
        }
    }

    private void doRegisterBookie(String regPath, BookieServiceInfo bookieServiceInfo) throws BookieException {
        // ZK ephemeral node for this Bookie.
        try {
            if (!checkRegNodeAndWaitExpired(regPath)) {
                // Create the ZK ephemeral node for this Bookie.
                zk.create(regPath, serializeBookieServiceInfo(bookieServiceInfo), zkAcls, CreateMode.EPHEMERAL);
                zkRegManagerInitialized = true;
            }
        } catch (KeeperException ke) {
            log.error("ZK exception registering ephemeral Znode for Bookie!", ke);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new MetadataStoreException(ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Interrupted exception registering ephemeral Znode for Bookie!", ie);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new MetadataStoreException(ie);
        } catch (IOException e) {
            throw new MetadataStoreException(e);
        }
    }

    private void doRegisterReadOnlyBookie(BookieId bookieId, BookieServiceInfo bookieServiceInfo)
            throws BookieException {
        try {
            if (null == zk.exists(this.bookieReadonlyRegistrationPath, false)) {
                try {
                    zk.create(this.bookieReadonlyRegistrationPath, serializeBookieServiceInfo(bookieServiceInfo),
                            zkAcls, CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }
            String regPath = bookieReadonlyRegistrationPath + "/" + bookieId;
            doRegisterBookie(regPath, bookieServiceInfo);
            // clear the write state
            regPath = bookieRegistrationPath + "/" + bookieId;
            try {
                if (hookOnRegisterReadOnly != null) {
                    hookOnRegisterReadOnly.apply(null);
                }
                // Clear the current registered node
                zk.delete(regPath, -1);
            } catch (KeeperException.NoNodeException nne) {
                log.warn("No writable bookie registered node {} when transitioning to readonly",
                        regPath, nne);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        String regPath;
        if (!readOnly) {
            regPath = bookieRegistrationPath + "/" + bookieId;
        } else {
            regPath = bookieReadonlyRegistrationPath + "/" + bookieId;
        }
        doUnregisterBookie(regPath);
    }

    private void doUnregisterBookie(String regPath) throws BookieException {
        try {
            zk.delete(regPath, -1);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException(ie);
        } catch (KeeperException e) {
            throw new MetadataStoreException(e);
        }
    }

    //
    // Cookie Management
    //

    @Override
    public void writeCookie(BookieId bookieId,
                            Versioned<byte[]> cookieData) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            if (Version.NEW == cookieData.getVersion()) {
                if (zk.exists(cookiePath, false) == null) {
                    try {
                        zk.create(cookiePath, new byte[0], zkAcls, CreateMode.PERSISTENT);
                    } catch (NodeExistsException nne) {
                        log.info("More than one bookie tried to create {} at once. Safe to ignore.",
                                cookiePath);
                    }
                }
                zk.create(zkPath, cookieData.getValue(), zkAcls, CreateMode.PERSISTENT);
            } else {
                if (!(cookieData.getVersion() instanceof LongVersion)) {
                    throw new BookieIllegalOpException("Invalid version type, expected it to be LongVersion");
                }
                zk.setData(
                        zkPath,
                        cookieData.getValue(),
                        (int) ((LongVersion) cookieData.getVersion()).getLongVersion());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException("Interrupted writing cookie for bookie " + bookieId, ie);
        } catch (NoNodeException nne) {
            throw new CookieNotFoundException(bookieId.toString());
        } catch (NodeExistsException nee) {
            throw new CookieExistException(bookieId.toString());
        } catch (KeeperException e) {
            throw new MetadataStoreException("Failed to write cookie for bookie " + bookieId);
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            Stat stat = zk.exists(zkPath, false);
            byte[] data = zk.getData(zkPath, false, stat);
            // sets stat version from ZooKeeper
            LongVersion version = new LongVersion(stat.getVersion());
            return new Versioned<>(data, version);
        } catch (NoNodeException nne) {
            throw new CookieNotFoundException(bookieId.toString());
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException("Failed to read cookie for bookie " + bookieId);
        }
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        String zkPath = getCookiePath(bookieId);
        try {
            zk.delete(zkPath, (int) ((LongVersion) version).getLongVersion());
        } catch (NoNodeException e) {
            throw new CookieNotFoundException(bookieId.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException("Interrupted deleting cookie for bookie " + bookieId, e);
        } catch (KeeperException e) {
            throw new MetadataStoreException("Failed to delete cookie for bookie " + bookieId);
        }

        log.info("Removed cookie from {} for bookie {}.", cookiePath, bookieId);
    }


    @Override
    public String getClusterInstanceId() throws BookieException {
        String instanceId = null;
        try {
            if (zk.exists(ledgersRootPath, null) == null) {
                log.error("BookKeeper metadata doesn't exist in zookeeper. "
                        + "Has the cluster been initialized? "
                        + "Try running bin/bookkeeper shell metaformat");
                throw new KeeperException.NoNodeException("BookKeeper metadata");
            }
            try {
                byte[] data = zk.getData(ledgersRootPath + "/"
                        + INSTANCEID, false, null);
                instanceId = new String(data, UTF_8);
            } catch (KeeperException.NoNodeException e) {
                log.info("INSTANCEID not exists in zookeeper. Not considering it for data verification");
            }
        } catch (KeeperException | InterruptedException e) {
            throw new MetadataStoreException("Failed to get cluster instance id", e);
        }
        return instanceId;
    }

    @Override
    public boolean prepareFormat() throws Exception {
        boolean ledgerRootExists = null != zk.exists(ledgersRootPath, false);
        boolean availableNodeExists = null != zk.exists(bookieRegistrationPath, false);
        // Create ledgers root node if not exists
        if (!ledgerRootExists) {
            ZkUtils.createFullPathOptimistic(zk, ledgersRootPath, "".getBytes(StandardCharsets.UTF_8), zkAcls,
                    CreateMode.PERSISTENT);
        }
        // create available bookies node if not exists
        if (!availableNodeExists) {
            zk.create(bookieRegistrationPath, "".getBytes(StandardCharsets.UTF_8), zkAcls, CreateMode.PERSISTENT);
        }

        // create readonly bookies node if not exists
        if (null == zk.exists(bookieReadonlyRegistrationPath, false)) {
            zk.create(bookieReadonlyRegistrationPath, new byte[0], zkAcls, CreateMode.PERSISTENT);
        }

        return ledgerRootExists;
    }

    @Override
    public boolean initNewCluster() throws Exception {
        String zkServers = ZKMetadataDriverBase.resolveZkServers(conf);
        String instanceIdPath = ledgersRootPath + "/" + INSTANCEID;
        log.info("Initializing ZooKeeper metadata for new cluster, ZKServers: {} ledger root path: {}", zkServers,
                ledgersRootPath);

        boolean ledgerRootExists = null != zk.exists(ledgersRootPath, false);

        if (ledgerRootExists) {
            log.error("Ledger root path: {} already exists", ledgersRootPath);
            return false;
        }

        List<Op> multiOps = Lists.newArrayListWithExpectedSize(4);

        // Create ledgers root node
        multiOps.add(Op.create(ledgersRootPath, EMPTY_BYTE_ARRAY, zkAcls, CreateMode.PERSISTENT));

        // create available bookies node
        multiOps.add(Op.create(bookieRegistrationPath, EMPTY_BYTE_ARRAY, zkAcls, CreateMode.PERSISTENT));

        // create readonly bookies node
        multiOps.add(Op.create(
                bookieReadonlyRegistrationPath,
                EMPTY_BYTE_ARRAY,
                zkAcls,
                CreateMode.PERSISTENT));

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        multiOps.add(Op.create(instanceIdPath, instanceId.getBytes(UTF_8),
                zkAcls, CreateMode.PERSISTENT));

        // execute the multi ops
        zk.multi(multiOps);

        // creates the new layout and stores in zookeeper
        AbstractZkLedgerManagerFactory.newLedgerManagerFactory(conf, layoutManager);

        log.info("Successfully initiated cluster. ZKServers: {} ledger root path: {} instanceId: {}", zkServers,
                ledgersRootPath, instanceId);
        return true;
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        String zkServers = ZKMetadataDriverBase.resolveZkServers(conf);
        log.info("Nuking ZooKeeper metadata of existing cluster, ZKServers: {} ledger root path: {}",
                zkServers, ledgersRootPath);

        boolean ledgerRootExists = null != zk.exists(ledgersRootPath, false);
        if (!ledgerRootExists) {
            log.info("There is no existing cluster with ledgersRootPath: {} in ZKServers: {}, "
                    + "so exiting nuke operation", ledgersRootPath, zkServers);
            return true;
        }

        boolean availableNodeExists = null != zk.exists(bookieRegistrationPath, false);
        try (RegistrationClient regClient = new ZKRegistrationClient(
                zk,
                ledgersRootPath,
                null,
                false
        )) {
            if (availableNodeExists) {
                Collection<BookieId> rwBookies = FutureUtils
                        .result(regClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
                if (rwBookies != null && !rwBookies.isEmpty()) {
                    log.error("Bookies are still up and connected to this cluster, "
                            + "stop all bookies before nuking the cluster");
                    return false;
                }

                boolean readonlyNodeExists = null != zk.exists(bookieReadonlyRegistrationPath, false);
                if (readonlyNodeExists) {
                    Collection<BookieId> roBookies = FutureUtils
                            .result(regClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
                    if (roBookies != null && !roBookies.isEmpty()) {
                        log.error("Readonly Bookies are still up and connected to this cluster, "
                                + "stop all bookies before nuking the cluster");
                        return false;
                    }
                }
            }
        }

        LedgerManagerFactory ledgerManagerFactory =
                AbstractZkLedgerManagerFactory.newLedgerManagerFactory(conf, layoutManager);
        return ledgerManagerFactory.validateAndNukeExistingCluster(conf, layoutManager);
    }

    @Override
    public boolean format() throws Exception {
        // Clear underreplicated ledgers
        try {
            ZKUtil.deleteRecursive(zk, ZkLedgerUnderreplicationManager.getBasePath(ledgersRootPath)
                    + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("underreplicated ledgers root path node not exists in zookeeper to delete");
            }
        }

        // Clear underreplicatedledger locks
        try {
            ZKUtil.deleteRecursive(zk, ZkLedgerUnderreplicationManager.getBasePath(ledgersRootPath) + '/'
                    + BookKeeperConstants.UNDER_REPLICATION_LOCK);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("underreplicatedledger locks node not exists in zookeeper to delete");
            }
        }

        // Clear the cookies
        try {
            ZKUtil.deleteRecursive(zk, cookiePath);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("cookies node not exists in zookeeper to delete");
            }
        }

        // Clear the INSTANCEID
        try {
            zk.delete(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID, -1);
        } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) {
                log.debug("INSTANCEID not exists in zookeeper to delete");
            }
        }

        // create INSTANCEID
        String instanceId = UUID.randomUUID().toString();
        zk.create(ledgersRootPath + "/" + BookKeeperConstants.INSTANCEID,
                instanceId.getBytes(StandardCharsets.UTF_8), zkAcls, CreateMode.PERSISTENT);

        log.info("Successfully formatted BookKeeper metadata");
        return true;
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        String regPath = bookieRegistrationPath + "/" + bookieId;
        String readonlyRegPath = bookieReadonlyRegistrationPath + "/" + bookieId;
        try {
            return ((null != zk.exists(regPath, false)) || (null != zk.exists(readonlyRegPath, false)));
        } catch (KeeperException e) {
            log.error("ZK exception while checking registration ephemeral znodes for BookieId: {}", bookieId, e);
            throw new MetadataStoreException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("InterruptedException while checking registration ephemeral znodes for BookieId: {}", bookieId,
                    e);
            throw new MetadataStoreException(e);
        }
    }

    public void betweenRegisterReadOnlyBookie(Function<Void, Void> fn) {
        hookOnRegisterReadOnly = fn;
    }
}
