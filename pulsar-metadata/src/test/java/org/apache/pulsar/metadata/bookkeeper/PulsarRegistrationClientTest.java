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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookieWatcher;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
@CustomLog
public class PulsarRegistrationClientTest extends BaseMetadataStoreTest {

    private static Set<BookieId> prepareNBookies(int num) {
        Set<BookieId> bookies = new HashSet<>();
        for (int i = 0; i < num; i++) {
            bookies.add(new BookieSocketAddress("127.0.0.1", 3181 + i).toBookieId());
        }
        return bookies;
    }

    @Test(dataProvider = "impl")
    public void testGetWritableBookies(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = new ArrayList<>();
        for (BookieId address : addresses) {
            children.add(address.toString());
            rm.registerBookie(address, false, new BookieServiceInfo());
        }

        Versioned<Set<BookieId>> result = result(rc.getWritableBookies());

        assertEquals(result.getValue().size(), addresses.size());
    }


    @Test(dataProvider = "impl")
    public void testGetReadonlyBookies(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = new ArrayList<>();
        for (BookieId address : addresses) {
            children.add(address.toString());
            rm.registerBookie(address, true, new BookieServiceInfo());
        }

        Versioned<Set<BookieId>> result = result(rc.getReadOnlyBookies());

        assertEquals(result.getValue().size(), addresses.size());
    }

    @Test(dataProvider = "impl")
    public void testGetBookieServiceInfo(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        List<BookieId> addresses = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            addresses.add(BookieId.parse("BOOKIE-" + i));
        }
        Map<BookieId, BookieServiceInfo> bookieServiceInfos = new HashMap<>();
        Set<BookieId> readOnlyBookies = new HashSet<>();
        int port = 223;
        for (BookieId address : addresses) {
            BookieServiceInfo info = new BookieServiceInfo();
            BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
            endpoint.setAuth(Collections.emptyList());
            endpoint.setExtensions(Collections.emptyList());
            endpoint.setId("id");
            endpoint.setHost("localhost");
            endpoint.setPort(port++);
            endpoint.setProtocol("bookie-rpc");
            info.setEndpoints(Arrays.asList(endpoint));
            bookieServiceInfos.put(address, info);
            // some readonly, some writable
            boolean readOnly = port % 2 == 0;
            if (readOnly) {
                readOnlyBookies.add(address);
            }
            rm.registerBookie(address, readOnly, info);
            // write the cookie
            rm.writeCookie(address, new Versioned<>(new byte[0], Version.NEW));
        }

        // trigger loading the BookieServiceInfo in the local cache
        getAndVerifyAllBookies(rc, addresses);

        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                BookieServiceInfo bookieServiceInfo = rc.getBookieServiceInfo(address).get().getValue();
                compareBookieServiceInfo(bookieServiceInfo, bookieServiceInfos.get(address));
            }});

        // shutdown the bookies (but keep the cookie)
        for (BookieId address : addresses) {
            rm.unregisterBookie(address, readOnlyBookies.contains(address));
            readOnlyBookies.remove(address);
        }

        // getAllBookies should find all the bookies in any case (it reads the cookies)
        getAndVerifyAllBookies(rc, addresses);

        // getBookieServiceInfo should fail with BKBookieHandleNotAvailableException
        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                assertTrue(
                        expectThrows(ExecutionException.class, () -> {
                            rc.getBookieServiceInfo(address).get();
                        }).getCause() instanceof BKException.BKBookieHandleNotAvailableException);
            }});


        // restart the bookies, all writable
        // we 'register' the bookie, but do not write the cookie again
        for (BookieId address : addresses) {
            rm.registerBookie(address, false, bookieServiceInfos.get(address));
        }

        getAndVerifyAllBookies(rc, addresses);

        // verify that infos are available again
        Awaitility.await()
                .ignoreExceptionsMatching(e -> e.getCause() instanceof BKException.BKBookieHandleNotAvailableException)
                .untilAsserted(() -> {
                    for (BookieId address : addresses) {
                        BookieServiceInfo bookieServiceInfo = rc.getBookieServiceInfo(address).get().getValue();
                        compareBookieServiceInfo(bookieServiceInfo, bookieServiceInfos.get(address));
                    }
                });

        // update the infos
        port = 111;
        for (BookieId address : addresses) {
            BookieServiceInfo info = new BookieServiceInfo();
            BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
            endpoint.setAuth(Collections.emptyList());
            endpoint.setExtensions(Collections.emptyList());
            endpoint.setId("id");
            endpoint.setHost("localhost");
            endpoint.setPort(port++);
            endpoint.setProtocol("bookie-rpc");
            info.setEndpoints(Arrays.asList(endpoint));
            bookieServiceInfos.put(address, info);
            // some readonly, some writable
            boolean readOnly = port % 2 == 0;

            // remove the previous info from the metadata service
            rm.unregisterBookie(address, readOnlyBookies.contains(address));

            rm.registerBookie(address, readOnly, info);

            if (readOnly) {
                readOnlyBookies.add(address);
            }
        }

        // verify that the client tracked the changes
        Awaitility
                .await()
                .ignoreExceptionsMatching(e -> e.getCause() instanceof BKException.BKBookieHandleNotAvailableException)
                .untilAsserted(() -> {
                    // verify that infos are updated
                    for (BookieId address : addresses) {
                        BookieServiceInfo bookieServiceInfo = rc.getBookieServiceInfo(address).get().getValue();
                        compareBookieServiceInfo(bookieServiceInfo, bookieServiceInfos.get(address));
                    }
                });

    }

    private static void getAndVerifyAllBookies(RegistrationClient rc, List<BookieId> addresses)
            throws InterruptedException, ExecutionException {
        Set<BookieId> all = rc.getAllBookies().get().getValue();
        assertEquals(all.size(), addresses.size());
        for (BookieId id : all) {
            assertTrue(addresses.contains(id));
        }
        for (BookieId id : addresses) {
            assertTrue(all.contains(id));
        }
    }

    private void compareBookieServiceInfo(BookieServiceInfo a, BookieServiceInfo b) {
        assertEquals(a.getProperties(), b.getProperties());
        assertEquals(a.getEndpoints().size(), b.getEndpoints().size());
        for (int i = 0; i < a.getEndpoints().size(); i++) {
            BookieServiceInfo.Endpoint e1 = a.getEndpoints().get(i);
            BookieServiceInfo.Endpoint e2 = b.getEndpoints().get(i);
            assertEquals(e1.getHost(), e2.getHost());
            assertEquals(e1.getPort(), e2.getPort());
            assertEquals(e1.getId(), e2.getId());
            assertEquals(e1.getProtocol(), e2.getProtocol());
            assertEquals(e1.getExtensions(), e2.getExtensions());
            assertEquals(e1.getAuth(), e2.getAuth());
        }

    }

    @Test(dataProvider = "impl")
    public void testGetAllBookies(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = new ArrayList<>();
        for (BookieId address : addresses) {
            children.add(address.toString());
            rm.writeCookie(address, new Versioned<>(new byte[0], Version.NEW));
        }

        Versioned<Set<BookieId>> result = result(rc.getAllBookies());
        assertEquals(result.getValue().size(), addresses.size());
    }

    @Test(dataProvider = "readOnly")
    public void testBookieInfoReloadedAfterFailedRead(boolean readOnly) throws Exception {
        @Cleanup
        FailingReadsMetadataStore store = new FailingReadsMetadataStore("memory:" + UUID.randomUUID());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        BookieId bookie = new BookieSocketAddress("127.0.0.1", 3181).toBookieId();
        String registrationPath = ledgersRoot + "/" + AVAILABLE_NODE + (readOnly ? "/" + READONLY : "") + "/" + bookie;

        // The read of the bookie info, triggered by the registration notification, fails
        CompletableFuture<Void> failedRead = store.failNextRead(registrationPath);
        CompletableFuture<Void> releaseReload = new CompletableFuture<>();
        store.delayNextRead(registrationPath, releaseReload);
        try {
            rm.registerBookie(bookie, readOnly, new BookieServiceInfo());
            failedRead.get(10, TimeUnit.SECONDS);

            // Keep the reload pending so a scheduled watcher retry cannot fill the cache before this assertion.
            assertTrue(expectThrows(ExecutionException.class,
                    () -> rc.getBookieServiceInfo(bookie).get(10, TimeUnit.SECONDS))
                    .getCause() instanceof BKException.BKBookieHandleNotAvailableException);
        } finally {
            releaseReload.complete(null);
        }

        // The miss reloads the bookie info, so a retry finds it
        Awaitility.await().ignoreExceptions().untilAsserted(() -> rc.getBookieServiceInfo(bookie).get());
    }

    @DataProvider(name = "readOnly")
    public static Object[][] readOnly() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "readOnly")
    public void testBookieWatcherRecoversAfterFailedRead(boolean readOnly) throws Exception {
        @Cleanup
        FailingReadsMetadataStore store = new FailingReadsMetadataStore("memory:" + UUID.randomUUID());
        String ledgersRoot = "/ledgers";
        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, new ServerConfiguration());

        PulsarMetadataClientDriver.init();
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri("metadata-store:memory:local")
                .setNumWorkerThreads(1)
                .setNumIOThreads(1);
        conf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        @Cleanup
        BookKeeper bk = new BookKeeper(conf);
        BookieWatcher watcher = bk.getClientCtx().getBookieWatcher();
        BookieSocketAddress address = new BookieSocketAddress("127.0.0.1", 3181);
        BookieId bookie = address.toBookieId();
        assertThat(watcher.isBookieUnavailable(bookie)).isTrue();

        BookieServiceInfo info = new BookieServiceInfo();
        BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
        endpoint.setId("bookie");
        endpoint.setHost("127.0.0.1");
        endpoint.setPort(3181);
        endpoint.setProtocol("bookie-rpc");
        endpoint.setAuth(Collections.emptyList());
        endpoint.setExtensions(Collections.emptyList());
        info.setEndpoints(List.of(endpoint));
        String path = ledgersRoot + "/" + AVAILABLE_NODE + (readOnly ? "/" + READONLY : "") + "/" + bookie;
        CompletableFuture<Void> failedRead = store.failNextRead(path);
        rm.registerBookie(bookie, readOnly, info);
        failedRead.get(10, TimeUnit.SECONDS);

        // Address resolution can recover through #27052 without delivering the dropped membership update.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() ->
                assertThat(watcher.getBookieAddressResolver().resolve(bookie)).isEqualTo(address));
        if (readOnly) {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(watcher.isBookieUnavailable(bookie)).as("registered read-only bookie").isFalse());
            expectThrows(BKException.BKNotEnoughBookiesException.class,
                    () -> watcher.newEnsemble(1, 1, 1, Collections.emptyMap()));
        } else {
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .ignoreExceptionsMatching(ex -> ex instanceof BKException.BKNotEnoughBookiesException)
                    .untilAsserted(() -> assertThat(watcher.newEnsemble(1, 1, 1, Collections.emptyMap()))
                            .containsExactly(bookie));
            assertThat(watcher.isBookieUnavailable(bookie)).isFalse();
        }
    }

    @Test(dataProvider = "readOnly")
    public void testWatchBookiesRecoversWithoutAddressLookup(boolean readOnly) throws Exception {
        @Cleanup
        FailingReadsMetadataStore store = new FailingReadsMetadataStore("memory:" + UUID.randomUUID());
        String ledgersRoot = "/ledgers";
        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, new ServerConfiguration());
        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);
        AtomicReference<Set<BookieId>> observed = new AtomicReference<>(Collections.emptySet());
        RegistrationClient.RegistrationListener listener = bookies -> observed.set(bookies.getValue());
        (readOnly ? rc.watchReadOnlyBookies(listener) : rc.watchWritableBookies(listener))
                .get(10, TimeUnit.SECONDS);

        BookieId bookie = BookieId.parse("127.0.0.1:3181");
        String path = ledgersRoot + "/" + AVAILABLE_NODE + (readOnly ? "/" + READONLY : "") + "/" + bookie;
        CompletableFuture<Void> failedReads = store.failNextReads(path, 3);
        rm.registerBookie(bookie, readOnly, new BookieServiceInfo());
        // No address lookup or another metadata event drives the retries.
        failedReads.get(10, TimeUnit.SECONDS);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(observed.get()).containsExactly(bookie));
        assertThat(rc.getBookieServiceInfo(bookie).get(10, TimeUnit.SECONDS).getValue()).isNotNull();
    }

    @Test
    public void testBookieInfoReloadDoesNotResurrectDeletedBookie() throws Exception {
        @Cleanup
        FailingReadsMetadataStore store = new FailingReadsMetadataStore("memory:" + UUID.randomUUID());
        String ledgersRoot = "/ledgers";
        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, new ServerConfiguration());
        CompletableFuture<Void> reloadCompleted = new CompletableFuture<>();
        AtomicBoolean afterDeletion = new AtomicBoolean();
        CompletableFuture<Void> postDeletionReadCompleted = new CompletableFuture<>();
        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot) {
            @Override
            public CompletableFuture<Optional<CacheGetResult<BookieServiceInfo>>> readBookieInfoAsWritableBookie(
                    BookieId bookieId) {
                boolean postDeletion = afterDeletion.get();
                return super.readBookieInfoAsWritableBookie(bookieId).whenComplete((info, ex) -> {
                    if (ex == null && info.isPresent()) {
                        reloadCompleted.complete(null);
                    }
                    if (postDeletion) {
                        if (ex == null) {
                            postDeletionReadCompleted.complete(null);
                        } else {
                            postDeletionReadCompleted.completeExceptionally(ex);
                        }
                    }
                });
            }
        };
        AtomicBoolean deleting = new AtomicBoolean();
        CompletableFuture<Void> deletedView = new CompletableFuture<>();
        rc.watchWritableBookies(bookies -> {
            if (deleting.get() && bookies.getValue().isEmpty()) {
                deletedView.complete(null);
            }
        }).get(10, TimeUnit.SECONDS);
        BookieId bookie = BookieId.parse("127.0.0.1:3181");
        String path = ledgersRoot + "/" + AVAILABLE_NODE + "/" + bookie;
        CompletableFuture<Void> failedRead = store.failNextRead(path);
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        // Arm the delay before registration so it also holds any automatic retry of the failed read.
        CompletableFuture<Void> readStarted = store.delayNextRead(path, releaseRead);
        try {
            rm.registerBookie(bookie, false, new BookieServiceInfo());
            failedRead.get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                rc.getBookieServiceInfo(bookie);
                return readStarted.isDone();
            });
            CompletableFuture<Void> deletedNotification = new CompletableFuture<>();
            // Registered after the client, so completion means the deletion was also delivered to the client.
            @Cleanup("run")
            Runnable cancelListener = store.registerCancellableListener(notification -> {
                if (notification.getType() == NotificationType.Deleted && notification.getPath().equals(path)) {
                    deletedNotification.complete(null);
                }
            });
            deleting.set(true);
            rm.unregisterBookie(bookie, false);
            deletedNotification.get(10, TimeUnit.SECONDS);
            // Queue another miss after the deletion notification. Its completion is a barrier for the
            // deletion even if a scheduled membership retry also publishes an empty view.
            afterDeletion.set(true);
            rc.getBookieServiceInfo(bookie);

            // Complete the successful read captured before the deletion, after its notification was delivered.
            releaseRead.complete(null);
            reloadCompleted.get(10, TimeUnit.SECONDS);
            postDeletionReadCompleted.get(10, TimeUnit.SECONDS);
            deletedView.get(10, TimeUnit.SECONDS);
            assertThat(expectThrows(ExecutionException.class,
                    () -> rc.getBookieServiceInfo(bookie).get(10, TimeUnit.SECONDS)))
                    .hasCauseInstanceOf(BKException.BKBookieHandleNotAvailableException.class);
        } finally {
            releaseRead.complete(null);
        }
    }

    private static class FailingReadsMetadataStore extends LocalMemoryMetadataStore {
        private final Map<String, ReadFailures> failingReads = new ConcurrentHashMap<>();
        private final Map<String, ReadDelay> delayedReads = new ConcurrentHashMap<>();

        private record ReadFailures(AtomicInteger remaining, CompletableFuture<Void> failed) { }

        private record ReadDelay(CompletableFuture<Void> started, CompletableFuture<Void> release) { }

        FailingReadsMetadataStore(String url) throws MetadataStoreException {
            super(url, MetadataStoreConfig.builder().build());
        }

        /**
         * Fail the next read of the path. The returned future completes once the read has failed.
         */
        CompletableFuture<Void> failNextRead(String path) {
            return failNextReads(path, 1);
        }

        CompletableFuture<Void> failNextReads(String path, int count) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failingReads.put(path, new ReadFailures(new AtomicInteger(count), failed));
            return failed;
        }

        CompletableFuture<Void> delayNextRead(String path, CompletableFuture<Void> release) {
            CompletableFuture<Void> started = new CompletableFuture<>();
            delayedReads.put(path, new ReadDelay(started, release));
            return started;
        }

        @Override
        public CompletableFuture<Optional<GetResult>> storeGet(String path, Set<Option> opts) {
            ReadFailures failures = failingReads.get(path);
            int remaining = failures == null ? 0 : failures.remaining().getAndDecrement();
            if (remaining > 0) {
                if (remaining == 1) {
                    failingReads.remove(path, failures);
                    failures.failed().complete(null);
                }
                return CompletableFuture.failedFuture(new MetadataStoreException("Injected read failure"));
            }
            CompletableFuture<Optional<GetResult>> read = super.storeGet(path, opts);
            ReadDelay delay = delayedReads.remove(path);
            if (delay == null) {
                return read;
            }
            delay.started().complete(null);
            return delay.release().thenCompose(__ -> read);
        }
    }

    @Test(dataProvider = "impl")
    public void testWatchWritableBookiesSuccess(String provider, Supplier<String> urlSupplier) throws Exception {
        testWatchBookiesSuccess(provider, urlSupplier, true);
    }

    @Test(dataProvider = "impl")
    public void testWatchReadonlyBookiesSuccess(String provider, Supplier<String> urlSupplier) throws Exception {
        testWatchBookiesSuccess(provider, urlSupplier, false);
    }

    private void testWatchBookiesSuccess(String provider, Supplier<String> urlSupplier, boolean isWritable)
            throws Exception {

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();

        @Cleanup
        RegistrationManager rm = new PulsarRegistrationManager(store, ledgersRoot, mock(AbstractConfiguration.class));

        @Cleanup
        RegistrationClient rc = new PulsarRegistrationClient(store, ledgersRoot);

        //
        // 1. test watch bookies with a listener
        //
        Queue<Versioned<Set<BookieId>>> updates = new ConcurrentLinkedQueue<>();
        Map<BookieId, Boolean> bookies = new ConcurrentHashMap<>();
        RegistrationClient.RegistrationListener listener = (b) -> {
            updates.add(b);
            b.getValue().forEach(x -> bookies.put(x, true));
        };

        int expectedBookies = 10;
        Set<BookieId> addresses = prepareNBookies(expectedBookies);

        if (isWritable) {
            result(rc.watchWritableBookies(listener));
        } else {
            result(rc.watchReadOnlyBookies(listener));
        }

        for (BookieId address : addresses) {
            rm.registerBookie(address, !isWritable, new BookieServiceInfo());
        }

        Awaitility.await().untilAsserted(() -> {
            assertFalse(updates.isEmpty());
            assertEquals(bookies.size(), expectedBookies);
        });
    }


    @SuppressWarnings("deprecation")
    @Test
    public void testNetworkDelayWithBkZkManager() throws Throwable {
        final String zksConnectionString = zks.getConnectionString();
        final String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();
        // prepare registration manager
        @Cleanup
        ZooKeeper zk = new ZooKeeper(zksConnectionString, 5000, null);
        final ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setZkLedgersRootPath(ledgersRoot);
        final FaultInjectableZKRegistrationManager rm =
                new FaultInjectableZKRegistrationManager(serverConfiguration, zk);
        rm.prepareFormat();
        // prepare registration client
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zksConnectionString,
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        @Cleanup
        RegistrationClient rc1 = new PulsarRegistrationClient(store, ledgersRoot);
        @Cleanup
        RegistrationClient rc2 = new PulsarRegistrationClient(store, ledgersRoot);

        final List<BookieId> addresses = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            addresses.add(BookieId.parse("BOOKIE-" + i));
        }
        final Map<BookieId, BookieServiceInfo> bookieServiceInfos = new HashMap<>();

        int port = 223;
        for (BookieId address : addresses) {
            BookieServiceInfo info = new BookieServiceInfo();
            BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
            endpoint.setAuth(Collections.emptyList());
            endpoint.setExtensions(Collections.emptyList());
            endpoint.setId("id");
            endpoint.setHost("localhost");
            endpoint.setPort(port++);
            endpoint.setProtocol("bookie-rpc");
            info.setEndpoints(Arrays.asList(endpoint));
            bookieServiceInfos.put(address, info);
            rm.registerBookie(address, false, info);
            // write the cookie
            rm.writeCookie(address, new Versioned<>(new byte[0], Version.NEW));
        }

        // trigger loading the BookieServiceInfo in the local cache
        getAndVerifyAllBookies(rc1, addresses);
        getAndVerifyAllBookies(rc2, addresses);

        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                compareBookieServiceInfo(rc1.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
                compareBookieServiceInfo(rc2.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
            }
        });

        // verified the init status.


        // mock network delay
        rm.betweenRegisterReadOnlyBookie(__ -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        for (int i = 0; i < addresses.size() / 2; i++) {
            final BookieId bkId = addresses.get(i);
            // turn some bookies to be read only.
            rm.registerBookie(bkId, true, bookieServiceInfos.get(bkId));
        }

        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                compareBookieServiceInfo(rc1.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
                compareBookieServiceInfo(rc2.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
            }
        });
    }
}
