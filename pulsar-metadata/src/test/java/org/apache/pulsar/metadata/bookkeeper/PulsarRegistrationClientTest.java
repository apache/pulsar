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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.*;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
@Slf4j
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
            rm.registerBookie(address, false, new BookieServiceInfo());
        }

        Versioned<Set<BookieId>> result = result(rc.getWritableBookies());

        assertEquals(addresses.size(), result.getValue().size());
    }


    @Test(dataProvider = "impl")
    public void testGetReadonlyBookies(String provider, Supplier<String> urlSupplier) throws Exception {
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
            rm.registerBookie(address, true, new BookieServiceInfo());
        }

        Versioned<Set<BookieId>> result = result(rc.getReadOnlyBookies());

        assertEquals(addresses.size(), result.getValue().size());
    }

    @Test(dataProvider = "impl")
    public void testGetBookieServiceInfo(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        assertEquals(addresses.size(), result.getValue().size());
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
                MetadataStoreConfig.builder().build());

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

        int BOOKIES = 10;
        Set<BookieId> addresses = prepareNBookies(BOOKIES);

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
            assertEquals(BOOKIES, bookies.size());
        });
    }


    @Test
    public void testNetworkDelayWithBkZkManager() throws Throwable {
        final String zksConnectionString = zks.getConnectionString();
        final String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();
        // prepare registration manager
        ZooKeeper zk = new ZooKeeper(zksConnectionString, 5000, new ZooKeeperWatcherBase(1000));
        final ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setZkLedgersRootPath(ledgersRoot);
        final FaultInjectableZKRegistrationManager rm = new FaultInjectableZKRegistrationManager(serverConfiguration, zk);
        rm.prepareFormat();
        // prepare registration client
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zksConnectionString,
                MetadataStoreConfig.builder().build());
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
