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
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
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
        List<String> children = Lists.newArrayList();
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
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            rm.registerBookie(address, true, new BookieServiceInfo());
        }

        Versioned<Set<BookieId>> result = result(rc.getReadOnlyBookies());

        assertEquals(addresses.size(), result.getValue().size());
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
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            boolean isReadOnly = children.size() % 2 == 0;
            rm.registerBookie(address, isReadOnly, new BookieServiceInfo());
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
        MetadataStoreExtended store =
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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

}
