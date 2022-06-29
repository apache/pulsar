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
package org.apache.bookkeeper.client;

import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.ZooKeeper;

/**
 * Test BookKeeperClient which allows access to members we don't
 * wish to expose in the public API.
 */
@Slf4j
public class BookKeeperTestClient extends BookKeeper {
    TestStatsProvider statsProvider;

    public BookKeeperTestClient(ClientConfiguration conf, TestStatsProvider statsProvider)
            throws IOException, InterruptedException, BKException {
        super(conf, null, null, new UnpooledByteBufAllocator(false),
                statsProvider == null ? NullStatsLogger.INSTANCE : statsProvider.getStatsLogger(""),
                null, null, null);
        this.statsProvider = statsProvider;
    }

    public BookKeeperTestClient(ClientConfiguration conf, ZooKeeper zkc)
            throws IOException, InterruptedException, BKException {
        super(conf, zkc, null, new UnpooledByteBufAllocator(false),
                NullStatsLogger.INSTANCE, null, null, null);
        this.statsProvider = statsProvider;
    }

    public BookKeeperTestClient(ClientConfiguration conf)
            throws InterruptedException, BKException, IOException {
        this(conf, (TestStatsProvider) null);
    }

    public ZooKeeper getZkHandle() {
        return ((ZKMetadataClientDriver) metadataDriver).getZk();
    }

    public ClientConfiguration getConf() {
        return super.getConf();
    }

    public BookieClient getBookieClient() {
        return bookieClient;
    }

    public Future<?> waitForReadOnlyBookie(BookieId b)
            throws Exception {
        return waitForBookieInSet(b, false);
    }

    public Future<?> waitForWritableBookie(BookieId b)
            throws Exception {
        return waitForBookieInSet(b, true);
    }

    /**
     * Wait for bookie to appear in either the writable set of bookies,
     * or the read only set of bookies. Also ensure that it doesn't exist
     * in the other set before completing.
     */
    private Future<?> waitForBookieInSet(BookieId b,
                                         boolean writable) throws Exception {
        log.info("Wait for {} to become {}",
                b, writable ? "writable" : "readonly");

        CompletableFuture<Void> readOnlyFuture = new CompletableFuture<>();
        CompletableFuture<Void> writableFuture = new CompletableFuture<>();

        RegistrationListener readOnlyListener = (bookies) -> {
            boolean contains = bookies.getValue().contains(b);
            if ((!writable && contains) || (writable && !contains)) {
                readOnlyFuture.complete(null);
            }
        };
        RegistrationListener writableListener = (bookies) -> {
            boolean contains = bookies.getValue().contains(b);
            if ((writable && contains) || (!writable && !contains)) {
                writableFuture.complete(null);
            }
        };

        getMetadataClientDriver().getRegistrationClient().watchWritableBookies(writableListener);
        getMetadataClientDriver().getRegistrationClient().watchReadOnlyBookies(readOnlyListener);

        if (writable) {
            return writableFuture
                    .thenCompose(ignored -> getMetadataClientDriver().getRegistrationClient().getReadOnlyBookies())
                    .thenCompose(readonlyBookies -> {
                        if (readonlyBookies.getValue().contains(b)) {
                            // if the bookie still shows up at readonly path, wait for it to disappear
                            return readOnlyFuture;
                        } else {
                            return FutureUtils.Void();
                        }
                    });
        } else {
            return readOnlyFuture
                    .thenCompose(ignored -> getMetadataClientDriver().getRegistrationClient().getWritableBookies())
                    .thenCompose(writableBookies -> {
                        if (writableBookies.getValue().contains(b)) {
                            // if the bookie still shows up at writable path, wait for it to disappear
                            return writableFuture;
                        } else {
                            return FutureUtils.Void();
                        }
                    });
        }
    }

    /**
     * Force a read to zookeeper to get list of bookies.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void readBookiesBlocking() throws InterruptedException, BKException {
        bookieWatcher.initialBlockingBookieRead();
    }

    public TestStatsProvider getTestStatsProvider() {
        return statsProvider;
    }
}

