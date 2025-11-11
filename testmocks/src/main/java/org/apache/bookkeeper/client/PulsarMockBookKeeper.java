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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Lists;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.OpenBuilderBase;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mocked version of BookKeeper client that keeps all ledgers data in memory.
 *
 * <p>This mocked client is meant to be used in unit tests for applications using the BookKeeper API.
 */
public class PulsarMockBookKeeper extends BookKeeper {

    final OrderedExecutor orderedExecutor;
    final ExecutorService executor;
    final ScheduledExecutorService scheduler;

    @Override
    public ClientConfiguration getConf() {
        return super.getConf();
    }

    final Map<Long, PulsarMockLedgerHandle> ledgers = new ConcurrentHashMap<>();
    final AtomicLong sequence = new AtomicLong(3);

    CompletableFuture<Void> defaultResponse = CompletableFuture.completedFuture(null);
    private static final List<BookieId> ensemble = Collections.unmodifiableList(Lists.newArrayList(
            new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
            new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
            new BookieSocketAddress("192.0.2.3", 1234).toBookieId()));

    public static Collection<BookieId> getMockEnsemble() {
        return ensemble;
    }

    final Queue<Long> addEntryDelaysMillis = new ConcurrentLinkedQueue<>();
    final Queue<Long> addEntryResponseDelaysMillis = new ConcurrentLinkedQueue<>();
    final List<CompletableFuture<Void>> failures = new ArrayList<>();
    final List<CompletableFuture<Void>> addEntryFailures = new ArrayList<>();
    @Setter
    @Getter
    private volatile PulsarMockReadHandleInterceptor readHandleInterceptor;

    public PulsarMockBookKeeper(OrderedExecutor orderedExecutor) throws Exception {
        this.orderedExecutor = orderedExecutor;
        this.executor = orderedExecutor.chooseThread();
        scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("mock-bk-scheduler"));
    }

    @Override
    public OrderedExecutor getMainWorkerPool() {
        return orderedExecutor;
    }

    @Override
    public LedgerHandle createLedger(DigestType digestType, byte passwd[])
            throws BKException, InterruptedException {
        return createLedger(3, 2, digestType, passwd);
    }

    @Override
    public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte passwd[])
            throws BKException, InterruptedException {
        return createLedger(ensSize, qSize, qSize, digestType, passwd);
    }

    @Override
    public void asyncCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, final DigestType digestType,
            final byte[] passwd, final CreateCallback cb, final Object ctx, Map<String, byte[]> properties) {
        getProgrammedFailure().thenComposeAsync((res) -> {
                try {
                    long id = sequence.getAndIncrement();
                    log.info("Creating ledger {}", id);
                    PulsarMockLedgerHandle lh =
                            new PulsarMockLedgerHandle(PulsarMockBookKeeper.this, id, digestType, passwd);
                    ledgers.put(id, lh);
                    return FutureUtils.value(lh);
                } catch (Throwable t) {
                    return FutureUtils.exception(t);
                }
            }, executor).whenCompleteAsync((lh, exception) -> {
                    if (exception != null) {
                        cb.createComplete(getExceptionCode(exception), null, ctx);
                    } else {
                        cb.createComplete(BKException.Code.OK, lh, ctx);
                    }
                }, executor);
    }

    @Override
    public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd) throws BKException, InterruptedException {
        checkProgrammedFail();

        try {
            long id = sequence.getAndIncrement();
            log.info("Creating ledger {}", id);
            PulsarMockLedgerHandle lh = new PulsarMockLedgerHandle(this, id, digestType, passwd);
            ledgers.put(id, lh);
            return lh;
        } catch (Throwable t) {
            log.error("Exception:", t);
            return null;
        }
    }

    @Override
    public void asyncCreateLedger(int ensSize, int qSize, DigestType digestType, byte[] passwd, CreateCallback cb,
            Object ctx) {
        asyncCreateLedger(ensSize, qSize, qSize, digestType, passwd, cb, ctx, Collections.emptyMap());
    }

    @Override
    public void asyncOpenLedger(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        getProgrammedFailure().thenComposeAsync((res) -> {
                PulsarMockLedgerHandle lh = ledgers.get(lId);
                if (lh == null) {
                    return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
                } else if (lh.digest != digestType) {
                    return FutureUtils.exception(new BKException.BKDigestMatchException());
                } else if (!Arrays.equals(lh.passwd, passwd)) {
                    return FutureUtils.exception(new BKException.BKUnauthorizedAccessException());
                } else {
                    return FutureUtils.value(lh);
                }
            }, executor).whenCompleteAsync((ledger, exception) -> {
                    if (exception != null) {
                        cb.openComplete(getExceptionCode(exception), null, ctx);
                    } else {
                        cb.openComplete(BKException.Code.OK, ledger, ctx);
                    }
                }, executor);
    }

    @Override
    public void asyncOpenLedgerNoRecovery(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        asyncOpenLedger(lId, digestType, passwd, cb, ctx);
    }

    @Override
    public void asyncDeleteLedger(long lId, DeleteCallback cb, Object ctx) {
        getProgrammedFailure().thenComposeAsync((res) -> {
                if (ledgers.containsKey(lId)) {
                    ledgers.remove(lId);
                    return FutureUtils.value(null);
                } else {
                    return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
                }
            }, executor).whenCompleteAsync((res, exception) -> {
                    if (exception != null) {
                        cb.deleteComplete(getExceptionCode(exception), ctx);
                    } else {
                        cb.deleteComplete(BKException.Code.OK, ctx);
                    }
                }, executor);
    }

    @Override
    public void deleteLedger(long lId) throws InterruptedException, BKException {
        checkProgrammedFail();

        if (!ledgers.containsKey(lId)) {
            throw BKException.create(BKException.Code.NoSuchLedgerExistsException);
        }

        ledgers.remove(lId);
    }

    @Override
    public void close() throws InterruptedException, BKException {
        shutdown();
    }



    @Override
    public OpenBuilder newOpenLedgerOp() {
        return new OpenBuilderBase() {
            @Override
            public CompletableFuture<ReadHandle> execute() {
                return getProgrammedFailure().thenCompose(
                        (res) -> {
                            int rc = validate();
                            if (rc != BKException.Code.OK) {
                                return FutureUtils.exception(BKException.create(rc));
                            }

                            PulsarMockLedgerHandle lh = ledgers.get(ledgerId);
                            if (lh == null) {
                                return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
                            } else if (lh.digest != DigestType.fromApiDigestType(digestType)) {
                                return FutureUtils.exception(new BKException.BKDigestMatchException());
                            } else if (!Arrays.equals(lh.passwd, password)) {
                                return FutureUtils.exception(new BKException.BKUnauthorizedAccessException());
                            } else {
                                return FutureUtils.value(new PulsarMockReadHandle(PulsarMockBookKeeper.this, ledgerId,
                                        lh.getLedgerMetadata(), lh.entries,
                                        PulsarMockBookKeeper.this::getReadHandleInterceptor, lh.totalLengthCounter));
                            }
                        });
            }
        };
    }

    @Override
    public DeleteBuilder newDeleteLedgerOp() {
        return new DeleteBuilder() {
            private long ledgerId;

            @Override
            public CompletableFuture<Void> execute() {
                CompletableFuture<Void> future = new CompletableFuture<>();
                asyncDeleteLedger(ledgerId, (res, ctx) -> {
                    if (res == BKException.Code.OK) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(BKException.create(res));
                    }
                }, null);
                return future;
            }

            @Override
            public DeleteBuilder withLedgerId(long ledgerId) {
                this.ledgerId = ledgerId;
                return this;
            }
        };
    }

    public void shutdown() {
        try {
            super.close();
        } catch (Exception e) {
        }
        synchronized (this) {
            defaultResponse = FutureUtils.exception(new BKException.BKClientClosedException());
        }
        for (PulsarMockLedgerHandle ledger : ledgers.values()) {
            ledger.entries.clear();
            ledger.totalLengthCounter.set(0);
        }
        scheduler.shutdown();
        ledgers.clear();
    }

    public Set<Long> getLedgers() {
        return ledgers.keySet();
    }

    public Map<Long, PulsarMockLedgerHandle> getLedgerMap() {
        return ledgers;
    }

    void checkProgrammedFail() throws BKException, InterruptedException {
        try {
            getProgrammedFailure().get();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof BKException) {
                throw (BKException) ee.getCause();
            } else {
                throw new BKException.BKUnexpectedConditionException();
            }
        }
    }

    synchronized boolean checkReturnEmptyLedger() {
        boolean shouldFailNow = (emptyLedgerAfter == 0);
        --emptyLedgerAfter;
        return shouldFailNow;
    }

    synchronized CompletableFuture<Void> getAddEntryFailure() {
        if (!addEntryFailures.isEmpty()){
            return addEntryFailures.remove(0);
        }
        return failures.isEmpty() ? defaultResponse : failures.remove(0);
    }

    synchronized CompletableFuture<Void> getProgrammedFailure() {
        return failures.isEmpty() ? defaultResponse : failures.remove(0);
    }

    public void delay(long millis) {
        CompletableFuture<Void> delayFuture = new CompletableFuture<>();
        scheduler.schedule(() -> {
            delayFuture.complete(null);
        }, millis, TimeUnit.MILLISECONDS);
        failures.add(delayFuture);
    }


    public void failNow(int rc) {
        failAfter(0, rc);
    }

    public void failAfter(int steps, int rc) {
        promiseAfter(steps, failures).completeExceptionally(BKException.create(rc));
    }

    public void addEntryFailAfter(int steps, int rc) {
        promiseAfter(steps, addEntryFailures).completeExceptionally(BKException.create(rc));
    }

    private int emptyLedgerAfter = -1;

    /**
     * After N times, make a ledger to appear to be empty.
     */
    public synchronized void returnEmptyLedgerAfter(int steps) {
        emptyLedgerAfter = steps;
    }

    public synchronized CompletableFuture<Void> promiseAfter(int steps) {
        return promiseAfter(steps, failures);
    }

    public synchronized CompletableFuture<Void> promiseAfter(int steps, List<CompletableFuture<Void>> failures) {
        while (failures.size() <= steps) {
            failures.add(defaultResponse);
        }
        CompletableFuture<Void> promise = new CompletableFuture<>();
        failures.set(steps, promise);
        return promise;
    }

    public synchronized void addEntryDelay(long delay, TimeUnit unit) {
        addEntryDelaysMillis.add(unit.toMillis(delay));
    }

    public synchronized void addEntryResponseDelay(long delay, TimeUnit unit) {
        checkArgument(delay >= 0, "The delay time must not be negative.");
        addEntryResponseDelaysMillis.add(unit.toMillis(delay));
    }

    static int getExceptionCode(Throwable t) {
        if (t instanceof BKException) {
            return ((BKException) t).getCode();
        } else if (t.getCause() != null) {
            return getExceptionCode(t.getCause());
        } else {
            return BKException.Code.UnexpectedConditionException;
        }
    }

    private final RegistrationClient mockRegistrationClient = new RegistrationClient() {
        @Override
        public void close() {

        }

        @Override
        public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
            return getAllBookies();
        }

        @Override
        public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
            return CompletableFuture.completedFuture(new Versioned<>(new HashSet<>(ensemble), new LongVersion(0)));
        }

        @Override
        public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
            return CompletableFuture.completedFuture(new Versioned<>(new HashSet<>(), new LongVersion(0)));
        }

        @Override
        public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
            return defaultResponse;
        }

        @Override
        public void unwatchWritableBookies(RegistrationListener listener) {

        }

        @Override
        public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
            return defaultResponse;
        }

        @Override
        public void unwatchReadOnlyBookies(RegistrationListener listener) {

        }
    };

    private final MetadataClientDriver metadataClientDriver = new MetadataClientDriver() {
        @Override
        public MetadataClientDriver initialize(ClientConfiguration conf, ScheduledExecutorService scheduler,
                                               StatsLogger statsLogger, Optional<Object> ctx) throws MetadataException {
            return this;
        }

        @Override
        public String getScheme() {
            return "mock";
        }

        @Override
        public RegistrationClient getRegistrationClient() {
            return mockRegistrationClient;
        }

        @Override
        public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
            return null;
        }

        @Override
        public LayoutManager getLayoutManager() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public void setSessionStateListener(SessionStateListener sessionStateListener) {

        }
    };

    @Override
    public MetadataClientDriver getMetadataClientDriver() {
        return metadataClientDriver;
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarMockBookKeeper.class);
}
