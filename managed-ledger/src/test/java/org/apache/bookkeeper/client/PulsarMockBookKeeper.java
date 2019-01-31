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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.OpenBuilderBase;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mocked version of BookKeeper client that keeps all ledgers data in memory.
 *
 * <p>This mocked client is meant to be used in unit tests for applications using the BookKeeper API.
 */
public class PulsarMockBookKeeper extends BookKeeper {

    final ExecutorService executor;
    final ZooKeeper zkc;

    @Override
    public ZooKeeper getZkHandle() {
        return zkc;
    }

    @Override
    public ClientConfiguration getConf() {
        return super.getConf();
    }

    Map<Long, PulsarMockLedgerHandle> ledgers = new ConcurrentHashMap<>();
    AtomicLong sequence = new AtomicLong(3);

    CompletableFuture<Void> defaultResponse = CompletableFuture.completedFuture(null);
    List<CompletableFuture<Void>> failures = new ArrayList<>();

    public PulsarMockBookKeeper(ZooKeeper zkc, ExecutorService executor) throws Exception {
        this.zkc = zkc;
        this.executor = executor;
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
                    PulsarMockLedgerHandle lh = new PulsarMockLedgerHandle(PulsarMockBookKeeper.this, id, digestType, passwd);
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
                            if (!validate()) {
                                return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
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
                                                                                  lh.getLedgerMetadata(), lh.entries));
                            }
                        });
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
        }

        ledgers.clear();
    }

    public Set<Long> getLedgers() {
        return ledgers.keySet();
    }

    void checkProgrammedFail() throws BKException, InterruptedException {
        try {
            getProgrammedFailure().get();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof BKException) {
                throw (BKException)ee.getCause();
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

    synchronized CompletableFuture<Void> getProgrammedFailure() {
        return failures.isEmpty() ? defaultResponse : failures.remove(0);
    }

    public void failNow(int rc) {
        failAfter(0, rc);
    }

    public void failAfter(int steps, int rc) {
        promiseAfter(steps).completeExceptionally(BKException.create(rc));
    }

    private int emptyLedgerAfter = -1;

    /**
     * After N times, make a ledger to appear to be empty
     */
    public synchronized void returnEmptyLedgerAfter(int steps) {
        emptyLedgerAfter = steps;
    }

    public synchronized CompletableFuture<Void> promiseAfter(int steps) {
        while (failures.size() <= steps) {
            failures.add(defaultResponse);
        }
        CompletableFuture<Void> promise = new CompletableFuture<>();
        failures.set(steps, promise);
        return promise;
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

    private static final Logger log = LoggerFactory.getLogger(PulsarMockBookKeeper.class);
}
