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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Test BookKeeperClient which allows access to members we don't wish to expose in the public API.
 */
public class MockBookKeeper extends BookKeeper {

    final ExecutorService executor = Executors.newFixedThreadPool(1, new DefaultThreadFactory("mock-bookkeeper"));

    public ZooKeeper getZkHandle() {
        return super.getZkHandle();
    }

    public ClientConfiguration getConf() {
        return super.getConf();
    }

    Map<Long, MockLedgerHandle> ledgers = new ConcurrentHashMap<Long, MockLedgerHandle>();
    AtomicLong sequence = new AtomicLong(3);
    AtomicBoolean stopped = new AtomicBoolean(false);
    AtomicInteger stepsToFail = new AtomicInteger(-1);
    int failReturnCode = BKException.Code.OK;
    int nextFailReturnCode = BKException.Code.OK;

    public MockBookKeeper(ClientConfiguration conf, ZooKeeper zk) throws Exception {
        super(conf, zk, new OioEventLoopGroup());
    }

    public LedgerHandle createLedger(DigestType digestType, byte passwd[]) throws BKException {
        return createLedger(3, 2, digestType, passwd);
    }

    public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte passwd[]) throws BKException {
        return createLedger(ensSize, qSize, qSize, digestType, passwd);
    }

    @Override
    public void asyncCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, final DigestType digestType,
            final byte[] passwd, final CreateCallback cb, final Object ctx) {
        if (stopped.get()) {
            cb.createComplete(BKException.Code.WriteException, null, ctx);
            return;
        }

        executor.execute(new Runnable() {
            public void run() {
                if (getProgrammedFailStatus()) {
                    if (failReturnCode != BkTimeoutOperation) {
                        cb.createComplete(failReturnCode, null, ctx);
                    }
                    return;
                }

                if (stopped.get()) {
                    cb.createComplete(BKException.Code.WriteException, null, ctx);
                    return;
                }

                try {
                    long id = sequence.getAndIncrement();
                    log.info("Creating ledger {}", id);
                    MockLedgerHandle lh = new MockLedgerHandle(MockBookKeeper.this, id, digestType, passwd);
                    ledgers.put(id, lh);
                    cb.createComplete(0, lh, ctx);
                } catch (Throwable t) {
                }
            }
        });
    }

    @Override
    public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd) throws BKException {
        checkProgrammedFail();

        if (stopped.get()) {
            throw BKException.create(BKException.Code.WriteException);
        }

        try {
            long id = sequence.getAndIncrement();
            log.info("Creating ledger {}", id);
            MockLedgerHandle lh = new MockLedgerHandle(this, id, digestType, passwd);
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
        asyncCreateLedger(ensSize, qSize, qSize, digestType, passwd, cb, ctx);
    }

    @Override
    public void asyncOpenLedger(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        if (getProgrammedFailStatus()) {
            if (failReturnCode != BkTimeoutOperation) {
                cb.openComplete(failReturnCode, null, ctx);
            }
            return;
        }

        if (stopped.get()) {
            cb.openComplete(BKException.Code.WriteException, null, ctx);
            return;
        }

        MockLedgerHandle lh = ledgers.get(lId);
        if (lh == null) {
            cb.openComplete(BKException.Code.NoSuchLedgerExistsException, null, ctx);
        } else if (lh.digest != digestType) {
            cb.openComplete(BKException.Code.DigestMatchException, null, ctx);
        } else if (!Arrays.equals(lh.passwd, passwd)) {
            cb.openComplete(BKException.Code.UnauthorizedAccessException, null, ctx);
        } else {
            cb.openComplete(0, lh, ctx);
        }
    }

    @Override
    public void asyncOpenLedgerNoRecovery(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        asyncOpenLedger(lId, digestType, passwd, cb, ctx);
    }

    @Override
    public void asyncDeleteLedger(long lId, DeleteCallback cb, Object ctx) {
        if (getProgrammedFailStatus()) {
            if (failReturnCode != BkTimeoutOperation) {
                cb.deleteComplete(failReturnCode, ctx);
            }
        } else if (stopped.get()) {
            cb.deleteComplete(BKException.Code.WriteException, ctx);
        } else if (ledgers.containsKey(lId)) {
            ledgers.remove(lId);
            cb.deleteComplete(0, ctx);
        } else {
            cb.deleteComplete(BKException.Code.NoSuchLedgerExistsException, ctx);
        }
    }

    @Override
    public void deleteLedger(long lId) throws InterruptedException, BKException {
        checkProgrammedFail();

        if (stopped.get()) {
            throw BKException.create(BKException.Code.WriteException);
        }

        if (!ledgers.containsKey(lId)) {
            throw BKException.create(BKException.Code.NoSuchLedgerExistsException);
        }

        ledgers.remove(lId);
    }

    @Override
    public void close() throws InterruptedException, BKException {
        checkProgrammedFail();
        super.close();
    }

    public void shutdown() {
        try {
            super.close();
        } catch (Exception e) {
        }
        stopped.set(true);
        for (MockLedgerHandle ledger : ledgers.values()) {
            ledger.entries.clear();
        }

        ledgers.clear();
        executor.shutdownNow();
    }

    public boolean isStopped() {
        return stopped.get();
    }

    public Set<Long> getLedgers() {
        return ledgers.keySet();
    }

    void checkProgrammedFail() throws BKException {
        int steps = stepsToFail.getAndDecrement();
        log.debug("Steps to fail: {}", steps);
        if (steps <= 0) {
            if (failReturnCode != BKException.Code.OK) {
                int rc = failReturnCode;
                failReturnCode = nextFailReturnCode;
                nextFailReturnCode = BKException.Code.OK;
                throw BKException.create(rc);
            }
        }
    }

    boolean getProgrammedFailStatus() {
        int steps = stepsToFail.getAndDecrement();
        log.debug("Steps to fail: {}", steps);
        return steps == 0;
    }

    public void failNow(int rc) {
        failNow(rc, BKException.Code.OK);
    }

    public void failNow(int rc, int nextErrorCode) {
        failAfter(0, rc);
    }

    public void failAfter(int steps, int rc) {
        failAfter(steps, rc, BKException.Code.OK);
    }

    public void failAfter(int steps, int rc, int nextErrorCode) {
        stepsToFail.set(steps);
        failReturnCode = rc;
        this.nextFailReturnCode = nextErrorCode;
    }

    public void timeoutAfter(int steps) {
        stepsToFail.set(steps);
        failReturnCode = BkTimeoutOperation;
    }

    private static final int BkTimeoutOperation = 1000;

    private static final Logger log = LoggerFactory.getLogger(MockBookKeeper.class);
}
