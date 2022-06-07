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
package org.apache.bookkeeper.mledger.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;

public class ManagedCursorMXBeanImpl implements ManagedCursorMXBean {
    private static final String[] LABEL_NAMES = new String[]{"namespace", "ledger_name", "cursor_name"};
    protected static final Pattern V2_LEDGER_NAME_PATTERN = Pattern.compile("^(([^/]+)/([^/]+)/([^/]+))/(.*)$");

    private static final Counter PERSIST_LEDGER_SUCCEED =
            Counter.build("pulsar_ml_cursor_persistLedgerSucceed", "-").labelNames(LABEL_NAMES).register();
    private static final Counter PERSIST_LEDGER_FAILED =
            Counter.build("pulsar_ml_cursor_persistLedgerErrors", "-").labelNames(LABEL_NAMES).register();
    private static final Counter PERSIST_ZOOKEEPER_SUCCEED =
            Counter.build("pulsar_ml_cursor_persistZookeeperSucceed", "-").labelNames(LABEL_NAMES).register();
    private static final Counter PERSIST_ZOOKEEPER_FAILED =
            Counter.build("pulsar_ml_cursor_persistZookeeperErrors", "-").labelNames(LABEL_NAMES).register();
    private static final Counter WRITE_CURSOR_LEDGER_SIZE =
            Counter.build("pulsar_ml_cursor_writeLedgerSize", "-").labelNames(LABEL_NAMES).register();
    private static final Counter WRITE_CURSOR_LEDGER_LOGICAL_SIZE =
            Counter.build("pulsar_ml_cursor_writeLedgerLogicalSize", "-").labelNames(LABEL_NAMES).register();
    private static final Counter READ_CURSOR_LEDGER_SIZE =
            Counter.build("pulsar_ml_cursor_readLedgerSize", "-").labelNames(LABEL_NAMES).register();
    private static final Gauge INDIVIDUAL_DELETED_MESSAGES =
            Gauge.build("pulsar_ml_cursor_nonContiguousDeletedMessagesRange", "-").labelNames(LABEL_NAMES).register();


    private final ManagedCursor managedCursor;
    private final String[] labelValues;

    private final Counter.Child persistLedgerSucceed;
    private final Counter.Child persistLedgerFailed;
    private final Counter.Child persistZookeeperSucceed;
    private final Counter.Child persistZookeeperFailed;
    private final Counter.Child writeCursorLedgerSize;
    private final Counter.Child writeCursorLedgerLogicalSize;
    private final Counter.Child readCursorLedgerSize;

    private ManagedCursorMXBeanImpl(ManagedCursor managedCursor) {
        this.managedCursor = managedCursor;

        String ledgerName = managedCursor.getManagedLedger().getName();
        String cursorName = managedCursor.getName();
        this.labelValues = new String[]{parseNamespaceFromLedgerName(ledgerName), ledgerName , cursorName};

        this.persistLedgerSucceed = PERSIST_LEDGER_SUCCEED.labels(labelValues);
        this.persistLedgerFailed = PERSIST_LEDGER_FAILED.labels(labelValues);
        this.persistZookeeperSucceed = PERSIST_ZOOKEEPER_SUCCEED.labels(labelValues);
        this.persistZookeeperFailed = PERSIST_ZOOKEEPER_FAILED.labels(labelValues);
        this.writeCursorLedgerSize = WRITE_CURSOR_LEDGER_SIZE.labels(labelValues);
        this.writeCursorLedgerLogicalSize = WRITE_CURSOR_LEDGER_LOGICAL_SIZE.labels(labelValues);
        this.readCursorLedgerSize = READ_CURSOR_LEDGER_SIZE.labels(labelValues);
        INDIVIDUAL_DELETED_MESSAGES.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return managedCursor.getTotalNonContiguousDeletedMessagesRange();
            }
        }, labelValues);
    }

    public static ManagedCursorMXBean create(ManagedCursor managedCursor,
                                             boolean exposeManagedCursorMetricsInPrometheus) {
        if (exposeManagedCursorMetricsInPrometheus) {
            return new ManagedCursorMXBeanImpl(managedCursor);
        }

        return new NoopManagedCursorMXBean(managedCursor);
    }

    @Override
    public String getName() {
        return this.managedCursor.getName();
    }

    @Override
    public String getLedgerName() {
        return this.managedCursor.getManagedLedger().getName();
    }

    @Override
    public void persistToLedger(boolean success) {
        if (success) {
            this.persistLedgerSucceed.inc();
        } else {
            this.persistLedgerFailed.inc();
        }
    }

    @Override
    public void persistToZookeeper(boolean success) {
        if (success) {
            this.persistZookeeperSucceed.inc();
        } else {
            this.persistZookeeperFailed.inc();
        }
    }

    @Override
    public long getPersistLedgerSucceed() {
        return Double.valueOf(this.persistLedgerSucceed.get()).longValue();
    }

    @Override
    public long getPersistLedgerErrors() {
        return Double.valueOf(this.persistLedgerFailed.get()).longValue();
    }

    @Override
    public long getPersistZookeeperSucceed() {
        return Double.valueOf(this.persistZookeeperSucceed.get()).longValue();
    }

    @Override
    public long getPersistZookeeperErrors() {
        return Double.valueOf(this.persistZookeeperFailed.get()).longValue();
    }

    @Override
    public void addWriteCursorLedgerSize(long size) {
        this.writeCursorLedgerSize.inc(size * ((ManagedCursorImpl) managedCursor).config.getWriteQuorumSize());
        this.writeCursorLedgerLogicalSize.inc(size);
    }

    @Override
    public void addReadCursorLedgerSize(long size) {
        readCursorLedgerSize.inc(size);
    }

    @Override
    public long getWriteCursorLedgerSize() {
        return Double.valueOf(this.writeCursorLedgerSize.get()).longValue();
    }

    @Override
    public long getWriteCursorLedgerLogicalSize() {
        return Double.valueOf(this.writeCursorLedgerLogicalSize.get()).longValue();
    }

    @Override
    public long getReadCursorLedgerSize() {
        return Double.valueOf(this.readCursorLedgerSize.get()).longValue();
    }

    @Override
    protected void finalize() throws Throwable {
        PERSIST_LEDGER_SUCCEED.remove(labelValues);
        PERSIST_LEDGER_FAILED.remove(labelValues);
        PERSIST_ZOOKEEPER_SUCCEED.remove(labelValues);
        PERSIST_ZOOKEEPER_FAILED.remove(labelValues);
        WRITE_CURSOR_LEDGER_SIZE.remove(labelValues);
        WRITE_CURSOR_LEDGER_LOGICAL_SIZE.remove(labelValues);
        READ_CURSOR_LEDGER_SIZE.remove(labelValues);
        INDIVIDUAL_DELETED_MESSAGES.remove(labelValues);
    }

    /**
     * @see org.apache.pulsar.broker.stats.metrics.AbstractMetrics#parseNamespaceFromLedgerName(String)
     */
    private static String parseNamespaceFromLedgerName(String ledgerName) {
        Matcher m = V2_LEDGER_NAME_PATTERN.matcher(ledgerName);

        if (m.matches()) {
            return m.group(1);
        } else {
            throw new RuntimeException("Failed to parse the namespace from ledger name : " + ledgerName);
        }
    }


    public static class NoopManagedCursorMXBean implements ManagedCursorMXBean {

        private final ManagedCursor managedCursor;

        public NoopManagedCursorMXBean(ManagedCursor managedCursor) {
            this.managedCursor = managedCursor;
        }

        @Override
        public String getName() {
            return this.managedCursor.getName();
        }

        @Override
        public String getLedgerName() {
            return this.managedCursor.getManagedLedger().getName();
        }

        @Override
        public void persistToLedger(boolean success) {

        }

        @Override
        public void persistToZookeeper(boolean success) {

        }

        @Override
        public long getPersistLedgerSucceed() {
            return 0;
        }

        @Override
        public long getPersistLedgerErrors() {
            return 0;
        }

        @Override
        public long getPersistZookeeperSucceed() {
            return 0;
        }

        @Override
        public long getPersistZookeeperErrors() {
            return 0;
        }

        @Override
        public void addWriteCursorLedgerSize(long size) {

        }

        @Override
        public void addReadCursorLedgerSize(long size) {

        }

        @Override
        public long getWriteCursorLedgerSize() {
            return 0;
        }

        @Override
        public long getWriteCursorLedgerLogicalSize() {
            return 0;
        }

        @Override
        public long getReadCursorLedgerSize() {
            return 0;
        }
    }
}
