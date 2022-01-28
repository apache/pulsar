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
package org.apache.pulsar.broker.stats.metrics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;

public class ManagedCursorMetrics extends AbstractMetrics {

    private Map<String, String> dimensionMap;
    private List<Metrics> metricsCollection;

    public ManagedCursorMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metricsCollection = Lists.newArrayList();
        this.dimensionMap = Maps.newHashMap();
    }

    @Override
    public synchronized List<Metrics> generate() {
        return aggregate();
    }


    /**
     * Aggregation by namespace, ledger, cursor.
     *
     * @return List<Metrics>
     */
    private List<Metrics> aggregate() {
        metricsCollection.clear();
        for (Map.Entry<String, ManagedLedgerImpl> e : getManagedLedgers().entrySet()) {
            String ledgerName = e.getKey();
            ManagedLedgerImpl ledger = e.getValue();
            String namespace = parseNamespaceFromLedgerName(ledgerName);

            ManagedCursorContainer cursorContainer = ledger.getCursors();
            Iterator<ManagedCursor> cursorIterator = cursorContainer.iterator();

            while (cursorIterator.hasNext()) {
                ManagedCursorImpl cursor = (ManagedCursorImpl) cursorIterator.next();
                ManagedCursorMXBean cStats = cursor.getStats();
                dimensionMap.clear();
                dimensionMap.put("namespace", namespace);
                dimensionMap.put("ledger_name", ledgerName);
                dimensionMap.put("cursor_name", cursor.getName());
                Metrics metrics = createMetrics(dimensionMap);
                metrics.put("brk_ml_cursor_nonContiguousDeletedMessagesRange",
                        (long) cursor.getTotalNonContiguousDeletedMessagesRange());
                metrics.put("brk_ml_cursor_persistLedgerSucceed", cStats.getPersistLedgerSucceed());
                metrics.put("brk_ml_cursor_persistLedgerErrors", cStats.getPersistLedgerErrors());
                metrics.put("brk_ml_cursor_persistZookeeperSucceed", cStats.getPersistZookeeperSucceed());
                metrics.put("brk_ml_cursor_persistZookeeperErrors", cStats.getPersistZookeeperErrors());
                metrics.put("brk_ml_cursor_writeLedgerSize", cStats.getWriteCursorLedgerSize());
                metrics.put("brk_ml_cursor_writeLedgerLogicalSize", cStats.getWriteCursorLedgerLogicalSize());
                metrics.put("brk_ml_cursor_readLedgerSize", cStats.getReadCursorLedgerSize());
                metricsCollection.add(metrics);
            }
        }
        return metricsCollection;
    }
}
