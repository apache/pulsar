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
package io.debezium.connector.mysql;

import static io.debezium.connector.mysql.GtidSet.GTID_DELIMITER;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mysql.signal.ExecuteSnapshotPulsarSignal;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







/**
 * Mysql read only incremental snapshot context.
 *
 * @param <T>
 */
@NotThreadSafe
public class MySqlReadOnlyIncrementalSnapshotContext<T>
        extends AbstractIncrementalSnapshotContext<T> {

    public static final String SIGNAL_OFFSET = INCREMENTAL_SNAPSHOT_KEY + "_signal_offset";
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlReadOnlyIncrementalSnapshotContext.class);
    private final Queue<ExecuteSnapshotPulsarSignal> executeSnapshotSignals = new ConcurrentLinkedQueue<>();
    private GtidSet previousLowWatermark;
    private GtidSet previousHighWatermark;
    private GtidSet lowWatermark;
    private GtidSet highWatermark;
    private String signalOffset;

    public MySqlReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MySqlReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    protected static <V> IncrementalSnapshotContext<V> init(
            MySqlReadOnlyIncrementalSnapshotContext<V> context,
            Map<String, ?> offsets
    ) {
        AbstractIncrementalSnapshotContext.init(context, offsets);
        final String signalOffset = (String) offsets.get(SIGNAL_OFFSET);
        context.setSignalOffset(signalOffset);
        return context;
    }

    public static <V> MySqlReadOnlyIncrementalSnapshotContext<V> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <V> MySqlReadOnlyIncrementalSnapshotContext<V> load(
            Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        MySqlReadOnlyIncrementalSnapshotContext<V> context =
                new MySqlReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public void setLowWatermark(GtidSet lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public void setHighWatermark(GtidSet highWatermark) {
        this.highWatermark = highWatermark.subtract(lowWatermark);
    }

    public boolean updateWindowState(OffsetContext offsetContext) {
        String currentGtid = getCurrentGtid(offsetContext);
        if (!windowOpened && lowWatermark != null) {
            boolean pastLowWatermark = !lowWatermark.contains(currentGtid);
            if (pastLowWatermark) {
                LOGGER.debug("Current gtid {}, low watermark {}", currentGtid, lowWatermark);
                windowOpened = true;
            }
        }
        if (windowOpened && highWatermark != null) {
            boolean pastHighWatermark = !highWatermark.contains(currentGtid);
            if (pastHighWatermark) {
                LOGGER.debug("Current gtid {}, high watermark {}", currentGtid, highWatermark);
                closeWindow();
                return true;
            }
        }
        return false;
    }

    public boolean reachedHighWatermark(String currentGtid) {
        if (highWatermark == null) {
            return false;
        }
        if (currentGtid == null) {
            return true;
        }
        String[] gtid = GTID_DELIMITER.split(currentGtid);
        GtidSet.UUIDSet uuidSet = getUuidSet(gtid[0]);
        if (uuidSet != null) {
            long maxTransactionId = uuidSet.getIntervals().stream()
                    .mapToLong(GtidSet.Interval::getEnd)
                    .max()
                    .getAsLong();
            if (maxTransactionId <= Long.parseLong(gtid[1])) {
                LOGGER.debug("Gtid {} reached high watermark {}", currentGtid, highWatermark);
                return true;
            }
        }
        return false;
    }

    public String getCurrentGtid(OffsetContext offsetContext) {
        return offsetContext.getSourceInfo().getString(SourceInfo.GTID_KEY);
    }

    public void closeWindow() {
        windowOpened = false;
        previousHighWatermark = highWatermark;
        highWatermark = null;
        previousLowWatermark = lowWatermark;
        lowWatermark = null;
    }

    private GtidSet.UUIDSet getUuidSet(String serverId) {
        return highWatermark.getUUIDSets().isEmpty()
                ? lowWatermark.forServerWithId(serverId) : highWatermark.forServerWithId(serverId);
    }

    public boolean serverUuidChanged() {
        return highWatermark.getUUIDSets().size() > 1;
    }

    public String getSignalOffset() {
        return signalOffset;
    }

    public void setSignalOffset(String signalOffset) {
        this.signalOffset = signalOffset;
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        Map<String, Object> snapshotOffset = super.store(offset);
        snapshotOffset.put(SIGNAL_OFFSET, signalOffset);
        return snapshotOffset;
    }

    public void enqueueDataCollectionsToSnapshot(List<String> dataCollectionIds, MessageId signalOffset) {
        executeSnapshotSignals.add(new ExecuteSnapshotPulsarSignal(dataCollectionIds, signalOffset));
    }

    public ExecuteSnapshotPulsarSignal getExecuteSnapshotSignals() {
        return executeSnapshotSignals.poll();
    }

    public boolean hasExecuteSnapshotSignals() {
        return !executeSnapshotSignals.isEmpty();
    }

    public boolean watermarksChanged() {
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }
}