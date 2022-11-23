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


import io.debezium.connector.mysql.signal.ExecuteSnapshotPulsarSignal;
import io.debezium.connector.mysql.signal.PulsarSignalThread;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mysql readonly incremental snapshot change event source.
 *
 * @param <T>
 */
public class MySqlReadOnlyIncrementalSnapshotChangeEventSource<T extends DataCollectionId>
        extends AbstractIncrementalSnapshotChangeEventSource<MySqlPartition, T> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlReadOnlyIncrementalSnapshotChangeEventSource.class);
    private final String showMasterStmt = "SHOW MASTER STATUS";
    private final PulsarSignalThread<T> pulsarSignal;


    public MySqlReadOnlyIncrementalSnapshotChangeEventSource(
            RelationalDatabaseConnectorConfig config,
            JdbcConnection jdbcConnection,
            EventDispatcher<MySqlPartition, T> dispatcher,
            DatabaseSchema<?> databaseSchema,
            Clock clock,
            SnapshotProgressListener<MySqlPartition> progressListener,
            DataChangeEventListener<MySqlPartition> dataChangeEventListener) {
        super(config,
                jdbcConnection,
                dispatcher,
                databaseSchema,
                clock,
                progressListener,
                dataChangeEventListener);
        pulsarSignal = new PulsarSignalThread<>(MySqlConnector.class, config, this);
    }

    @Override
    public void init(MySqlPartition partition, OffsetContext offsetContext) {
        super.init(partition, offsetContext);
        String signalOffset = getContext().getSignalOffset();
        if (signalOffset != null) {
            pulsarSignal.seek(signalOffset);
        }
        pulsarSignal.start();
    }

    @Override
    public void processMessage(MySqlPartition partition,
                               DataCollectionId dataCollectionId,
                               Object key,
                               OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        checkEnqueuedSnapshotSignals(partition, offsetContext);
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition);
        } else if (!window.isEmpty() && getContext().deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    public void processHeartbeat(MySqlPartition partition, OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        checkEnqueuedSnapshotSignals(partition, offsetContext);
        readUntilGtidChange(partition, offsetContext);
    }

    private void readUntilGtidChange(MySqlPartition partition,
                                     OffsetContext offsetContext) throws InterruptedException {
        String currentGtid = getContext().getCurrentGtid(offsetContext);
        while (getContext().snapshotRunning() && getContext().reachedHighWatermark(currentGtid)) {
            getContext().closeWindow();
            sendWindowEvents(partition, offsetContext);
            readChunk(partition);
            if (currentGtid == null && getContext().watermarksChanged()) {
                return;
            }
        }
    }

    @Override
    public void processFilteredEvent(MySqlPartition partition,
                                     OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        checkEnqueuedSnapshotSignals(partition, offsetContext);
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition);
        }
    }

    public void enqueueDataCollectionNamesToSnapshot(
            List<String> dataCollectionIds, MessageId signalOffset) {
        getContext().enqueueDataCollectionsToSnapshot(dataCollectionIds, signalOffset);
    }

    @Override
    public void processTransactionStartedEvent(MySqlPartition partition,
                                               OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition);
        }
    }

    @Override
    public void processTransactionCommittedEvent(MySqlPartition partition,
                                                 OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        readUntilGtidChange(partition, offsetContext);
    }

    protected void updateLowWatermark() {
        getExecutedGtidSet(getContext()::setLowWatermark);
    }

    protected void updateHighWatermark() {
        getExecutedGtidSet(getContext()::setHighWatermark);
    }

    private void getExecutedGtidSet(Consumer<GtidSet> watermark) {
        try {
            jdbcConnection.query(showMasterStmt, rs -> {
                if (rs.next()) {
                    if (rs.getMetaData().getColumnCount() > 4) {
                        // This column exists only in MySQL 5.6.5 or later ...
                        final String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                        watermark.accept(new GtidSet(gtidSet));
                    } else {
                        throw new UnsupportedOperationException(
                                "Need to add support for executed GTIDs for versions prior to 5.6.5");
                    }
                }
            });
            jdbcConnection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void emitWindowOpen() {
        updateLowWatermark();
    }

    @Override
    protected void emitWindowClose(MySqlPartition partition) throws InterruptedException {
        updateHighWatermark();
        if (getContext().serverUuidChanged()) {
            rereadChunk(partition);
        }
    }

    @Override
    protected void sendEvent(MySqlPartition partition, EventDispatcher<MySqlPartition, T> dispatcher,
                             OffsetContext offsetContext, Object[] row)
            throws InterruptedException {
        SourceInfo sourceInfo = ((MySqlOffsetContext) offsetContext).getSource();
        String query = sourceInfo.getQuery();
        sourceInfo.setQuery(null);
        super.sendEvent(partition, dispatcher, offsetContext, row);
        sourceInfo.setQuery(query);
    }

    private void checkEnqueuedSnapshotSignals(
            MySqlPartition partition,
            OffsetContext offsetContext) throws InterruptedException {
        while (getContext().hasExecuteSnapshotSignals()) {
            addDataCollectionNamesToSnapshot(getContext().getExecuteSnapshotSignals(), partition, offsetContext);
        }
    }

    private void addDataCollectionNamesToSnapshot(ExecuteSnapshotPulsarSignal executeSnapshotSignal,
                                                  MySqlPartition partition,
                                                  OffsetContext offsetContext)
            throws InterruptedException {
        super.addDataCollectionNamesToSnapshot(partition, executeSnapshotSignal.getDataCollections(), offsetContext);
        // to string
        getContext().setSignalOffset(new String(executeSnapshotSignal.getSignalOffset().toByteArray()));
    }

    private MySqlReadOnlyIncrementalSnapshotContext<T> getContext() {
        return (MySqlReadOnlyIncrementalSnapshotContext<T>) context;
    }
}