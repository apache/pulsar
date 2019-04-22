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
package org.apache.pulsar.sql.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import lombok.Data;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import com.google.common.base.Predicate;
import org.apache.bookkeeper.conf.ClientConfiguration;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries;

public class PulsarSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarSplitManager.class);

    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingStrategy splitSchedulingStrategy) {

        int numSplits = this.pulsarConnectorConfig.getTargetNumSplits();

        PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = layoutHandle.getTable();
        TupleDomain<ColumnHandle> tupleDomain = layoutHandle.getTupleDomain();

        TopicName topicName = TopicName.get("persistent", NamespaceName.get(tableHandle.getSchemaName()),
                tableHandle.getTableName());

        SchemaInfo schemaInfo;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName()));
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get schema for topic "
                    + String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName())
                    + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
        }

        Collection<PulsarSplit> splits;
        try {
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                splits = getSplitsNonPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo, tupleDomain);
                log.debug("Splits for non-partitioned topic %s: %s", topicName, splits);
            } else {
                splits = getSplitsPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo, tupleDomain);
                log.debug("Splits for partitioned topic %s: %s", topicName, splits);
            }
        } catch (Exception e) {
            log.error(e, "Failed to get splits");
            throw new RuntimeException(e);
        }
        return new FixedSplitSource(splits);
    }

    @VisibleForTesting
    ManagedLedgerFactory getManagedLedgerFactory() throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(this.pulsarConnectorConfig.getZookeeperUri())
                .setClientTcpNoDelay(false)
                .setStickyReadsEnabled(true)
                .setUseV2WireProtocol(true);
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain) throws Exception {
        int numPartitions;
        try {
            numPartitions = (this.pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString())).partitions;
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get metadata for partitioned topic "
                    + topicName + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(),e);
        }

        int actualNumSplits = Math.max(numPartitions, numSplits);

        int splitsPerPartition = actualNumSplits / numPartitions;

        int splitRemainder = actualNumSplits % numPartitions;

        ManagedLedgerFactory managedLedgerFactory = getManagedLedgerFactory();

        try {
            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numPartitions; i++) {

                int splitsForThisPartition = (splitRemainder > i) ? splitsPerPartition + 1 : splitsPerPartition;
                splits.addAll(
                        getSplitsForTopic(
                                topicName.getPartition(i).getPersistenceNamingEncoding(),
                                managedLedgerFactory,
                                splitsForThisPartition,
                                tableHandle,
                                schemaInfo,
                                topicName.getPartition(i).getLocalName(),
                                tupleDomain)
                );
            }
            return splits;
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain) throws Exception {
        ManagedLedgerFactory managedLedgerFactory = null;
        try {
            managedLedgerFactory = getManagedLedgerFactory();

            return getSplitsForTopic(
                    topicName.getPersistenceNamingEncoding(),
                    managedLedgerFactory,
                    numSplits,
                    tableHandle,
                    schemaInfo,
                    tableHandle.getTableName(), tupleDomain);
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsForTopic(String topicNamePersistenceEncoding,
                                              ManagedLedgerFactory managedLedgerFactory,
                                              int numSplits,
                                              PulsarTableHandle tableHandle,
                                              SchemaInfo schemaInfo, String tableName,
                                              TupleDomain<ColumnHandle> tupleDomain)
            throws ManagedLedgerException, InterruptedException {

        ReadOnlyCursor readOnlyCursor = null;
        try {
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    PositionImpl.earliest, new ManagedLedgerConfig());

            long numEntries = readOnlyCursor.getNumberOfEntries();
            if (numEntries <= 0) {
                return Collections.EMPTY_LIST;
            }

            PredicatePushdownInfo predicatePushdownInfo = PredicatePushdownInfo.getPredicatePushdownInfo(
                    this.connectorId,
                    tupleDomain,
                    managedLedgerFactory,
                    topicNamePersistenceEncoding,
                    numEntries);

            PositionImpl initialStartPosition;
            if (predicatePushdownInfo != null) {
                numEntries = predicatePushdownInfo.getNumOfEntries();
                initialStartPosition = predicatePushdownInfo.getStartPosition();
            } else {
                initialStartPosition = (PositionImpl) readOnlyCursor.getReadPosition();
            }


            readOnlyCursor.close();
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    initialStartPosition, new ManagedLedgerConfig());

            long remainder = numEntries % numSplits;

            long avgEntriesPerSplit = numEntries / numSplits;

            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numSplits; i++) {
                long entriesForSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;
                PositionImpl startPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                readOnlyCursor.skipEntries(Math.toIntExact(entriesForSplit));
                PositionImpl endPosition = (PositionImpl) readOnlyCursor.getReadPosition();

                splits.add(new PulsarSplit(i, this.connectorId,
                        tableHandle.getSchemaName(),
                        tableName,
                        entriesForSplit,
                        new String(schemaInfo.getSchema()),
                        schemaInfo.getType(),
                        startPosition.getEntryId(),
                        endPosition.getEntryId(),
                        startPosition.getLedgerId(),
                        endPosition.getLedgerId(),
                        tupleDomain));
            }
            return splits;
        } finally {
            if (readOnlyCursor != null) {
                try {
                    readOnlyCursor.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @Data
    private static class PredicatePushdownInfo {
        private PositionImpl startPosition;
        private PositionImpl endPosition;
        private long numOfEntries;

        private PredicatePushdownInfo(PositionImpl startPosition, PositionImpl endPosition, long numOfEntries) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.numOfEntries = numOfEntries;
        }

        public static PredicatePushdownInfo getPredicatePushdownInfo(String connectorId,
                                                                     TupleDomain<ColumnHandle> tupleDomain,
                                                                     ManagedLedgerFactory managedLedgerFactory,
                                                                     String topicNamePersistenceEncoding,
                                                                     long totalNumEntries) throws
                ManagedLedgerException, InterruptedException {

            ReadOnlyCursor readOnlyCursor = null;
            try {
                readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                        topicNamePersistenceEncoding,
                        PositionImpl.earliest, new ManagedLedgerConfig());

                if (tupleDomain.getDomains().isPresent()) {
                    Domain domain = tupleDomain.getDomains().get().get(PulsarInternalColumn.PUBLISH_TIME
                            .getColumnHandle(connectorId, false));
                    if (domain != null) {
                        // TODO support arbitrary number of ranges
                        // only worry about one range for now
                        if (domain.getValues().getRanges().getRangeCount() == 1) {

                            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

                            Long upperBoundTs = null;
                            Long lowerBoundTs = null;

                            Range range = domain.getValues().getRanges().getOrderedRanges().get(0);

                            if (!range.getHigh().isUpperUnbounded()) {
                                upperBoundTs = new SqlTimestampWithTimeZone(range.getHigh().getValueBlock().get()
                                        .getLong(0, 0)).getMillisUtc();
                            }

                            if (!range.getLow().isLowerUnbounded()) {
                                lowerBoundTs = new SqlTimestampWithTimeZone(range.getLow().getValueBlock().get()
                                        .getLong(0, 0)).getMillisUtc();
                            }

                            PositionImpl overallStartPos;
                            if (lowerBoundTs == null) {
                                overallStartPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallStartPos = findPosition(readOnlyCursor, lowerBoundTs);
                            }

                            PositionImpl overallEndPos;
                            if (upperBoundTs == null) {

                                readOnlyCursor.skipEntries(Math.toIntExact(totalNumEntries));
                                overallEndPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallEndPos = findPosition(readOnlyCursor, upperBoundTs);
                            }

                            // Just use a close bound since presto can always filter out the extra entries even if
                            // the bound
                            // should be open or a mixture of open and closed
                            com.google.common.collect.Range<PositionImpl> posRange
                                    = com.google.common.collect.Range.range(overallStartPos,
                                    com.google.common.collect.BoundType.CLOSED,
                                    overallEndPos, com.google.common.collect.BoundType.CLOSED);

                            long numOfEntries = readOnlyCursor.getNumberOfEntries(posRange) - 1;

                            PredicatePushdownInfo predicatePushdownInfo
                                    = new PredicatePushdownInfo(overallStartPos, overallEndPos, numOfEntries);
                            log.debug("Predicate pushdown optimization calculated: %s", predicatePushdownInfo);
                            return predicatePushdownInfo;
                        }
                    }
                }
            } finally {
                if (readOnlyCursor != null) {
                    readOnlyCursor.close();
                }
            }
            return null;
        }
    }

    private static PositionImpl findPosition(ReadOnlyCursor readOnlyCursor, long timestamp) throws
            ManagedLedgerException,
            InterruptedException {
        return (PositionImpl) readOnlyCursor.findNewestMatching(SearchAllAvailableEntries, new Predicate<Entry>() {
            @Override
            public boolean apply(Entry entry) {
                MessageImpl msg = null;
                try {
                    msg = MessageImpl.deserialize(entry.getDataBuffer());

                    return msg.getPublishTime() <= timestamp;
                } catch (Exception e) {
                    log.error(e, "Failed To deserialize message when finding position with error: %s", e);
                } finally {
                    entry.release();
                    if (msg != null) {
                        msg.recycle();
                    }
                }
                return false;
            }
        });
    }
}
