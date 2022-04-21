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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Enumeration;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.util.Errors;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ManagedLedgerOfflineBacklog {

    private final byte[] password;
    private final BookKeeper.DigestType digestType;
    private static final int META_READ_TIMEOUT_SECONDS = 60;
    private final boolean accurate;
    private final String brokerName;

    public ManagedLedgerOfflineBacklog(DigestType digestType, byte[] password, String brokerName,
            boolean accurate) {
        this.digestType = BookKeeper.DigestType.fromApiDigestType(digestType);
        this.password = password;
        this.accurate = accurate;
        this.brokerName = brokerName;
    }

    // need a better way than to duplicate the functionality below from ML
    private long getNumberOfEntries(Range<PositionImpl> range,
            NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers) {
        PositionImpl fromPosition = range.lowerEndpoint();
        boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
        PositionImpl toPosition = range.upperEndpoint();
        boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

        if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
            // If the 2 positions are in the same ledger
            long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
            count += fromIncluded ? 1 : 0;
            count += toIncluded ? 1 : 0;
            return count;
        } else {
            long count = 0;
            // If the from & to are pointing to different ledgers, then we need to :
            // 1. Add the entries in the ledger pointed by toPosition
            count += toPosition.getEntryId();
            count += toIncluded ? 1 : 0;

            // 2. Add the entries in the ledger pointed by fromPosition
            MLDataFormats.ManagedLedgerInfo.LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : ledgers
                    .subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false).values()) {
                count += ls.getEntries();
            }

            return count;
        }
    }

    public PersistentOfflineTopicStats getEstimatedUnloadedTopicBacklog(ManagedLedgerFactoryImpl factory,
            String managedLedgerName) throws Exception {
        return estimateUnloadedTopicBacklog(factory, TopicName.get("persistent://" + managedLedgerName));
    }

    public PersistentOfflineTopicStats estimateUnloadedTopicBacklog(ManagedLedgerFactoryImpl factory,
            TopicName topicName) throws Exception {
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        long numberOfEntries = 0;
        long totalSize = 0;
        final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
        final PersistentOfflineTopicStats offlineTopicStats = new PersistentOfflineTopicStats(managedLedgerName,
                brokerName);

        // calculate total managed ledger size and number of entries without loading the topic
        readLedgerMeta(factory, topicName, ledgers);
        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : ledgers.values()) {
            numberOfEntries += ls.getEntries();
            totalSize += ls.getSize();
            if (accurate) {
                offlineTopicStats.addLedgerDetails(ls.getEntries(), ls.getTimestamp(), ls.getSize(), ls.getLedgerId());
            }
        }
        offlineTopicStats.totalMessages = numberOfEntries;
        offlineTopicStats.storageSize = totalSize;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Total number of entries - {} and size - {}", managedLedgerName, numberOfEntries, totalSize);
        }

        // calculate per cursor message backlog
        calculateCursorBacklogs(factory, topicName, ledgers, offlineTopicStats);
        offlineTopicStats.statGeneratedAt.setTime(System.currentTimeMillis());

        return offlineTopicStats;
    }

    private void readLedgerMeta(final ManagedLedgerFactoryImpl factory, final TopicName topicName,
            final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers) throws Exception {
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        MetaStore store = factory.getMetaStore();
        BookKeeper bk = factory.getBookKeeper();
        final CountDownLatch mlMetaCounter = new CountDownLatch(1);

        store.getManagedLedgerInfo(managedLedgerName, false /* createIfMissing */,
                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
                        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                            ledgers.put(ls.getLedgerId(), ls);
                        }

                        // find no of entries in last ledger
                        if (!ledgers.isEmpty()) {
                            final long id = ledgers.lastKey();
                            AsyncCallback.OpenCallback opencb = (rc, lh, ctx1) -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Opened ledger {}: {}", managedLedgerName, id,
                                            BKException.getMessage(rc));
                                }
                                if (rc == BKException.Code.OK) {
                                    MLDataFormats.ManagedLedgerInfo.LedgerInfo info =
                                        MLDataFormats.ManagedLedgerInfo.LedgerInfo
                                            .newBuilder().setLedgerId(id).setEntries(lh.getLastAddConfirmed() + 1)
                                            .setSize(lh.getLength()).setTimestamp(System.currentTimeMillis()).build();
                                    ledgers.put(id, info);
                                    mlMetaCounter.countDown();
                                } else if (Errors.isNoSuchLedgerExistsException(rc)) {
                                    log.warn("[{}] Ledger not found: {}", managedLedgerName, ledgers.lastKey());
                                    ledgers.remove(ledgers.lastKey());
                                    mlMetaCounter.countDown();
                                } else {
                                    log.error("[{}] Failed to open ledger {}: {}", managedLedgerName, id,
                                            BKException.getMessage(rc));
                                    mlMetaCounter.countDown();
                                }
                            };

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Opening ledger {}", managedLedgerName, id);
                            }
                            try {
                                bk.asyncOpenLedgerNoRecovery(id, digestType, password, opencb, null);
                            } catch (Exception e) {
                                log.warn("[{}] Failed to open ledger {}: {}", managedLedgerName, id, e);
                                mlMetaCounter.countDown();
                            }
                        } else {
                            log.warn("[{}] Ledger list empty", managedLedgerName);
                            mlMetaCounter.countDown();
                        }
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        log.warn("[{}] Unable to obtain managed ledger metadata - {}", managedLedgerName, e);
                        mlMetaCounter.countDown();
                    }
                });

        if (accurate) {
            // block until however long it takes for operation to complete
            mlMetaCounter.await();
        } else {
            mlMetaCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        }
    }

    private void calculateCursorBacklogs(final ManagedLedgerFactoryImpl factory, final TopicName topicName,
            final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers,
            final PersistentOfflineTopicStats offlineTopicStats) throws Exception {

        if (ledgers.isEmpty()) {
            return;
        }
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        MetaStore store = factory.getMetaStore();
        BookKeeper bk = factory.getBookKeeper();
        final CountDownLatch allCursorsCounter = new CountDownLatch(1);
        final long errorInReadingCursor = -1;
        ConcurrentOpenHashMap<String, Long> ledgerRetryMap =
                ConcurrentOpenHashMap.<String, Long>newBuilder().build();

        final MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgers.lastEntry().getValue();
        final PositionImpl lastLedgerPosition = new PositionImpl(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Last ledger position {}", managedLedgerName, lastLedgerPosition);
        }

        store.getCursors(managedLedgerName, new MetaStore.MetaStoreCallback<List<String>>() {
            @Override
            public void operationComplete(List<String> cursors, Stat v) {
                // Load existing cursors
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Found {} cursors", managedLedgerName, cursors.size());
                }

                if (cursors.isEmpty()) {
                    allCursorsCounter.countDown();
                    return;
                }

                final CountDownLatch cursorCounter = new CountDownLatch(cursors.size());

                for (final String cursorName : cursors) {
                    // determine subscription position from cursor ledger
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loading cursor {}", managedLedgerName, cursorName);
                    }

                    AsyncCallback.OpenCallback cursorLedgerOpenCb = (rc, lh, ctx1) -> {
                        long ledgerId = lh.getId();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Opened cursor ledger {} for cursor {}. rc={}", managedLedgerName, ledgerId,
                                    cursorName, rc);
                        }
                        if (rc != BKException.Code.OK) {
                            log.warn("[{}] Error opening metadata ledger {} for cursor {}: {}", managedLedgerName,
                                    ledgerId, cursorName, BKException.getMessage(rc));
                            cursorCounter.countDown();
                            return;
                        }
                        long lac = lh.getLastAddConfirmed();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Cursor {} LAC {} read from ledger {}", managedLedgerName, cursorName, lac,
                                    ledgerId);
                        }

                        if (lac == LedgerHandle.INVALID_ENTRY_ID) {
                            // save the ledger id and cursor to retry outside of this call back
                            // since we are trying to read the same cursor ledger, we will block until
                            // this current callback completes, since an attempt to read the entry
                            // will block behind this current operation to complete
                            ledgerRetryMap.put(cursorName, ledgerId);
                            log.info("[{}] Cursor {} LAC {} read from ledger {}", managedLedgerName, cursorName, lac,
                                    ledgerId);
                            cursorCounter.countDown();
                            return;
                        }
                        final long entryId = lac;
                        // read last acked message position for subscription
                        lh.asyncReadEntries(entryId, entryId, new AsyncCallback.ReadCallback() {
                            @Override
                            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                                    Object ctx) {
                                try {
                                    if (log.isDebugEnabled()) {
                                        log.debug("readComplete rc={} entryId={}", rc, entryId);
                                    }
                                    if (rc != BKException.Code.OK) {
                                        log.warn("[{}] Error reading from metadata ledger {} for cursor {}: {}",
                                                managedLedgerName, ledgerId, cursorName, BKException.getMessage(rc));
                                        // indicate that this cursor should be excluded
                                        offlineTopicStats.addCursorDetails(cursorName, errorInReadingCursor,
                                                lh.getId());
                                    } else {
                                        LedgerEntry entry = seq.nextElement();
                                        MLDataFormats.PositionInfo positionInfo;
                                        try {
                                            positionInfo = MLDataFormats.PositionInfo.parseFrom(entry.getEntry());
                                        } catch (InvalidProtocolBufferException e) {
                                            log.warn(
                                                "[{}] Error reading position from metadata ledger {} for cursor {}: {}",
                                                managedLedgerName, ledgerId, cursorName, e);
                                            offlineTopicStats.addCursorDetails(cursorName, errorInReadingCursor,
                                                    lh.getId());
                                            return;
                                        }
                                        final PositionImpl lastAckedMessagePosition = new PositionImpl(positionInfo);
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Cursor {} MD {} read last ledger position {}",
                                                    managedLedgerName, cursorName, lastAckedMessagePosition,
                                                    lastLedgerPosition);
                                        }
                                        // calculate cursor backlog
                                        Range<PositionImpl> range = Range.openClosed(lastAckedMessagePosition,
                                                lastLedgerPosition);
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Calculating backlog for cursor {} using range {}",
                                                    managedLedgerName, cursorName, range);
                                        }
                                        long cursorBacklog = getNumberOfEntries(range, ledgers);
                                        offlineTopicStats.messageBacklog += cursorBacklog;
                                        offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, lh.getId());
                                    }
                                } finally {
                                    cursorCounter.countDown();
                                }
                            }
                        }, null);

                    }; // end of cursor meta read callback

                    store.asyncGetCursorInfo(managedLedgerName, cursorName,
                            new MetaStore.MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {
                                @Override
                                public void operationComplete(MLDataFormats.ManagedCursorInfo info,
                                        Stat stat) {
                                    long cursorLedgerId = info.getCursorsLedgerId();
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Cursor {} meta-data read ledger id {}", managedLedgerName,
                                                cursorName, cursorLedgerId);
                                    }
                                    if (cursorLedgerId != -1) {
                                        bk.asyncOpenLedgerNoRecovery(cursorLedgerId, digestType, password,
                                                cursorLedgerOpenCb, null);
                                    } else {
                                        PositionImpl lastAckedMessagePosition = new PositionImpl(
                                                info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());
                                        Range<PositionImpl> range = Range.openClosed(lastAckedMessagePosition,
                                                lastLedgerPosition);
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Calculating backlog for cursor {} using range {}",
                                                    managedLedgerName, cursorName, range);
                                        }
                                        long cursorBacklog = getNumberOfEntries(range, ledgers);
                                        offlineTopicStats.messageBacklog += cursorBacklog;
                                        offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, cursorLedgerId);
                                        cursorCounter.countDown();
                                    }

                                }

                                @Override
                                public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                    log.warn("[{}] Unable to obtain cursor ledger for cursor {}: {}", managedLedgerName,
                                            cursorName, e);
                                    cursorCounter.countDown();
                                }
                            });
                } // for every cursor find backlog
                try {
                    if (accurate) {
                        cursorCounter.await();
                    } else {
                        cursorCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    log.warn("[{}] Error reading subscription positions{}", managedLedgerName, e);
                } finally {
                    allCursorsCounter.countDown();
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                log.warn("[{}] Failed to get the cursors list", managedLedgerName, e);
                allCursorsCounter.countDown();
            }
        });
        if (accurate) {
            allCursorsCounter.await();
        } else {
            allCursorsCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        // go through ledgers where LAC was -1
        if (accurate && ledgerRetryMap.size() > 0) {
            ledgerRetryMap.forEach((cursorName, ledgerId) -> {
                if (log.isDebugEnabled()) {
                    log.debug("Cursor {} Ledger {} Trying to obtain MD from BkAdmin", cursorName, ledgerId);
                }
                PositionImpl lastAckedMessagePosition = tryGetMDPosition(bk, ledgerId, cursorName);
                if (lastAckedMessagePosition == null) {
                    log.warn("[{}] Cursor {} read from ledger {}. Unable to determine cursor position",
                            managedLedgerName, cursorName, ledgerId);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cursor {} read from ledger using bk admin {}. position {}", managedLedgerName,
                                cursorName, ledgerId, lastAckedMessagePosition);
                    }
                    // calculate cursor backlog
                    Range<PositionImpl> range = Range.openClosed(lastAckedMessagePosition, lastLedgerPosition);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Calculating backlog for cursor {} using range {}", managedLedgerName,
                                cursorName, range);
                    }
                    long cursorBacklog = getNumberOfEntries(range, ledgers);
                    offlineTopicStats.messageBacklog += cursorBacklog;
                    offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, ledgerId);
                }
            });
        }
    }

    private PositionImpl tryGetMDPosition(BookKeeper bookKeeper, long ledgerId, String cursorName) {
        BookKeeperAdmin bookKeeperAdmin = null;
        long lastEntry = LedgerHandle.INVALID_ENTRY_ID;
        PositionImpl lastAckedMessagePosition = null;
        try {
            bookKeeperAdmin = new BookKeeperAdmin(bookKeeper);
            for (LedgerEntry ledgerEntry : bookKeeperAdmin.readEntries(ledgerId, 0, lastEntry)) {
                lastEntry = ledgerEntry.getEntryId();
                if (log.isDebugEnabled()) {
                    log.debug(" Read entry {} from ledger {} for cursor {}", lastEntry, ledgerId, cursorName);
                }
                MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(ledgerEntry.getEntry());
                lastAckedMessagePosition = new PositionImpl(positionInfo);
                if (log.isDebugEnabled()) {
                    log.debug("Cursor {} read position {}", cursorName, lastAckedMessagePosition);
                }
            }
        } catch (Exception e) {
            log.warn("Unable to determine LAC for ledgerId {} for cursor {}: {}", ledgerId, cursorName, e);
        } finally {
            if (bookKeeperAdmin != null) {
                try {
                    bookKeeperAdmin.close();
                } catch (Exception e) {
                    log.warn("Unable to close bk admin for ledgerId {} for cursor {}", ledgerId, cursorName, e);
                }
            }

        }
        return lastAckedMessagePosition;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerOfflineBacklog.class);
}
