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
package org.apache.pulsar.broker.service.persistent;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import lombok.AllArgsConstructor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.LedgerNotExistException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.event.data.MessageExpireEventData;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentMessageExpiryMonitor implements FindEntryCallback {
    private final ManagedCursor cursor;
    private final String subName;
    private final String topicName;
    private final Rate msgExpired;
    private final LongAdder totalMsgExpired;
    private final PersistentSubscription subscription;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int expirationCheckInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentMessageExpiryMonitor>
            expirationCheckInProgressUpdater = AtomicIntegerFieldUpdater
            .newUpdater(PersistentMessageExpiryMonitor.class, "expirationCheckInProgress");

    public PersistentMessageExpiryMonitor(String topicName, String subscriptionName, ManagedCursor cursor,
                                          PersistentSubscription subscription) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = subscriptionName;
        this.subscription = subscription;
        this.msgExpired = new Rate();
        this.totalMsgExpired = new LongAdder();
    }

    @VisibleForTesting
    public boolean isAutoSkipNonRecoverableData() {
        // check to avoid test failures
        return this.cursor.getManagedLedger() != null
                && this.cursor.getManagedLedger().getConfig().isAutoSkipNonRecoverableData();
    }

    public boolean expireMessages(int messageTTLInSeconds) {
        if (expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.info("[{}][{}] Starting message expiry check, ttl= {} seconds", topicName, subName,
                    messageTTLInSeconds);
            // First filter the entire Ledger reached TTL based on the Ledger closing time to avoid client clock skew
            checkExpiryByLedgerClosureTime(cursor, messageTTLInSeconds);
            // Some part of entries in active Ledger may have reached TTL, so we need to continue searching.
            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
                try {
                    long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                    return MessageImpl.isEntryExpired(messageTTLInSeconds, entryTimestamp);
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for expiry check", topicName, subName, e);
                } finally {
                    entry.release();
                }
                return false;
            }, this, null);
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore expire-message scheduled task, last check is still running", topicName,
                        subName);
            }
            return false;
        }
    }
    private void checkExpiryByLedgerClosureTime(ManagedCursor cursor, int messageTTLInSeconds) {
        if (messageTTLInSeconds <= 0) {
            return;
        }
        if (cursor instanceof ManagedCursorImpl managedCursor) {
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) managedCursor.getManagedLedger();
            Position deletedPosition = managedCursor.getMarkDeletedPosition();
            SortedMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfoSortedMap =
                    managedLedger.getLedgersInfo().subMap(deletedPosition.getLedgerId(), true,
                            managedLedger.getLedgersInfo().lastKey(), true);
            MLDataFormats.ManagedLedgerInfo.LedgerInfo info = null;
            for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo : ledgerInfoSortedMap.values()) {
                if (!ledgerInfo.hasTimestamp() || ledgerInfo.getTimestamp() == 0L
                        || !MessageImpl.isEntryExpired(messageTTLInSeconds, ledgerInfo.getTimestamp())) {
                    break;
                }
                info = ledgerInfo;
            }
            if (info != null && info.getLedgerId() > -1) {
                PositionImpl position = PositionImpl.get(info.getLedgerId(), info.getEntries() - 1);
                if (((PositionImpl) managedLedger.getLastConfirmedEntry()).compareTo(position) < 0) {
                    findEntryComplete(managedLedger.getLastConfirmedEntry(), null);
                } else {
                    findEntryComplete(position, null);
                }
            }
        }
    }

    public boolean expireMessages(Position messagePosition) {
        // If it's beyond last position of this topic, do nothing.
        if (((PositionImpl) subscription.getTopic().getLastPosition()).compareTo((PositionImpl) messagePosition) < 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore expire-message scheduled task, given position {} is beyond "
                         + "current topic's last position {}", topicName, subName, messagePosition,
                        subscription.getTopic().getLastPosition());
            }
            return false;
        }
        if (expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.info("[{}][{}] Starting message expiry check, position= {} seconds", topicName, subName,
                    messagePosition);

            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
                try {
                    // If given position larger than entry position.
                    return ((PositionImpl) entry.getPosition()).compareTo((PositionImpl) messagePosition) <= 0;
                } finally {
                    entry.release();
                }
            }, this, null);
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore expire-message scheduled task, last check is still running", topicName,
                        subName);
            }
            return false;
        }
    }


    public void updateRates() {
        msgExpired.calculateRate();
    }

    public double getMessageExpiryRate() {
        return msgExpired.getRate();
    }

    public long getTotalMessageExpired() {
        return totalMsgExpired.sum();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentMessageExpiryMonitor.class);

    @AllArgsConstructor
    class MessageExpiryMarkDeleteCallback implements MarkDeleteCallback {
        private Position position;

        @Override
        public void markDeleteComplete(Object ctx) {
            long numMessagesExpired = (long) ctx - cursor.getNumberOfEntriesInBacklog(false);
            msgExpired.recordMultipleEvents(numMessagesExpired, 0 /* no value stats */);
            totalMsgExpired.add(numMessagesExpired);
            // If the subscription is a Key_Shared subscription, we should to trigger message dispatch.
            if (subscription.getType() == SubType.Key_Shared) {
                subscription.getDispatcher().markDeletePositionMoveForward();
            }
            subscription.getTopic().getBrokerService().getTopicEventsDispatcher()
                    .newEvent(topicName, TopicEvent.MESSAGE_EXPIRE)
                    .data(MessageExpireEventData.builder()
                            .subscriptionName(subName)
                            .position(position.toString())
                            .build())
                    .dispatch();
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted {} messages", topicName, subName, numMessagesExpired);
            }
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Message expiry failed - mark delete failed", topicName, subName, exception);
            updateRates();
        }
    }

    @VisibleForTesting
    public MarkDeleteCallback getMarkDeleteCallback(Position position) {
        return new MessageExpiryMarkDeleteCallback(position);
    }

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        if (position != null) {
            log.info("[{}][{}] Expiring all messages until position {}", topicName, subName, position);
            Position prevMarkDeletePos = cursor.getMarkDeletedPosition();
            cursor.asyncMarkDelete(position, cursor.getProperties(), getMarkDeleteCallback(position),
                    cursor.getNumberOfEntriesInBacklog(false));
            if (!Objects.equals(cursor.getMarkDeletedPosition(), prevMarkDeletePos) && subscription != null) {
                subscription.updateLastMarkDeleteAdvancedTimestamp();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No messages to expire", topicName, subName);
            }
            updateRates();
        }
        expirationCheckInProgress = FALSE;
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Finding expired entry operation failed", topicName, subName, exception);
        }
        if (isAutoSkipNonRecoverableData() && failedReadPosition.isPresent()
                && (exception instanceof NonRecoverableLedgerException)) {
            log.warn("[{}][{}] read failed from ledger at position:{} : {}", topicName, subName, failedReadPosition,
                    exception.getMessage());
            if (exception instanceof LedgerNotExistException) {
                long failedLedgerId = failedReadPosition.get().getLedgerId();
                ManagedLedgerImpl ledger = ((ManagedLedgerImpl) cursor.getManagedLedger());
                Position lastPositionInLedger = ledger.getOptionalLedgerInfo(failedLedgerId)
                        .map(ledgerInfo -> PositionImpl.get(failedLedgerId, ledgerInfo.getEntries() - 1))
                        .orElseGet(() -> {
                            Long nextExistingLedger = ledger.getNextValidLedger(failedReadPosition.get().getLedgerId());
                            if (nextExistingLedger == null) {
                                log.info("[{}] [{}] Couldn't find next next valid ledger for expiry monitor when find "
                                                + "entry failed {}", ledger.getName(), ledger.getName(),
                                        failedReadPosition);
                                return (PositionImpl) failedReadPosition.get();
                            } else {
                                return PositionImpl.get(nextExistingLedger, -1);
                            }
                        });
                log.info("[{}][{}] ledger not existed, will complete the last position of the non-existed"
                        + " ledger:{}", topicName, subName, lastPositionInLedger);
                findEntryComplete(lastPositionInLedger, ctx);
            } else {
                findEntryComplete(failedReadPosition.get(), ctx);
            }
            return;
        }
        expirationCheckInProgress = FALSE;
        updateRates();
    }
}
