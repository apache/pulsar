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
package org.apache.pulsar.broker.service.persistent;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
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
    private final boolean autoSkipNonRecoverableData;
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
        // check to avoid test failures
        this.autoSkipNonRecoverableData = this.cursor.getManagedLedger() != null
                && this.cursor.getManagedLedger().getConfig().isAutoSkipNonRecoverableData();
    }

    public boolean expireMessages(int messageTTLInSeconds) {
        if (expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.info("[{}][{}] Starting message expiry check, ttl= {} seconds", topicName, subName,
                    messageTTLInSeconds);

            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
                try {
                    long entryTimestamp = MessageImpl.getEntryTimestamp(entry.getDataBuffer());
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

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            long numMessagesExpired = (long) ctx - cursor.getNumberOfEntriesInBacklog(false);
            msgExpired.recordMultipleEvents(numMessagesExpired, 0 /* no value stats */);
            totalMsgExpired.add(numMessagesExpired);
            updateRates();
            // If the subscription is a Key_Shared subscription, we should to trigger message dispatch.
            if (subscription != null && subscription.getType() == SubType.Key_Shared) {
                subscription.getDispatcher().markDeletePositionMoveForward();
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted {} messages", topicName, subName, numMessagesExpired);
            }
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Message expiry failed - mark delete failed", topicName, subName, exception);
            updateRates();
        }
    };

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        if (position != null) {
            log.info("[{}][{}] Expiring all messages until position {}", topicName, subName, position);
            Position prevMarkDeletePos = cursor.getMarkDeletedPosition();
            cursor.asyncMarkDelete(position, markDeleteCallback, cursor.getNumberOfEntriesInBacklog(false));
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
        if (autoSkipNonRecoverableData && failedReadPosition.isPresent()
                && (exception instanceof NonRecoverableLedgerException)) {
            log.warn("[{}][{}] read failed from ledger at position:{} : {}", topicName, subName, failedReadPosition,
                    exception.getMessage());
            findEntryComplete(failedReadPosition.get(), ctx);
        }
        expirationCheckInProgress = FALSE;
        updateRates();
    }
}
