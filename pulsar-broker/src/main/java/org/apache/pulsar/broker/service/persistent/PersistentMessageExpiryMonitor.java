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
import io.github.merlimat.slog.Logger;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.LedgerNotExistException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.pulsar.broker.service.MessageExpirer;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.stats.Rate;
import org.jspecify.annotations.Nullable;
/**
 */
public class PersistentMessageExpiryMonitor implements FindEntryCallback, MessageExpirer {

    private static final Logger LOG = Logger.get(PersistentMessageExpiryMonitor.class);
    private final Logger log;

    private final ManagedCursor cursor;
    private final String subName;
    private final PersistentTopic topic;
    private final String topicName;
    private final Rate msgExpired;
    private final LongAdder totalMsgExpired;
    private final PersistentSubscription subscription;
    private final PersistentMessageFinder finder;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int expirationCheckInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentMessageExpiryMonitor>
            expirationCheckInProgressUpdater = AtomicIntegerFieldUpdater
            .newUpdater(PersistentMessageExpiryMonitor.class, "expirationCheckInProgress");

    public PersistentMessageExpiryMonitor(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                          @Nullable PersistentSubscription subscription) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.cursor = cursor;
        this.subName = subscriptionName;
        this.subscription = subscription;
        this.msgExpired = new Rate();
        this.totalMsgExpired = new LongAdder();
        this.log = LOG.with()
                .attr("topic", topicName)
                .attr("subscription", subscriptionName)
                .build();
        int managedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis = topic.getBrokerService().pulsar()
                .getConfig().getManagedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis();
        this.finder = new PersistentMessageFinder(topicName, cursor,
                managedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis);
    }

    @VisibleForTesting
    public boolean isAutoSkipNonRecoverableData() {
        // check to avoid test failures
        return this.cursor.getManagedLedger() != null
                && this.cursor.getManagedLedger().getConfig().isAutoSkipNonRecoverableData();
    }

    @Override
    public CompletableFuture<Boolean> expireMessagesAsync(int messageTTLInSeconds) {
        return CompletableFuture.supplyAsync(() -> expireMessages(messageTTLInSeconds), topic.getOrderedExecutor());
    }

    @Override
    public boolean expireMessages(int messageTTLInSeconds) {
        if (!expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.debug()
                    .log("Ignore expire-message scheduled task, last check is still running");
            return false;
        }
        log.info()
                .attr("messageTTLInSeconds", messageTTLInSeconds)
                .log("Starting message expiry check");
        // First filter the entire Ledger reached TTL based on the Ledger closing time to avoid client clock skew
        checkExpiryByLedgerClosureTime(cursor, messageTTLInSeconds);
        // Some part of entries in active Ledger may have reached TTL, so we need to continue searching.
        long expiredMessageTimestamp = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(messageTTLInSeconds);
        finder.findMessages(expiredMessageTimestamp, this);
        return true;
    }

    private void checkExpiryByLedgerClosureTime(ManagedCursor cursor, int messageTTLInSeconds) {
        if (messageTTLInSeconds <= 0) {
            return;
        }
        ManagedLedger managedLedger = cursor.getManagedLedger();
        Position deletedPosition = cursor.getMarkDeletedPosition();
        SortedMap<Long, ManagedLedgerInfo.LedgerInfo> ledgerInfoSortedMap =
                managedLedger.getLedgersInfo().subMap(deletedPosition.getLedgerId(), true,
                        managedLedger.getLedgersInfo().lastKey(), true);
        ManagedLedgerInfo.LedgerInfo info = null;
        for (ManagedLedgerInfo.LedgerInfo ledgerInfo : ledgerInfoSortedMap.values()) {
            if (!ledgerInfo.hasTimestamp() || ledgerInfo.getTimestamp() == 0L
                    || !MessageImpl.isEntryExpired(messageTTLInSeconds, ledgerInfo.getTimestamp())) {
                break;
            }
            info = ledgerInfo;
        }
        if (info != null && info.getLedgerId() > -1) {
            Position position = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
            if (managedLedger.getLastConfirmedEntry().compareTo(position) < 0) {
                findEntryComplete(managedLedger.getLastConfirmedEntry(), null);
            } else {
                findEntryComplete(position, null);
            }
        }
    }

    @Override
    public boolean expireMessages(Position messagePosition) {
        // If it's beyond last position of this topic, do nothing.
        Position topicLastPosition = this.topic.getLastPosition();
        ManagedLedger managedLedger = cursor.getManagedLedger();
        if (managedLedger instanceof ManagedLedgerImpl ml) {
            // Confirm the position is valid.
            Optional<ManagedLedgerInfo.LedgerInfo> ledgerInfoOptional =
                    ml.getOptionalLedgerInfo(messagePosition.getLedgerId());
            if (ledgerInfoOptional.isPresent()) {
                if (messagePosition.getEntryId() >= 0
                        && ledgerInfoOptional.get().getEntries() - 1 >= messagePosition.getEntryId()) {
                    findEntryComplete(messagePosition, null);
                    return true;
                }
            }
        }
        // Fallback to the slower solution if the managed ledger is not an instance of ManagedLedgerImpl.
        if (topicLastPosition.compareTo(messagePosition) < 0) {
            log.debug()
                    .attr("messagePosition", messagePosition)
                    .attr("topicLastPosition", topicLastPosition)
                    .log("Ignore expire-message scheduled task, given position is beyond "
                            + "current topic's last position");
            return false;
        }
        if (expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.info()
                    .attr("messagePosition", messagePosition)
                    .log("Starting message expiry check");

            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
                try {
                    // If given position larger than entry position.
                    return entry.getPosition().compareTo(messagePosition) <= 0;
                } finally {
                    entry.release();
                }
            }, this, null);
            return true;
        } else {
            log.debug("Ignore expire-message scheduled task, last check is still running");
            return false;
        }
    }


    public void updateRates() {
        msgExpired.calculateRate();
    }

    public double getMessageExpiryRate() {
        return msgExpired.getRate();
    }

    public long getMessageExpiryCount() {
        return msgExpired.getCount();
    }

    public long getTotalMessageExpired() {
        return totalMsgExpired.sum();
    }
    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            long numMessagesExpired = (long) ctx - cursor.getNumberOfEntriesInBacklog(false);
            msgExpired.recordMultipleEvents(numMessagesExpired, 0 /* no value stats */);
            totalMsgExpired.add(numMessagesExpired);
            // If the subscription is a Key_Shared subscription, we should to trigger message dispatch.
            if (subscription != null && subscription.getType() == SubType.Key_Shared) {
                subscription.getDispatcher().markDeletePositionMoveForward();
            }
            expirationCheckInProgress = FALSE;
            log.debug()
                    .attr("numMessagesExpired", numMessagesExpired)
                    .log("Mark deleted messages");
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn()
                    .exception(exception)
                    .log("Message expiry failed - mark delete failed");
            expirationCheckInProgress = FALSE;
            updateRates();
        }
    };

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        if (position != null) {
            var markDeletedPosition = cursor.getMarkDeletedPosition();
            if (markDeletedPosition != null && markDeletedPosition.compareTo(position) >= 0) {
                expirationCheckInProgress = FALSE;
                return;
            }
            log.info()
                    .attr("position", position)
                    .log("Expiring all messages until position");
            Position prevMarkDeletePos = cursor.getMarkDeletedPosition();
            cursor.asyncMarkDelete(position, cursor.getProperties(), markDeleteCallback,
                    cursor.getNumberOfEntriesInBacklog(false));
            if (!Objects.equals(cursor.getMarkDeletedPosition(), prevMarkDeletePos) && subscription != null) {
                subscription.updateLastMarkDeleteAdvancedTimestamp();
            }
        } else {
            log.debug("No messages to expire");
            expirationCheckInProgress = FALSE;
            updateRates();
        }
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        log.debug()
                .exception(exception)
                .log("Finding expired entry operation failed");
        if (isAutoSkipNonRecoverableData() && failedReadPosition.isPresent()
                && (exception instanceof NonRecoverableLedgerException)) {
            log.warn()
                    .attr("failedReadPosition", failedReadPosition)
                    .exceptionMessage(exception)
                    .log("Read failed from ledger");
            if (exception instanceof LedgerNotExistException) {
                long failedLedgerId = failedReadPosition.get().getLedgerId();
                ManagedLedger ledger = cursor.getManagedLedger();
                Position lastPositionInLedger = ledger.getOptionalLedgerInfo(failedLedgerId)
                        .map(ledgerInfo -> PositionFactory.create(failedLedgerId, ledgerInfo.getEntries() - 1))
                        .orElseGet(() -> {
                            Long nextExistingLedger =
                                ledger.getLedgersInfo().ceilingKey(failedReadPosition.get().getLedgerId() + 1);
                            if (nextExistingLedger == null) {
                                log.info()
                                        .attr("failedReadPosition", failedReadPosition)
                                        .log("Couldn't find next valid ledger for expiry monitor "
                                                + "when find entry failed");
                                return failedReadPosition.get();
                            } else {
                                return PositionFactory.create(nextExistingLedger, -1);
                            }
                        });
                log.info()
                        .attr("lastPositionInLedger", lastPositionInLedger)
                        .log("Ledger does not exist, will complete the last position of the non-existent ledger");
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
