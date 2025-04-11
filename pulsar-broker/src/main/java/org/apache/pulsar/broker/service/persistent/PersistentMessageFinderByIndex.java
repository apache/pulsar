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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Codec;

/**
 * given a message index and find the first message (position) (published) at or before the index.
 */
@Slf4j
public class PersistentMessageFinderByIndex implements AsyncCallbacks.FindEntryCallback {
    private final ManagedCursor cursor;
    private final String subName;
    private final String topicName;
    private long index = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int messageFindInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentMessageFinderByIndex> messageFindInProgressUpdater =
            AtomicIntegerFieldUpdater.newUpdater(PersistentMessageFinderByIndex.class, "messageFindInProgress");

    public PersistentMessageFinderByIndex(String topicName, ManagedCursor cursor) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = Codec.decode(cursor.getName());
    }

    public void findMessagesByIndex(final long index, AsyncCallbacks.FindEntryCallback callback) {
        this.index = index;
        if (messageFindInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Starting message position find at index {}", subName, index);
            }

            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                try {
                    BrokerEntryMetadata meta = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                    if (meta != null) {
                        return meta.getIndex() < index;
                    }
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for message position find", topicName, subName, e);
                } finally {
                    entry.release();
                }
                return false;
            }, this, callback);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore message position find scheduled task, last find is still running", topicName,
                        subName);
            }
            callback.findEntryFailed(
                    new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                    Optional.empty(), null);
        }
    }

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (position != null) {
            log.info("[{}][{}] Found position {} closest to provided index {}", topicName, subName, position,
                    index);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No position found closest to provided index {}", topicName, subName, index);
            }
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] message position find operation failed for provided index {}", topicName, subName,
                    index, exception);
        }
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, failedReadPosition, null);
    }
}
