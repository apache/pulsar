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
package org.apache.bookkeeper.mledger;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.readEntries;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * The task to perform replay on the whole managed ledger from a given position.
 */
@RequiredArgsConstructor
@Slf4j
public class ManagedLedgerReplayTask {

    private final String name;
    private final Executor executor; // run user-provided processor on entry
    private final int maxEntriesPerRead;
    @Getter // NOTE: the getter must be called in the callback of `replay` for thread safety
    private int numEntriesProcessed = 0;

    /**
     * This method will read entries from `cursor` until the last confirmed entry. `processor` will be applied on each
     * entry.
     *
     * @param cursor the managed cursor to read entries
     * @param processor the user-provided processor that accepts the position and data buffer of the entry
     * @return the future of the optional last position processed:
     *         1. If there is no more entry to read, return an empty optional.
     *         2. Otherwise, if no exception was thrown, it will always be the position of the last entry.
     *         3. If any exception is thrown from {@link EntryProcessor#process}, it will be the position of the last
     *            entry that has been processed successfully.
     *         4. If an unexpected exception is thrown, the future will complete exceptionally.
     * @apiNote The implementation of `processor` should not call `release()` on the buffer because this method will
     *          eventually release the buffer after it's processed.
     */
    public CompletableFuture<Optional<Position>> replay(ManagedCursor cursor, EntryProcessor processor) {
        try {
            numEntriesProcessed = 0;
            cursor.setAlwaysInactive(); // don't cache the replayed entries
            if (!cursor.hasMoreEntries()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return readAndProcess(cursor, null, processor);
        } catch (Throwable throwable) {
            return CompletableFuture.failedFuture(throwable);
        }
    }

    private CompletableFuture<Optional<Position>> readAndProcess(
            ManagedCursor cursor, @Nullable Position lastProcessedPosition, EntryProcessor processor) {
        return readEntries(cursor, maxEntriesPerRead, PositionFactory.LATEST).thenComposeAsync(entries -> {
            try {
                Position processedPosition = lastProcessedPosition;
                for (final var entry : entries) {
                    final var position = entry.getPosition();
                    final var buffer = entry.getDataBuffer();
                    // Pass a duplicated buffer to `processor` in case the buffer is retained and stored somewhere else
                    // and then process all buffers in batch.
                    try {
                        processor.process(position, buffer);
                    } catch (Throwable throwable) {
                        log.error("[{}] Failed to process entry {}", name, position, throwable);
                        return CompletableFuture.completedFuture(Optional.ofNullable(processedPosition));
                    }
                    // It does not need to be atomic because the update happens before the future completes
                    numEntriesProcessed++;
                    processedPosition = position;
                }
                if (cursor.hasMoreEntries()) {
                    return readAndProcess(cursor, processedPosition, processor);
                } else {
                    return CompletableFuture.completedFuture(Optional.ofNullable(processedPosition));
                }
            } finally {
                entries.forEach(Entry::release);
            }
        }, executor);
    }
}
