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
package org.apache.pulsar.compaction;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Consumer;

public interface CompactedTopic {
    CompletableFuture<CompactedTopicContext> newCompactedLedger(Position p, long compactedLedgerId);
    CompletableFuture<Void> deleteCompactedLedger(long compactedLedgerId);
    void asyncReadEntriesOrWait(ManagedCursor cursor,
                                int numberOfEntriesToRead,
                                boolean isFirstRead,
                                ReadEntriesCallback callback,
                                Consumer consumer);
    CompletableFuture<Entry> readLastEntryOfCompactedLedger();
    Optional<Position> getCompactionHorizon();
}
