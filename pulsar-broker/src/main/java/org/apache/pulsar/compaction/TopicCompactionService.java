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
package org.apache.pulsar.compaction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TopicCompactionService extends AutoCloseable {
    /**
     * Compact the topic.
     * Topic Compaction is a key-based retention mechanism. It keeps the most recent value for a given key and
     * user reads compacted data from TopicCompactionService.
     *
     * @return a future that will be completed when the compaction is done.
     */
    CompletableFuture<Void> compact();

    /**
     * Read the compacted entries from the TopicCompactionService.
     *
     * @param startPosition         the position to start reading from.
     * @param numberOfEntriesToRead the maximum number of entries to read.
     * @return a future that will be completed with the list of entries, this list can be null.
     */
    CompletableFuture<List<Entry>> readCompactedEntries(@Nonnull Position startPosition, int numberOfEntriesToRead);

    /**
     * Read the last compacted entry from the TopicCompactionService.
     *
     * @return a future that will be completed with the compacted last entry, this entry can be null.
     */
    CompletableFuture<Entry> readLastCompactedEntry();

    /**
     * Get the last compacted position from the TopicCompactionService.
     *
     * @return a future that will be completed with the last compacted position, this position can be null.
     */
    CompletableFuture<Position> getLastCompactedPosition();
}
