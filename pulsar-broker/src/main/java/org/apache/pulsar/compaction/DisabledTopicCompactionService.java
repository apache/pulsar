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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.jspecify.annotations.NonNull;

public class DisabledTopicCompactionService implements CompactionServiceFactory {

    private static final Entry DUMMY_ENTRY = new Entry() {
        @Override
        public byte[] getData() {
            return new byte[0];
        }

        @Override
        public byte[] getDataAndRelease() {
            return new byte[0];
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public ByteBuf getDataBuffer() {
            return Unpooled.wrappedBuffer(new byte[0]);
        }

        @Override
        public Position getPosition() {
            return PositionFactory.EARLIEST;
        }

        @Override
        public long getLedgerId() {
            return -1;
        }

        @Override
        public long getEntryId() {
            return -1;
        }

        @Override
        public boolean release() {
            return false;
        }
    };
    private static final TopicCompactionService DUMMY_TOPIC_COMPACTION_SERVICE = new TopicCompactionService() {

        @Override
        public void close() {
        }

        @Override
        public CompletableFuture<Void> compact() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<Entry>> readCompactedEntries(@NonNull Position startPosition,
                                                                   int numberOfEntriesToRead) {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Entry> readLastCompactedEntry() {
            return CompletableFuture.completedFuture(DUMMY_ENTRY);
        }

        @Override
        public CompletableFuture<Position> getLastCompactedPosition() {
            // Return a null position so that when handling the GetLastMessageId request, no `asyncReadEntry` call will
            // be performed. Instead, the mark delete position will be returned.
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Entry> findEntryByPublishTime(long publishTime) {
            return CompletableFuture.completedFuture(DUMMY_ENTRY);
        }

        @Override
        public CompletableFuture<Entry> findEntryByEntryIndex(long entryIndex) {
            return CompletableFuture.completedFuture(DUMMY_ENTRY);
        }
    };

    @Override
    public CompletableFuture<Void> initialize(@NonNull PulsarService pulsarService) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicCompactionService> newTopicCompactionService(@NonNull String topic) {
        return CompletableFuture.completedFuture(DUMMY_TOPIC_COMPACTION_SERVICE);
    }

    @Override
    public void close() throws Exception {
    }
}
