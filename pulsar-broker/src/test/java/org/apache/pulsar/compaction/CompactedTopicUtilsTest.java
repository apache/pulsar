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

import static org.apache.pulsar.compaction.CompactedTopicUtils.calculateTheLastBatchIndexInBatch;
import static org.apache.pulsar.compaction.Compactor.RETAINED_MESSAGE_COUNT_PROPERTY;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompactedTopicUtilsTest {

    @Test
    public void testReadCompactedEntriesWithEmptyEntries() throws ExecutionException, InterruptedException {
        Position lastCompactedPosition = PositionFactory.create(1, 100);
        TopicCompactionService service = Mockito.mock(TopicCompactionService.class);
        Mockito.doReturn(CompletableFuture.completedFuture(Collections.emptyList()))
                .when(service).readCompactedEntries(Mockito.any(), Mockito.intThat(argument -> argument > 0));
        Mockito.doReturn(CompletableFuture.completedFuture(lastCompactedPosition)).when(service)
                .getLastCompactedPosition();


        Position initPosition = PositionFactory.create(1, 90);
        AtomicReference<Position> readPositionRef = new AtomicReference<>(initPosition.getNext());
        ManagedCursorImpl cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(readPositionRef.get()).when(cursor).getReadPosition();
        Mockito.doReturn(1).when(cursor).applyMaxSizeCap(Mockito.anyInt(), Mockito.anyLong());
        Mockito.doAnswer(invocation -> {
            readPositionRef.set(invocation.getArgument(0));
            return null;
        }).when(cursor).seek(Mockito.any());

        CompletableFuture<List<Entry>> completableFuture = new CompletableFuture<>();
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        AsyncCallbacks.ReadEntriesCallback readEntriesCallback = new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                completableFuture.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
                throwableRef.set(exception);
            }
        };

        CompactedTopicUtils.asyncReadCompactedEntries(service, cursor, 1, 100,
                PositionFactory.LATEST, false, readEntriesCallback, false, null);

        List<Entry> entries = completableFuture.get();
        Assert.assertTrue(entries.isEmpty());
        Assert.assertNull(throwableRef.get());
        Assert.assertEquals(readPositionRef.get(), lastCompactedPosition.getNext());
    }

    @Test
    public void testCalculateTheLastBatchIndex() throws IOException {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("producer");
        metadata.setSequenceId(1L);
        metadata.setPublishTime(1L);

        MessageMetadata parsedMetadata = setRetainedMessageCount(metadata, null);
        Assert.assertEquals(calculateTheLastBatchIndexInBatch(parsedMetadata, Unpooled.buffer()), -1);

        parsedMetadata = setRetainedMessageCount(metadata, "abc");
        Assert.assertEquals(calculateTheLastBatchIndexInBatch(parsedMetadata, Unpooled.buffer()), -1);

        parsedMetadata = setRetainedMessageCount(metadata, "10");
        Assert.assertEquals(calculateTheLastBatchIndexInBatch(parsedMetadata, Unpooled.buffer()), 10);

        parsedMetadata = setRetainedMessageCount(metadata, "0");
        Assert.assertEquals(calculateTheLastBatchIndexInBatch(parsedMetadata, Unpooled.buffer()), -1);
    }

    private MessageMetadata setRetainedMessageCount(MessageMetadata metadata, String count) {
        if (count != null) {
            metadata.clearProperties();
            metadata.addProperty().setKey(RETAINED_MESSAGE_COUNT_PROPERTY).setValue(count);
        } else {
            metadata.getPropertiesList().clear();
        }
        byte[] bytes = metadata.toByteArray();
        MessageMetadata parsedMetadata = new MessageMetadata();
        parsedMetadata.parseFrom(bytes);
        return parsedMetadata;
    }

}
