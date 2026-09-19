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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class GetLastMessageIdEntryLeakTest {

    /**
     * Reproduces the ByteBuf leak in ServerCnx#getLargestBatchIndexWhenPossible: when
     * Commands.parseMessageMetadata throws on the entry read for the last position, the entry (and
     * its backing ByteBuf) must still be released.
     */
    @Test
    public void testEntryReleasedWhenParseMetadataThrows() {
        // A 2-byte, non-magic buffer: parseMessageMetadata reads it but fails at readUnsignedInt
        // (needs 4 bytes), which is exactly the corrupt-entry case that triggers the leak.
        EntryImpl entry = EntryImpl.create(1, 0, new byte[2]);
        ByteBuf buffer = entry.getDataBuffer();
        try {
            assertThatThrownBy(() -> ServerCnx.parseBatchSizeAndReleaseEntry(entry))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThat(buffer.refCnt()).as("entry's ByteBuf after metadata parsing failed").isZero();
        } finally {
            if (buffer.refCnt() > 0) {
                entry.release();
            }
        }
    }

    @DataProvider
    public Object[][] batchMetadata() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "batchMetadata")
    public void testEntryReleasedAfterReadingBatchSize(boolean cachedMetadata, boolean batched) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(0)
                .setPublishTime(0);
        if (batched) {
            metadata.setNumMessagesInBatch(3);
        }
        // An invalid buffer with cached metadata also verifies that parsing the buffer is skipped.
        ByteBuf buffer = cachedMetadata ? Unpooled.wrappedBuffer(new byte[2])
                : Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata, Unpooled.EMPTY_BUFFER);
        EntryImpl entry = EntryImpl.create(1, 0, buffer);
        buffer.release();
        if (cachedMetadata) {
            entry.setMessageMetadata(metadata);
        }
        try {
            assertThat(ServerCnx.parseBatchSizeAndReleaseEntry(entry)).isEqualTo(batched ? 3 : -1);
            assertThat(buffer.refCnt()).as("entry's ByteBuf after reading batch size").isZero();
        } finally {
            if (buffer.refCnt() > 0) {
                entry.release();
            }
        }
    }
}
