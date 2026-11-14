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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CheckpointV5Test {

    private static MessageId v4(long ledger, long entry) {
        return new MessageIdImpl(ledger, entry, 0);
    }

    // --- Regular checkpoint roundtrip ---

    @Test
    public void testRegularCheckpointRoundtrip() throws Exception {
        Map<Long, MessageId> positions = Map.of(
                0L, v4(10, 20),
                1L, v4(30, 40),
                2L, v4(50, 60));
        CheckpointV5 original = new CheckpointV5(positions);

        byte[] bytes = original.toByteArray();
        Checkpoint decoded = CheckpointV5.fromByteArray(bytes);

        assertTrue(decoded instanceof CheckpointV5,
                "expected CheckpointV5, got " + decoded.getClass());
        CheckpointV5 decodedV5 = (CheckpointV5) decoded;
        assertEquals(decodedV5.segmentPositions().size(), 3);
        assertEquals(decodedV5.segmentPositions().get(0L), v4(10, 20));
        assertEquals(decodedV5.segmentPositions().get(1L), v4(30, 40));
        assertEquals(decodedV5.segmentPositions().get(2L), v4(50, 60));
    }

    @Test
    public void testRegularCheckpointWithEmptyPositions() throws Exception {
        CheckpointV5 original = new CheckpointV5(Map.of());

        CheckpointV5 decoded = (CheckpointV5) CheckpointV5.fromByteArray(original.toByteArray());
        assertEquals(decoded.segmentPositions().size(), 0);
    }

    @Test
    public void testSegmentPositionsIsImmutable() {
        CheckpointV5 cp = new CheckpointV5(Map.of(0L, v4(1, 2)));
        assertThrows(UnsupportedOperationException.class,
                () -> cp.segmentPositions().put(1L, v4(5, 6)));
    }

    // --- EARLIEST / LATEST sentinels ---

    @Test
    public void testEarliestSentinelRoundtrip() throws Exception {
        byte[] bytes = CheckpointV5.EARLIEST.toByteArray();
        Checkpoint decoded = CheckpointV5.fromByteArray(bytes);
        assertSame(decoded, CheckpointV5.EARLIEST,
                "EARLIEST should deserialize back to the same singleton");
    }

    @Test
    public void testLatestSentinelRoundtrip() throws Exception {
        byte[] bytes = CheckpointV5.LATEST.toByteArray();
        Checkpoint decoded = CheckpointV5.fromByteArray(bytes);
        assertSame(decoded, CheckpointV5.LATEST);
    }

    @Test
    public void testSentinelsHaveCompactEncoding() {
        // Sentinels are a single type byte — keep the wire as tight as possible.
        assertEquals(CheckpointV5.EARLIEST.toByteArray().length, 1);
        assertEquals(CheckpointV5.LATEST.toByteArray().length, 1);
    }

    // --- Error handling ---

    @Test
    public void testFromByteArrayRejectsNull() {
        assertThrows(java.io.IOException.class, () -> CheckpointV5.fromByteArray(null));
    }

    @Test
    public void testFromByteArrayRejectsEmpty() {
        assertThrows(java.io.IOException.class, () -> CheckpointV5.fromByteArray(new byte[0]));
    }

    @Test
    public void testFromByteArrayRejectsUnknownType() {
        assertThrows(java.io.IOException.class,
                () -> CheckpointV5.fromByteArray(new byte[]{127, 0, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    public void testFromByteArrayRejectsTruncatedData() {
        byte[] bytes = new CheckpointV5(Map.of(1L, v4(10, 20))).toByteArray();
        for (int length = 1; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThrows(IOException.class, () -> CheckpointV5.fromByteArray(truncated));
        }
    }

    @DataProvider
    public Object[][] badEntryCountAndLength() {
        return new Object[][]{{-1, 0}, {1, -1}, {1, Integer.MAX_VALUE}};
    }

    @Test(dataProvider = "badEntryCountAndLength")
    public void testFromByteArrayRejectsBadLengths(int entryCount, int messageIdLength) {
        ByteBuffer buf = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES);
        buf.put((byte) 0).putInt(entryCount).putLong(1L).putInt(messageIdLength);
        assertThrows(IOException.class, () -> CheckpointV5.fromByteArray(buf.array()));
    }

    @Test
    public void testFromByteArrayRejectsNegativeBatchSizeInMessageId() {
        // The v4 encoder itself never writes a negative batch size
        MessageIdData idData = new MessageIdData().setLedgerId(1).setEntryId(2).setBatchIndex(0).setBatchSize(-1);
        ByteBuf idBuf = Unpooled.buffer(idData.getSerializedSize());
        idData.writeTo(idBuf);
        byte[] idBytes = ByteBufUtil.getBytes(idBuf);
        ByteBuffer buf = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES + idBytes.length);
        buf.put((byte) 0).putInt(1).putLong(1L).putInt(idBytes.length).put(idBytes);
        assertThrows(IOException.class, () -> CheckpointV5.fromByteArray(buf.array()));
    }
}
