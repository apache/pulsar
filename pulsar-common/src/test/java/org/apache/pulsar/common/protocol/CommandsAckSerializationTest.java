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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.util.collections.ConcurrentBitSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CommandsAckSerializationTest {
    @DataProvider
    public Object[][] ackGroups() {
        List<Object[]> cases = new ArrayList<>();
        for (int group = 0; group < 4; group++) {
            for (long requestId : new long[]{-1, 0, Long.MAX_VALUE}) {
                cases.add(new Object[]{group, requestId});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "ackGroups")
    public void existingIdsProduceIdenticalWireCommand(int group, long requestId) {
        List<MessageIdAdv> individual = new ArrayList<>();
        List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> batch = new ArrayList<>();
        if ((group & 1) != 0) {
            individual.add(id(1L << 40, Long.MAX_VALUE));
            individual.add(id(0, 0));
        }
        if ((group & 2) != 0) {
            ConcurrentBitSet mask = new ConcurrentBitSet(192);
            mask.set(0);
            mask.set(65);
            mask.set(191);
            batch.add(new AbstractMap.SimpleImmutableEntry<>(id(1L << 41, 1L << 42), mask));
            batch.add(new AbstractMap.SimpleImmutableEntry<>(id(3, 4), new ConcurrentBitSet()));
            batch.add(new AbstractMap.SimpleImmutableEntry<>(id(5, 6), null));
        }
        List<Triple<Long, Long, ConcurrentBitSet>> legacy = new ArrayList<>();
        for (MessageIdAdv id : individual) {
            legacy.add(Triple.of(id.getLedgerId(), id.getEntryId(), null));
        }
        for (Map.Entry<MessageIdAdv, ConcurrentBitSet> entry : batch) {
            legacy.add(Triple.of(entry.getKey().getLedgerId(), entry.getKey().getEntryId(), entry.getValue()));
        }
        ByteBuf expected = Commands.newMultiMessageAck(123, legacy, requestId);
        ByteBuf actual = Commands.newMultiMessageAck(123, individual, batch, requestId);
        try {
            assertTrue(ByteBufUtil.equals(actual, expected), "The existing wire encoding must be preserved");
            actual.skipBytes(4);
            int size = actual.readInt();
            BaseCommand parsed = new BaseCommand();
            parsed.parseFrom(actual, size);
            assertEquals(parsed.getType(), BaseCommand.Type.ACK);
            CommandAck ack = parsed.getAck();
            assertEquals(ack.getConsumerId(), 123L);
            assertEquals(ack.getAckType(), CommandAck.AckType.Individual);
            assertEquals(ack.hasRequestId(), requestId >= 0);
            if (requestId >= 0) {
                assertEquals(ack.getRequestId(), requestId);
            }
            assertEquals(ack.getMessageIdsCount(), legacy.size());
            for (int i = 0; i < legacy.size(); i++) {
                MessageIdData id = ack.getMessageIdAt(i);
                Triple<Long, Long, ConcurrentBitSet> entry = legacy.get(i);
                assertEquals(id.getLedgerId(), entry.getLeft().longValue());
                assertEquals(id.getEntryId(), entry.getMiddle().longValue());
                long[] words = entry.getRight() == null ? new long[0] : entry.getRight().toLongArray();
                assertEquals(id.getAckSetsCount(), words.length);
                for (int word = 0; word < words.length; word++) {
                    assertEquals(id.getAckSetAt(word), words[word]);
                }
            }
        } finally {
            actual.release();
            expected.release();
        }
    }

    private static MessageIdAdv id(long ledgerId, long entryId) {
        return new TestMessageId(ledgerId, entryId);
    }

    private record TestMessageId(long ledgerId, long entryId) implements MessageIdAdv {
        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException("Only the ID coordinates are used by command serialization");
        }
    }
}
