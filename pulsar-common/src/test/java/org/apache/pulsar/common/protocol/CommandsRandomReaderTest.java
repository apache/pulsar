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
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.RandomReadInvisibleReason;
import org.testng.annotations.Test;

public class CommandsRandomReaderTest {

    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4);
            int commandSize = (int) frame.readUnsignedInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            command.materialize();
            return command;
        } finally {
            frame.release();
        }
    }

    @Test
    public void testRandomReadEntryResultCommandRoundTrip() {
        BaseCommand command = parseFrame(Commands.newRandomReadEntryResult(
                7L, 11L, 3L, 4L, 2, RandomReadInvisibleReason.DELAYED_DELIVERY));

        assertEquals(command.getType(), BaseCommand.Type.RANDOM_READ_ENTRY_RESULT);
        assertTrue(command.hasRandomReadEntryResult());
        assertEquals(command.getRandomReadEntryResult().getRandomReaderId(), 7L);
        assertEquals(command.getRandomReadEntryResult().getRequestId(), 11L);
        assertEquals(command.getRandomReadEntryResult().getMessageId().getLedgerId(), 3L);
        assertEquals(command.getRandomReadEntryResult().getMessageId().getEntryId(), 4L);
        assertEquals(command.getRandomReadEntryResult().getMessageId().getPartition(), 2);
        assertEquals(command.getRandomReadEntryResult().getInvisibleReason(),
                RandomReadInvisibleReason.DELAYED_DELIVERY);
    }
}
