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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import java.io.IOException;
import org.testng.annotations.Test;

public class ChunkMessageIdImplTest {

    @Test
    public void compareToTest() {
        ChunkMessageIdImpl chunkMsgId1 = new ChunkMessageIdImpl(
                new MessageIdImpl(0, 0, 0),
                new MessageIdImpl(1, 1, 1)
        );
        ChunkMessageIdImpl chunkMsgId2 = new ChunkMessageIdImpl(
                new MessageIdImpl(2, 2, 2),
                new MessageIdImpl(3, 3, 3)
        );

        assertEquals(chunkMsgId1.compareTo(chunkMsgId2), -1);
        assertEquals(chunkMsgId2.compareTo(chunkMsgId1), 1);
        assertEquals(chunkMsgId2.compareTo(chunkMsgId2), 0);
    }

    @Test
    public void hashCodeTest() {
        ChunkMessageIdImpl chunkMsgId1 = new ChunkMessageIdImpl(
                new MessageIdImpl(0, 0, 0),
                new MessageIdImpl(1, 1, 1)
        );
        ChunkMessageIdImpl chunkMsgId2 = new ChunkMessageIdImpl(
                new MessageIdImpl(2, 2, 2),
                new MessageIdImpl(3, 3, 3)
        );

        assertEquals(chunkMsgId1.hashCode(), chunkMsgId1.hashCode());
        assertNotEquals(chunkMsgId1.hashCode(), chunkMsgId2.hashCode());
    }

    @Test
    public void equalsTest() {
        ChunkMessageIdImpl chunkMsgId1 = new ChunkMessageIdImpl(
                new MessageIdImpl(0, 0, 0),
                new MessageIdImpl(1, 1, 1)
        );
        ChunkMessageIdImpl chunkMsgId2 = new ChunkMessageIdImpl(
                new MessageIdImpl(2, 2, 2),
                new MessageIdImpl(3, 3, 3)
        );

        MessageIdImpl msgId = new MessageIdImpl(1, 1, 1);

        assertEquals(chunkMsgId1, chunkMsgId1);
        assertNotEquals(chunkMsgId2, chunkMsgId1);
        assertEquals(msgId, chunkMsgId1);
    }

    @Test
    public void serializeAndDeserializeTest() throws IOException {
        ChunkMessageIdImpl chunkMessageId = new ChunkMessageIdImpl(
                new MessageIdImpl(0, 0, 0),
                new MessageIdImpl(1, 1, 1)
        );
        byte[] serialized = chunkMessageId.toByteArray();
        ChunkMessageIdImpl deserialized = (ChunkMessageIdImpl) MessageIdImpl.fromByteArray(serialized);
        assertEquals(deserialized, chunkMessageId);
    }

}
