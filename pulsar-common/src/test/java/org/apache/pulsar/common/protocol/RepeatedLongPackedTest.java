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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.TestApi.MessageIdData;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.testng.annotations.Test;

public class RepeatedLongPackedTest {

    @Test
    public void testRepeatedLongPacked() throws Exception {
        MessageIdData messageIdData = MessageIdData.newBuilder()
            .setLedgerId(0L)
            .setEntryId(0L)
            .setPartition(0)
            .setBatchIndex(0)
            .addAckSet(1000)
            .addAckSet(1001)
            .addAckSet(1003)
            .build();

        int cmdSize = messageIdData.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(cmdSize);
        ByteBufCodedOutputStream outputStream = ByteBufCodedOutputStream.get(buf);
        messageIdData.writeTo(outputStream);

        messageIdData.recycle();
        outputStream.recycle();

        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(buf);
        MessageIdData newMessageIdData = MessageIdData.newBuilder()
            .mergeFrom(inputStream, null)
            .build();
        inputStream.recycle();

        assertEquals(3, newMessageIdData.getAckSetCount());
        assertEquals(1000, newMessageIdData.getAckSet(0));
        assertEquals(1001, newMessageIdData.getAckSet(1));
        assertEquals(1003, newMessageIdData.getAckSet(2));
        newMessageIdData.recycle();
    }

}
