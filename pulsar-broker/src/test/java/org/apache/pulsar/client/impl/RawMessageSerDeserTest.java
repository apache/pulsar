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

import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Cleanup;

@Test(groups = "broker-impl")
public class RawMessageSerDeserTest {
    static final Logger log = LoggerFactory.getLogger(RawMessageSerDeserTest.class);

    @Test
    public void testSerializationAndDeserialization() {
        int payload = 0xbeefcafe;
        ByteBuf headersAndPayload = Unpooled.buffer(4);
        headersAndPayload.writeInt(payload);

        MessageIdData id = new MessageIdData()
            .setLedgerId(0xf00)
            .setEntryId(0xbaa)
            .setPartition(10)
            .setBatchIndex(20);

        @Cleanup
        RawMessage m = new RawMessageImpl(id, headersAndPayload);
        ByteBuf serialized = m.serialize();
        byte[] bytes = new byte[serialized.readableBytes()];
        serialized.readBytes(bytes);

        RawMessage m2 = RawMessageImpl.deserializeFrom(Unpooled.wrappedBuffer(bytes));

        Assert.assertEquals(m2.getMessageIdData().getLedgerId(), m.getMessageIdData().getLedgerId());
        Assert.assertEquals(m2.getMessageIdData().getEntryId(), m.getMessageIdData().getEntryId());
        Assert.assertEquals(m2.getMessageIdData().getPartition(), m.getMessageIdData().getPartition());
        Assert.assertEquals(m2.getMessageIdData().getBatchIndex(), m.getMessageIdData().getBatchIndex());
        Assert.assertEquals(m2.getHeadersAndPayload(), m.getHeadersAndPayload());
    }
}
