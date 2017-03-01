/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.util.protobuf;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ByteBufCodedInputStreamTest {

    @Test
    public void testByteBufCondedInputStreamTest() throws IOException {
        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream
                .get(Unpooled.wrappedBuffer("Test-Message".getBytes()));
        assertTrue(inputStream.skipField(WireFormat.WIRETYPE_VARINT));
        assertTrue(inputStream.skipField(WireFormat.WIRETYPE_FIXED64));
        assertFalse(inputStream.skipField(WireFormat.WIRETYPE_END_GROUP));
        inputStream = ByteBufCodedInputStream.get(Unpooled.wrappedBuffer("1000".getBytes()));
        assertTrue(inputStream.skipField(WireFormat.WIRETYPE_FIXED32));
        assertTrue(inputStream.skipField(WireFormat.WIRETYPE_START_GROUP));

        try {
            assertTrue(inputStream.skipField(-1));
            fail("Should not happend");
        } catch (Exception e) {
            // pass
        }
        try {
            assertTrue(inputStream.skipField(WireFormat.WIRETYPE_LENGTH_DELIMITED));
            fail("Should not happend");
        } catch (Exception e) {
            // pass
        }

        try {
            inputStream.skipRawBytes(-1);
            fail("Should not happend");
        } catch (InvalidProtocolBufferException e) {
            // pass
        }

        try {
            inputStream.skipRawBytes(10);
            fail("Should not happend");
        } catch (InvalidProtocolBufferException e) {
            // pass
        }

    }

    @Test
    public void testWritingDouble() throws IOException {
        ByteBuf buf = Unpooled.buffer();
        buf.clear();
        ByteBufCodedOutputStream outputStream = ByteBufCodedOutputStream.get(buf);
        outputStream.writeDouble(12, 23d);
        outputStream.writeDouble(15, 13.13d);
        outputStream.writeDouble(1, -0.003d);

        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(buf);
        assertEquals(WireFormat.getTagFieldNumber(inputStream.readTag()), 12);
        assertEquals(inputStream.readDouble(), 23d);

        assertEquals(WireFormat.getTagFieldNumber(inputStream.readTag()), 15);
        assertEquals(inputStream.readDouble(), 13.13d);

        assertEquals(WireFormat.getTagFieldNumber(inputStream.readTag()), 1);
        assertEquals(inputStream.readDouble(), -0.003d);
    }
}
