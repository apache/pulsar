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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.offload.jcloud.DataBlockHeader;
import org.testng.annotations.Test;

@Slf4j
public class DataBlockHeaderV2Test {

    @Test
    public void dataBlockHeaderImplTest() throws Exception {
        int blockLength = 1024 * 1024;
        long firstEntryId = 3333L;
        long ledgerId = 3L;

        StreamingDataBlockHeaderImpl dataBlockHeader = StreamingDataBlockHeaderImpl.of(blockLength,
                ledgerId, firstEntryId);

        // verify get methods
        assertEquals(StreamingDataBlockHeaderImpl.getBlockMagicWord(), StreamingDataBlockHeaderImpl.MAGIC_WORD);
        assertEquals(dataBlockHeader.getBlockLength(), blockLength);
        assertEquals(dataBlockHeader.getFirstEntryId(), firstEntryId);
        assertEquals(dataBlockHeader.getLedgerId(), ledgerId);

        // verify toStream and fromStream
        InputStream stream = dataBlockHeader.toStream();
        stream.mark(0);
        StreamingDataBlockHeaderImpl rebuild = StreamingDataBlockHeaderImpl.fromStream(stream);
        assertEquals(rebuild.getBlockLength(), blockLength);
        assertEquals(rebuild.getFirstEntryId(), firstEntryId);
        assertEquals(rebuild.getLedgerId(), ledgerId);
        // verify InputStream reach end
        assertEquals(stream.read(), -1);

        stream.reset();
        byte[] streamContent = new byte[StreamingDataBlockHeaderImpl.getDataStartOffset()];

        // stream with all 0, simulate junk data, should throw exception for header magic not match.
        try (InputStream stream2 = new ByteArrayInputStream(streamContent, 0,
                StreamingDataBlockHeaderImpl.getDataStartOffset())) {
            DataBlockHeader rebuild2 = StreamingDataBlockHeaderImpl.fromStream(stream2);
            fail("Should throw IOException");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertTrue(e.getMessage().contains("Data block header magic word not match"));
        }

        // simulate read header too small, throw EOFException.
        stream.read(streamContent);
        try (InputStream stream3 =
                     new ByteArrayInputStream(streamContent, 0,
                             StreamingDataBlockHeaderImpl.getDataStartOffset() - 1)) {
            DataBlockHeader rebuild3 = StreamingDataBlockHeaderImpl.fromStream(stream3);
            fail("Should throw EOFException");
        } catch (EOFException e) {
            // expected
        }

        stream.close();
    }

}
