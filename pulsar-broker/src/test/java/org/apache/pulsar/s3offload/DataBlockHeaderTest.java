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
package org.apache.pulsar.s3offload;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.s3offload.DataBlockHeader;
import org.apache.pulsar.broker.s3offload.impl.DataBlockHeaderImpl;
import org.testng.annotations.Test;

@Slf4j
public class DataBlockHeaderTest {

    @Test
    public void dataBlockHeaderImplTest() throws Exception {
        int headerLength = 1024 * 1024;
        long firstEntryId = 3333L;

        DataBlockHeaderImpl dataBlockHeader = DataBlockHeaderImpl.of(headerLength,
            firstEntryId);

        // verify get methods
        assertEquals(dataBlockHeader.getBlockMagicWord(), 0xDBDBDBDB);
        assertEquals(dataBlockHeader.getBlockLength(), headerLength);
        assertEquals(dataBlockHeader.getFirstEntryId(), firstEntryId);

        // verify toStream and fromStream
        InputStream stream = dataBlockHeader.toStream();
        stream.mark(0);
        DataBlockHeader rebuild = DataBlockHeaderImpl.fromStream(stream);
        assertEquals(rebuild.getBlockLength(), headerLength);
        assertEquals(rebuild.getFirstEntryId(), firstEntryId);
        // verify InputStream reach end
        assertEquals(stream.read(), -1);

        stream.reset();
        byte streamContent[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];

        // stream with all 0, simulate junk data, should throw exception for header magic not match.
        try(InputStream stream2 = new ByteArrayInputStream(streamContent, 0, DataBlockHeaderImpl.getDataStartOffset())) {
            DataBlockHeader rebuild2 = DataBlockHeaderImpl.fromStream(stream2);
            fail("Should throw IOException");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertTrue(e.getMessage().contains("Data block header magic word not match"));
        }

        // simulate read header too small, throw EOFException.
        stream.read(streamContent);
        try(InputStream stream3 =
                new ByteArrayInputStream(streamContent, 0, DataBlockHeaderImpl.getDataStartOffset() - 1)) {
            DataBlockHeader rebuild3 = DataBlockHeaderImpl.fromStream(stream3);
            fail("Should throw EOFException");
        } catch (Exception e) {
            assertTrue(e instanceof java.io.EOFException);
        }

        stream.close();
    }

}
