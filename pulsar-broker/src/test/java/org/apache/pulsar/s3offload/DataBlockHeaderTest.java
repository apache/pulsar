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

import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
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
        assertTrue(dataBlockHeader.getBlockMagicWord() == 0xDBDBDBDB);
        assertTrue(dataBlockHeader.getBlockLength() == headerLength);
        assertTrue(dataBlockHeader.getFirstEntryId() == firstEntryId);

        // verify toStream and fromStream
        InputStream stream = dataBlockHeader.toStream();
        DataBlockHeaderImpl rebuild = DataBlockHeaderImpl.fromStream(stream);

        assertTrue(rebuild.getBlockMagicWord() == 0xDBDBDBDB);
        assertTrue(rebuild.getBlockLength() == headerLength);
        assertTrue(rebuild.getFirstEntryId() == firstEntryId);
    }

}
