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

import org.testng.annotations.Test;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class DataBlockUtilsTest {

    @Test
    public void parseLedgerIdTest() throws Exception {
       UUID id = UUID.randomUUID();
       long ledgerId = 123124;
        String key = DataBlockUtils.dataBlockOffloadKey(ledgerId, id);
        String keyIndex = DataBlockUtils.indexBlockOffloadKey(ledgerId, id);

        assertEquals(ledgerId, DataBlockUtils.parseLedgerId(key).longValue());
        assertEquals(ledgerId, DataBlockUtils.parseLedgerId(keyIndex).longValue());

        assertNull(DataBlockUtils.parseLedgerId(null));
        assertNull(DataBlockUtils.parseLedgerId(""));
        assertNull(DataBlockUtils.parseLedgerId("-ledger-"));
        assertNull(DataBlockUtils.parseLedgerId("something"));
        assertNull(DataBlockUtils.parseLedgerId("-ledger-index"));
    }

    @Test
    public void parseContextUuidTest() throws Exception {
        UUID id = UUID.randomUUID();
        long ledgerId = 123124;
        String key = DataBlockUtils.dataBlockOffloadKey(ledgerId, id);
        String keyIndex = DataBlockUtils.indexBlockOffloadKey(ledgerId, id);

        assertEquals(ledgerId, DataBlockUtils.parseLedgerId(key).longValue());
        assertEquals(ledgerId, DataBlockUtils.parseLedgerId(keyIndex).longValue());
        assertEquals(id.toString(), DataBlockUtils.parseContextUuid(key, ledgerId));
        assertEquals(id.toString(), DataBlockUtils.parseContextUuid(keyIndex, ledgerId));

        assertNull(DataBlockUtils.parseContextUuid(null, null));
        assertNull(DataBlockUtils.parseContextUuid(null, ledgerId));
        assertNull(DataBlockUtils.parseContextUuid("foo", null));
        assertNull(DataBlockUtils.parseContextUuid("-ledger-" + ledgerId, ledgerId));
        assertNull(DataBlockUtils.parseContextUuid("something" + ledgerId, ledgerId));
    }

}
