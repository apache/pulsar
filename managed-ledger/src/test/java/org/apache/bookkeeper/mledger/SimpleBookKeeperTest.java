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
package org.apache.bookkeeper.mledger;

import com.google.common.base.Charsets;
import java.nio.charset.Charset;
import java.util.Enumeration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class SimpleBookKeeperTest extends MockedBookKeeperTestCase {

    private static final String SECRET = "secret";
    private static final Charset Encoding = Charsets.UTF_8;

    @Test
    public void simpleTest() throws Exception {

        LedgerHandle ledger = bkc.createLedger(DigestType.MAC, SECRET.getBytes());
        long ledgerId = ledger.getId();
        log.info("Writing to ledger: {}", ledgerId);

        for (int i = 0; i < 10; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes(Encoding));
        }

        ledger.close();

        ledger = bkc.openLedger(ledgerId, DigestType.MAC, SECRET.getBytes());

        Enumeration<LedgerEntry> entries = ledger.readEntries(0, 9);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String content = new String(entry.getEntry(), Encoding);
            log.info("Entry {}  lenght={} content='{}'", entry.getEntryId(), entry.getLength(), content);
        }

        ledger.close();
    }

    private static Logger log = LoggerFactory.getLogger(SimpleBookKeeperTest.class);

}
