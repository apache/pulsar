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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class ManagedLedgerBinaryFormatConversion extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    @DataProvider(name = "gracefulClose")
    public static Object[][] protobufFormat() {
        return new Object[][] { { false }, { true } };
    }

    @Test(timeOut = 20000, dataProvider = "gracefulClose")
    void textToBinary(boolean gracefulClose) throws Exception {
        ManagedLedgerFactoryConfig textConf = new ManagedLedgerFactoryConfig();
        textConf.setUseProtobufBinaryFormatInZK(false);
        ManagedLedgerFactory textFactory = new ManagedLedgerFactoryImpl(bkc, zkc, textConf);
        ManagedLedger ledger = textFactory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("test-0".getBytes(Encoding));
        Position p1 = ledger.addEntry("test-1".getBytes(Encoding));
        ledger.addEntry("test-2".getBytes(Encoding));

        c1.delete(p1);

        if (gracefulClose) {
            ledger.close();
        }

        // Reopen with binary format
        ManagedLedgerFactoryConfig binaryConf = new ManagedLedgerFactoryConfig();
        binaryConf.setUseProtobufBinaryFormatInZK(true);
        ManagedLedgerFactory binaryFactory = new ManagedLedgerFactoryImpl(bkc, zkc, binaryConf);
        ledger = binaryFactory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // The 'p1' entry was already deleted
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);

        textFactory.shutdown();
        binaryFactory.shutdown();
    }

    @Test(timeOut = 20000, dataProvider = "gracefulClose")
    void binaryToText(boolean gracefulClose) throws Exception {
        ManagedLedgerFactoryConfig binaryConf = new ManagedLedgerFactoryConfig();
        binaryConf.setUseProtobufBinaryFormatInZK(true);
        ManagedLedgerFactory binaryFactory = new ManagedLedgerFactoryImpl(bkc, zkc, binaryConf);
        ManagedLedger ledger = binaryFactory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("test-0".getBytes(Encoding));
        Position p1 = ledger.addEntry("test-1".getBytes(Encoding));
        ledger.addEntry("test-2".getBytes(Encoding));

        c1.delete(p1);

        if (gracefulClose) {
            ledger.close();
        }

        // Reopen with binary format

        ManagedLedgerFactoryConfig textConf = new ManagedLedgerFactoryConfig();
        textConf.setUseProtobufBinaryFormatInZK(false);
        ManagedLedgerFactory textFactory = new ManagedLedgerFactoryImpl(bkc, zkc, textConf);
        ledger = textFactory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        // The 'p1' entry was already deleted
        assertEquals(c1.getNumberOfEntriesInBacklog(), 2);

        textFactory.shutdown();
        binaryFactory.shutdown();
    }
}
