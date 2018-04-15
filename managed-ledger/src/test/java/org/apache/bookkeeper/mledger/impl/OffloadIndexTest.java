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
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.mledger.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.OffloadIndexEntry;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.testng.annotations.Test;

@Slf4j
public class OffloadIndexTest {

    @Test
    public void OffloadIndexEntryImplTest() {
        // verify OffloadIndexEntryImpl builder
        OffloadIndexEntryImpl entry1 = OffloadIndexEntryImpl.builder().entryId(0).partId(2).offset(0).build();
        OffloadIndexEntryImpl entry2 = OffloadIndexEntryImpl.builder().entryId(100).partId(3).offset(1234).build();

        // verify OffloadIndexEntryImpl get
        assertTrue(entry1.getEntryId() == 0L);
        assertTrue(entry1.getPartId() == 2);
        assertTrue(entry1.getOffset() == 0L);

        assertTrue(entry2.getEntryId() == 100L);
        assertTrue(entry2.getPartId() == 3);
        assertTrue(entry2.getOffset() == 1234L);

        // verify compareTo
        assertTrue(entry1.compareTo(entry1) == 0);
        assertTrue(entry1.compareTo(entry2) < 0);
        assertTrue(entry2.compareTo(entry1) > 0);

        // verify OffloadIndexEntryImpl set
        entry1.setEntryId(2000L);
        entry1.setPartId(3000);
        entry1.setOffset(4000L);
        assertTrue(entry1.getEntryId() == 2000L);
        assertTrue(entry1.getPartId() == 3000);
        assertTrue(entry1.getOffset() == 4000L);
    }


    // use mock to setLastEntryId
    class LedgerMetadataMock extends org.apache.bookkeeper.client.LedgerMetadata {
        long lastId = 0;
        public LedgerMetadataMock(int ensembleSize, int writeQuorumSize, int ackQuorumSize, org.apache.bookkeeper.client.BookKeeper.DigestType digestType, byte[] password, Map<String, byte[]> customMetadata, boolean storeSystemtimeAsLedgerCreationTime) {
            super(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, password, customMetadata, storeSystemtimeAsLedgerCreationTime);
        }

        @Override
        public long getLastEntryId(){
            return  lastId;
        }

        public void setLastEntryId(long lastId) {
            this.lastId = lastId;
        }
    }

    private LedgerMetadata createLedgerMetadata() throws Exception {

        Map<String, byte[]> metadataCustom = Maps.newHashMap();
        metadataCustom.put("key1", "value1".getBytes(UTF_8));
        metadataCustom.put("key7", "value7".getBytes(UTF_8));

        ArrayList<BookieSocketAddress> bookies = Lists.newArrayList();
        BookieSocketAddress BOOKIE1 = new BookieSocketAddress("127.0.0.1:3181");
        BookieSocketAddress BOOKIE2 = new BookieSocketAddress("127.0.0.2:3181");
        BookieSocketAddress BOOKIE3 = new BookieSocketAddress("127.0.0.3:3181");
        bookies.add(0, BOOKIE1);
        bookies.add(1, BOOKIE2);
        bookies.add(2, BOOKIE3);

        LedgerMetadataMock metadata = new LedgerMetadataMock(3, 3, 2,
            DigestType.CRC32C, "password".getBytes(UTF_8), metadataCustom, false);

        metadata.addEnsemble(0, bookies);
        metadata.setLastEntryId(5000);
        return metadata;
    }

    // prepare metadata, then use builder to build a OffloadIndexBlockImpl
    // verify get methods and readout methods.
    @Test
    public void OffloadIndexBlockImplTest() throws Exception {
        OffloadIndexBlockBuilder blockBuilder = OffloadIndexBlockBuilder.create();
        LedgerMetadata metadata = createLedgerMetadata();
        blockBuilder.withMetadata(metadata);

        blockBuilder.addBlock(0, 2, 0);
        blockBuilder.addBlock(1000, 3, 64 * 1024 * 1024);
        blockBuilder.addBlock(2000, 4, 64 * 1024 * 1024);
        OffloadIndexBlock indexBlock = blockBuilder.build();

        // verify getEntryCount and getLedgerMetadata
        assertTrue(indexBlock.getEntryCount() == 3);
        assertTrue(indexBlock.getLedgerMetadata() == metadata);

        // verify getEntry
        OffloadIndexEntry entry1 = indexBlock.getEntry(0);
        assertTrue(entry1.getEntryId() == 0 && entry1.getPartId() == 2 && entry1.getOffset() == 0);

        OffloadIndexEntry entry11 = indexBlock.getEntry(500);
        assertTrue(entry1.equals(entry11));

        OffloadIndexEntry entry2 = indexBlock.getEntry(1000);
        assertTrue(entry2.getEntryId() == 1000 &&
            entry2.getPartId() == 3 &&
            entry2.getOffset() == 64 * 1024 * 1024);

        OffloadIndexEntry entry22 = indexBlock.getEntry(1300);
        assertTrue(entry2.equals(entry22));

        OffloadIndexEntry entry3 = indexBlock.getEntry(2000);

        assertTrue(entry3.getEntryId() == 2000 &&
            entry3.getPartId() == 4 &&
            entry3.getOffset() == 2 * 64 * 1024 * 1024);

        OffloadIndexEntry entry33 = indexBlock.getEntry(3000);
        assertTrue(entry3.equals(entry33));

        OffloadIndexEntry entry4 = indexBlock.getEntry(6000);
        assertNull(entry4);

        // verify readOut
        ByteBuf out = indexBlock.readOut();
        int magic = out.readInt();
        int indexBlockLength = out.readInt();
        int segmentMetadataLength = out.readInt();
        int indexEntryCount = out.readInt();

        // verify counter
        assertTrue(magic == 1000);
        assertTrue(indexBlockLength == out.writerIndex());
        assertTrue(indexEntryCount == 3);

        out.readBytes(segmentMetadataLength);

        // verify entry
        OffloadIndexEntry e1 = OffloadIndexEntryImpl.builder()
            .entryId(out.readLong()).partId(out.readInt()).offset(out.readLong()).build();
        OffloadIndexEntry e2 = OffloadIndexEntryImpl.builder()
            .entryId(out.readLong()).partId(out.readInt()).offset(out.readLong()).build();
        OffloadIndexEntry e3 = OffloadIndexEntryImpl.builder()
            .entryId(out.readLong()).partId(out.readInt()).offset(out.readLong()).build();

        assertTrue(e1.getEntryId() == entry1.getEntryId() &&
            e1.getPartId() == entry1.getPartId() && e1.getOffset() == entry1.getOffset());
        assertTrue(e2.getEntryId() == entry2.getEntryId() &&
            e2.getPartId() == entry2.getPartId() && e2.getOffset() == entry2.getOffset());
        assertTrue(e3.getEntryId() == entry3.getEntryId() &&
            e3.getPartId() == entry3.getPartId() && e3.getOffset() == entry3.getOffset());

        out.release();
        indexBlock.close();
    }

}
