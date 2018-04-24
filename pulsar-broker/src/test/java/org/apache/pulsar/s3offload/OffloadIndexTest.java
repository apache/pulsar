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

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.pulsar.broker.s3offload.OffloadIndexBlock;
import org.apache.pulsar.broker.s3offload.OffloadIndexBlockBuilder;
import org.apache.pulsar.broker.s3offload.OffloadIndexEntry;
import org.apache.pulsar.broker.s3offload.impl.OffloadIndexEntryImpl;
import org.testng.annotations.Test;

@Slf4j
public class OffloadIndexTest {

    @Test
    public void offloadIndexEntryImplTest() {
        // verify OffloadIndexEntryImpl builder
        OffloadIndexEntryImpl entry1 = OffloadIndexEntryImpl.of(0, 2, 0);
        OffloadIndexEntryImpl entry2 = OffloadIndexEntryImpl.of(100, 3, 1234);

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
    // verify get methods, readout and fromStream methods.
    @Test
    public void offloadIndexBlockImplTest() throws Exception {
        OffloadIndexBlockBuilder blockBuilder = OffloadIndexBlockBuilder.create();
        LedgerMetadata metadata = createLedgerMetadata();
        log.debug("created metadata: {}", metadata.toString());

        blockBuilder.withMetadata(metadata);

        blockBuilder.addBlock(0, 2, 0);
        blockBuilder.addBlock(1000, 3, 64 * 1024 * 1024);
        blockBuilder.addBlock(2000, 4, 64 * 1024 * 1024);
        OffloadIndexBlock indexBlock = blockBuilder.build();

        // verify getEntryCount and getLedgerMetadata
        assertTrue(indexBlock.getEntryCount() == 3);
        assertTrue(indexBlock.getLedgerMetadata() == metadata);

        // verify getIndexEntryForEntry
        OffloadIndexEntry entry1 = indexBlock.getIndexEntryForEntry(0);
        assertTrue(entry1.getEntryId() == 0 && entry1.getPartId() == 2 && entry1.getOffset() == 0);

        OffloadIndexEntry entry11 = indexBlock.getIndexEntryForEntry(500);
        assertTrue(entry1.equals(entry11));

        OffloadIndexEntry entry2 = indexBlock.getIndexEntryForEntry(1000);
        assertTrue(entry2.getEntryId() == 1000 &&
            entry2.getPartId() == 3 &&
            entry2.getOffset() == 64 * 1024 * 1024);

        OffloadIndexEntry entry22 = indexBlock.getIndexEntryForEntry(1300);
        assertTrue(entry2.equals(entry22));

        OffloadIndexEntry entry3 = indexBlock.getIndexEntryForEntry(2000);

        assertTrue(entry3.getEntryId() == 2000 &&
            entry3.getPartId() == 4 &&
            entry3.getOffset() == 2 * 64 * 1024 * 1024);

        OffloadIndexEntry entry33 = indexBlock.getIndexEntryForEntry(3000);
        assertTrue(entry3.equals(entry33));

        OffloadIndexEntry entry4 = indexBlock.getIndexEntryForEntry(6000);
        assertNull(entry4);

        // verify toStream
        InputStream out = indexBlock.toStream();
        byte b[] = new byte[1024];
        int readoutLen = out.read(b);
        out.close();
        ByteBuf wrapper = Unpooled.wrappedBuffer(b);
        int magic = wrapper.readInt();
        int indexBlockLength = wrapper.readInt();
        int segmentMetadataLength = wrapper.readInt();
        int indexEntryCount = wrapper.readInt();

        // verify counter
        assertTrue(magic == indexBlock.getIndexMagicWord());
        assertTrue(indexBlockLength == readoutLen);
        assertTrue(indexEntryCount == 3);

        wrapper.readBytes(segmentMetadataLength);
        log.debug("magic: {}, blockLength: {}, metadataLength: {}, indexCount: {}",
            magic, indexBlockLength, segmentMetadataLength, indexEntryCount);

        // verify entry
        OffloadIndexEntry e1 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(), wrapper.readLong());
        OffloadIndexEntry e2 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(), wrapper.readLong());
        OffloadIndexEntry e3 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(), wrapper.readLong());;

        assertTrue(e1.getEntryId() == entry1.getEntryId() &&
            e1.getPartId() == entry1.getPartId() && e1.getOffset() == entry1.getOffset());
        assertTrue(e2.getEntryId() == entry2.getEntryId() &&
            e2.getPartId() == entry2.getPartId() && e2.getOffset() == entry2.getOffset());
        assertTrue(e3.getEntryId() == entry3.getEntryId() &&
            e3.getPartId() == entry3.getPartId() && e3.getOffset() == entry3.getOffset());
        wrapper.release();

        // verify build OffloadIndexBlock from InputStream
        InputStream out2 = indexBlock.toStream();
        OffloadIndexBlock indexBlock2 = blockBuilder.fromStream(out2);
        // 1. verify metadata that got from inputstream success.
        LedgerMetadata metadata2 = indexBlock2.getLedgerMetadata();
        log.debug("built metadata: {}", metadata2.toString());
        assertTrue(metadata2.getAckQuorumSize() == metadata.getAckQuorumSize());
        assertTrue(metadata2.getEnsembleSize() == metadata.getEnsembleSize());
        assertTrue(metadata2.getDigestType() == metadata.getDigestType());
        assertTrue(metadata2.getEnsembleAt(0).toString().equals(metadata.getEnsembleAt(0).toString()));

        // 2. verify set all the entries
        assertTrue(indexBlock2.getEntryCount() == indexBlock.getEntryCount());
        indexBlock.close();
    }

}
