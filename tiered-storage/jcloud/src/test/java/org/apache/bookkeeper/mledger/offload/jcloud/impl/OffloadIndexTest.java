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

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.testng.annotations.Test;

@Slf4j
public class OffloadIndexTest {

    @Test
    public void offloadIndexEntryImplTest() {
        // verify OffloadIndexEntryImpl builder
        OffloadIndexEntryImpl entry1 = OffloadIndexEntryImpl.of(0, 2, 0, 20);
        OffloadIndexEntryImpl entry2 = OffloadIndexEntryImpl.of(100, 3, 1234, 20);

        // verify OffloadIndexEntryImpl get
        assertEquals(entry1.getEntryId(), 0L);
        assertEquals(entry1.getPartId(), 2);
        assertEquals(entry1.getOffset(), 0L);
        assertEquals(entry1.getDataOffset(), 20L);

        assertEquals(entry2.getEntryId(), 100L);
        assertEquals(entry2.getPartId(), 3);
        assertEquals(entry2.getOffset(), 1234L);
        assertEquals(entry2.getDataOffset(), 1254L);
    }


    // use mock to setLastEntryId
//    public static class LedgerMetadataMock extends org.apache.bookkeeper.client.LedgerMetadata {
//        long lastId = 0;
//        public LedgerMetadataMock(int ensembleSize, int writeQuorumSize, int ackQuorumSize, org.apache.bookkeeper.client.BookKeeper.DigestType digestType, byte[] password, Map<String, byte[]> customMetadata, boolean storeSystemtimeAsLedgerCreationTime) {
//            super(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, password, customMetadata, storeSystemtimeAsLedgerCreationTime);
//        }
//
//        @Override
//        public long getLastEntryId(){
//            return  lastId;
//        }
//
//        public void setLastEntryId(long lastId) {
//            this.lastId = lastId;
//        }
//    }

    public static LedgerMetadata createLedgerMetadata(long id) throws Exception {

        Map<String, byte[]> metadataCustom = Maps.newHashMap();
        metadataCustom.put("key1", "value1".getBytes(UTF_8));
        metadataCustom.put("key7", "value7".getBytes(UTF_8));

        ArrayList<BookieId> bookies = Lists.newArrayList();
        bookies.add(0, new BookieSocketAddress("127.0.0.1:3181").toBookieId());
        bookies.add(1, new BookieSocketAddress("127.0.0.2:3181").toBookieId());
        bookies.add(2, new BookieSocketAddress("127.0.0.3:3181").toBookieId());

        return LedgerMetadataBuilder.create().withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                .withDigestType(DigestType.CRC32C).withPassword("password".getBytes(UTF_8))
                .withCustomMetadata(metadataCustom).withClosedState().withLastEntryId(5000).withLength(100)
                .newEnsembleEntry(0L, bookies).withId(id).build();

    }

    public static LedgerInfo createLedgerInfo(long id) throws Exception {

        Map<String, byte[]> metadataCustom = Maps.newHashMap();
        metadataCustom.put("key1", "value1".getBytes(UTF_8));
        metadataCustom.put("key7", "value7".getBytes(UTF_8));

        return LedgerInfo.newBuilder().setLedgerId(id).setEntries(5001).setSize(10000).build();
    }

    // prepare metadata, then use builder to build a OffloadIndexBlockImpl
    // verify get methods, readout and fromStream methods.
    @Test
    public void offloadIndexBlockImplTest() throws Exception {
        OffloadIndexBlockBuilder blockBuilder = OffloadIndexBlockBuilder.create();
        LedgerMetadata metadata = createLedgerMetadata(1); // use dummy ledgerId, from BK 4.12 the ledger is is required
        log.debug("created metadata: {}", metadata.toString());

        blockBuilder.withLedgerMetadata(metadata).withDataObjectLength(1).withDataBlockHeaderLength(23455);

        blockBuilder.addBlock(0, 2, 64 * 1024 * 1024);
        blockBuilder.addBlock(1000, 3, 64 * 1024 * 1024);
        blockBuilder.addBlock(2000, 4, 64 * 1024 * 1024);
        OffloadIndexBlock indexBlock = blockBuilder.build();

        // verify getEntryCount and getLedgerMetadata
        assertEquals(indexBlock.getEntryCount(), 3);
        assertEquals(indexBlock.getLedgerMetadata(), metadata);

        // verify getIndexEntryForEntry
        OffloadIndexEntry entry1 = indexBlock.getIndexEntryForEntry(0);
        assertEquals(entry1.getEntryId(), 0);
        assertEquals(entry1.getPartId(), 2);
        assertEquals(entry1.getOffset(), 0);

        OffloadIndexEntry entry11 = indexBlock.getIndexEntryForEntry(500);
        assertEquals(entry11, entry1);

        OffloadIndexEntry entry2 = indexBlock.getIndexEntryForEntry(1000);
        assertEquals(entry2.getEntryId(), 1000);
        assertEquals(entry2.getPartId(), 3);
        assertEquals(entry2.getOffset(), 64 * 1024 * 1024);

        OffloadIndexEntry entry22 = indexBlock.getIndexEntryForEntry(1300);
        assertEquals(entry22, entry2);

        OffloadIndexEntry entry3 = indexBlock.getIndexEntryForEntry(2000);

        assertEquals(entry3.getEntryId(), 2000);
        assertEquals(entry3.getPartId(), 4);
        assertEquals(entry3.getOffset(), 2 * 64 * 1024 * 1024);

        OffloadIndexEntry entry33 = indexBlock.getIndexEntryForEntry(3000);
        assertEquals(entry33, entry3);

        try {
            OffloadIndexEntry entry4 = indexBlock.getIndexEntryForEntry(6000);
            fail("Should throw IndexOutOfBoundsException.");
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
            assertEquals(e.getMessage(), "Entry index: 6000 beyond lastEntryId: 5000");
        }

        // verify toStream
        InputStream out = indexBlock.toStream();
        byte b[] = new byte[1024];
        int readoutLen = out.read(b);
        out.close();
        ByteBuf wrapper = Unpooled.wrappedBuffer(b);
        int magic = wrapper.readInt();
        int indexBlockLength = wrapper.readInt();
        long dataObjectLength = wrapper.readLong();
        long dataHeaderLength = wrapper.readLong();
        int indexEntryCount = wrapper.readInt();
        int segmentMetadataLength = wrapper.readInt();

        // verify counter
        assertEquals(magic, OffloadIndexBlockImpl.getIndexMagicWord());
        assertEquals(indexBlockLength, readoutLen);
        assertEquals(indexEntryCount, 3);
        assertEquals(dataObjectLength, 1);
        assertEquals(dataHeaderLength, 23455);

        wrapper.readBytes(segmentMetadataLength);
        log.debug("magic: {}, blockLength: {}, metadataLength: {}, indexCount: {}",
            magic, indexBlockLength, segmentMetadataLength, indexEntryCount);

        // verify entry
        OffloadIndexEntry e1 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                                                        wrapper.readLong(), dataHeaderLength);
        OffloadIndexEntry e2 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                                                        wrapper.readLong(), dataHeaderLength);
        OffloadIndexEntry e3 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                                                        wrapper.readLong(), dataHeaderLength);

        assertEquals(e1.getEntryId(), entry1.getEntryId());
        assertEquals(e1.getPartId(), entry1.getPartId());
        assertEquals(e1.getOffset(), entry1.getOffset());
        assertEquals(e1.getDataOffset(), entry1.getDataOffset());
        assertEquals(e2.getEntryId(), entry2.getEntryId());
        assertEquals(e2.getPartId(), entry2.getPartId());
        assertEquals(e2.getOffset(), entry2.getOffset());
        assertEquals(e2.getDataOffset(), entry2.getDataOffset());
        assertEquals(e3.getEntryId(), entry3.getEntryId());
        assertEquals(e3.getPartId(), entry3.getPartId());
        assertEquals(e3.getOffset(), entry3.getOffset());
        assertEquals(e3.getDataOffset(), entry3.getDataOffset());
        wrapper.release();

        // verify build OffloadIndexBlock from InputStream
        InputStream out2 = indexBlock.toStream();
        int streamLength = out2.available();
        out2.mark(0);
        OffloadIndexBlock indexBlock2 = (OffloadIndexBlock) blockBuilder.fromStream(out2);
        // 1. verify metadata that got from inputstream success.
        LedgerMetadata metadata2 = indexBlock2.getLedgerMetadata();
        log.debug("built metadata: {}", metadata2.toString());
        assertEquals(metadata2.getAckQuorumSize(), metadata.getAckQuorumSize());
        assertEquals(metadata2.getEnsembleSize(), metadata.getEnsembleSize());
        assertEquals(metadata2.getDigestType(), metadata.getDigestType());
        assertEquals(metadata2.getAllEnsembles().entrySet(), metadata.getAllEnsembles().entrySet());
        // 2. verify set all the entries
        assertEquals(indexBlock2.getEntryCount(), indexBlock.getEntryCount());
        // 3. verify reach end
        assertEquals(out2.read(), -1);


        out2.reset();
        byte streamContent[] = new byte[streamLength];
        // stream with all 0, simulate junk data, should throw exception for header magic not match.
        try(InputStream stream3 = new ByteArrayInputStream(streamContent, 0, streamLength)) {
            OffloadIndexBlock indexBlock3 = (OffloadIndexBlock) blockBuilder.fromStream(stream3);
            fail("Should throw IOException");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertTrue(e.getMessage().contains("Invalid MagicWord"));
        }

        // simulate read header too small, throw EOFException.
        out2.read(streamContent);
        try(InputStream stream4 =
                new ByteArrayInputStream(streamContent, 0, streamLength - 1)) {
            OffloadIndexBlock indexBlock4 = (OffloadIndexBlock) blockBuilder.fromStream(stream4);
            fail("Should throw EOFException");
        } catch (Exception e) {
            assertTrue(e instanceof java.io.EOFException);
        }

        out2.close();
        indexBlock.close();
    }

}
