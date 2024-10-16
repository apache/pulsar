/*
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * ManagedCursorInfo metadata test.
 */
@Slf4j
public class ManagedCursorInfoMetadataTest {
    private final String INVALID_TYPE = "INVALID_TYPE";

    @DataProvider(name = "compressionTypeProvider")
    private Object[][] compressionTypeProvider() {
        return new Object[][]{
                {null},
                {INVALID_TYPE},
                {CompressionType.NONE.name()},
                {CompressionType.LZ4.name()},
                {CompressionType.ZLIB.name()},
                {CompressionType.ZSTD.name()},
                {CompressionType.SNAPPY.name()}
        };
    }

    private MLDataFormats.ManagedCursorInfo.Builder generateManagedCursorInfo(long ledgerId, int positionNumber) {
        MLDataFormats.ManagedCursorInfo.Builder builder = MLDataFormats.ManagedCursorInfo.newBuilder();

        builder.setCursorsLedgerId(ledgerId);
        builder.setMarkDeleteLedgerId(ledgerId);

        List<MLDataFormats.BatchedEntryDeletionIndexInfo> batchedEntryDeletionIndexInfos = new ArrayList<>();
        for (int i = 0; i < positionNumber; i++) {
            MLDataFormats.NestedPositionInfo nestedPositionInfo = MLDataFormats.NestedPositionInfo.newBuilder()
                    .setEntryId(i).setLedgerId(i).build();
            MLDataFormats.BatchedEntryDeletionIndexInfo batchedEntryDeletionIndexInfo = MLDataFormats
                    .BatchedEntryDeletionIndexInfo.newBuilder().setPosition(nestedPositionInfo).build();
            batchedEntryDeletionIndexInfos.add(batchedEntryDeletionIndexInfo);
        }
        builder.addAllBatchedEntryDeletionIndexInfo(batchedEntryDeletionIndexInfos);

        return builder;
    }

    @Test(dataProvider = "compressionTypeProvider")
    public void testEncodeAndDecode(String compressionType) throws IOException {
        long ledgerId = 10000;
        MLDataFormats.ManagedCursorInfo.Builder builder = generateManagedCursorInfo(ledgerId, 1000);
        MetaStoreImpl metaStore;
        if (INVALID_TYPE.equals(compressionType)) {
            IllegalArgumentException compressionTypeEx = expectThrows(IllegalArgumentException.class, () -> {
                new MetaStoreImpl(null, null, null, new MetadataCompressionConfig(compressionType));
            });
            assertEquals(compressionTypeEx.getMessage(),
                    "No enum constant org.apache.bookkeeper.mledger.proto.MLDataFormats.CompressionType."
                            + compressionType);
            return;
        } else {
            metaStore = new MetaStoreImpl(null, null, null, new MetadataCompressionConfig(compressionType));
        }

        MLDataFormats.ManagedCursorInfo managedCursorInfo = builder.build();
        byte[] compressionBytes = metaStore.compressCursorInfo(managedCursorInfo);
        log.info("[{}] Uncompressed data size: {}, compressed data size: {}",
                compressionType, managedCursorInfo.getSerializedSize(), compressionBytes.length);
        if (compressionType == null || compressionType.equals(CompressionType.NONE.name())) {
            assertEquals(compressionBytes.length, managedCursorInfo.getSerializedSize());
        }

        // parse compression data and unCompression data, check their results.
        MLDataFormats.ManagedCursorInfo info1 = metaStore.parseManagedCursorInfo(compressionBytes);
        MLDataFormats.ManagedCursorInfo info2 = metaStore.parseManagedCursorInfo(managedCursorInfo.toByteArray());
        assertEquals(info1, info2);
    }

    @Test(dataProvider = "compressionTypeProvider")
    public void testCompressionThreshold(String compressionType) throws IOException {
        int compressThreshold = 512;

        long ledgerId = 10000;
        // should not compress
        MLDataFormats.ManagedCursorInfo smallInfo = generateManagedCursorInfo(ledgerId, 1).build();
        assertTrue(smallInfo.getSerializedSize() < compressThreshold);

        // should compress
        MLDataFormats.ManagedCursorInfo bigInfo = generateManagedCursorInfo(ledgerId, 1000).build();
        assertTrue(bigInfo.getSerializedSize() > compressThreshold);

        MetaStoreImpl metaStore;
        if (INVALID_TYPE.equals(compressionType)) {
            IllegalArgumentException compressionTypeEx = expectThrows(IllegalArgumentException.class, () -> {
                new MetaStoreImpl(null, null, null,
                        new MetadataCompressionConfig(compressionType, compressThreshold));
            });
            assertEquals(compressionTypeEx.getMessage(),
                    "No enum constant org.apache.bookkeeper.mledger.proto.MLDataFormats.CompressionType."
                            + compressionType);
            return;
        } else {
            metaStore = new MetaStoreImpl(null, null, null,
                    new MetadataCompressionConfig(compressionType, compressThreshold));
        }

        byte[] compressionBytes = metaStore.compressCursorInfo(smallInfo);
        // not compressed
        assertEquals(compressionBytes.length, smallInfo.getSerializedSize());


        byte[] compressionBigBytes = metaStore.compressCursorInfo(bigInfo);
        // compressed
        assertTrue(compressionBigBytes.length != smallInfo.getSerializedSize());
    }
}
