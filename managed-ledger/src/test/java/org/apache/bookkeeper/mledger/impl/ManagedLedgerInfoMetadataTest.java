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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * ManagedLedgerInfo metadata test.
 */
@Slf4j
public class ManagedLedgerInfoMetadataTest {

    @DataProvider(name = "compressionTypeProvider")
    private Object[][] compressionTypeProvider() {
        return new Object[][] {
                {null},
                {"INVALID_TYPE"},
                {CompressionType.NONE.name()},
                {CompressionType.LZ4.name()},
                {CompressionType.ZLIB.name()},
                {CompressionType.ZSTD.name()},
                {CompressionType.SNAPPY.name()}
        };
    }

    @SuppressWarnings("deprecation")
    private ManagedLedgerInfo generateManagedLedgerInfo(long ledgerId, int ledgerInfoNumber) {
        ManagedLedgerInfo managedLedgerInfo = new ManagedLedgerInfo();
        for (int i = 0; i < ledgerInfoNumber; i++) {
            LedgerInfo ledgerInfo = managedLedgerInfo.addLedgerInfo();
            ledgerInfo.setLedgerId(ledgerId);
            ledgerInfo.setEntries(RandomUtils.nextInt());
            ledgerInfo.setSize(RandomUtils.nextLong());
            ledgerInfo.setTimestamp(System.currentTimeMillis());

            UUID uuid = UUID.randomUUID();
            ledgerInfo.setOffloadContext()
                    .setUidMsb(uuid.getMostSignificantBits())
                    .setUidLsb(uuid.getLeastSignificantBits());
            Map<String, String> offloadDriverMetadata = new HashMap<>();
            offloadDriverMetadata.put("bucket", "test-bucket");
            offloadDriverMetadata.put("managedLedgerOffloadDriver", "pulsar-offload-dev");
            offloadDriverMetadata.put("serviceEndpoint", "https://s3.eu-west-1.amazonaws.com");
            offloadDriverMetadata.put("region", "eu-west-1");
            OffloadUtils.setOffloadDriverMetadata(
                    ledgerInfo,
                    "aws-s3",
                    offloadDriverMetadata
            );

            ledgerId++;
        }

        return managedLedgerInfo;
    }

    @Test(dataProvider = "compressionTypeProvider")
    public void testEncodeAndDecode(String compressionType) throws Exception {
        long ledgerId = 10000;
        ManagedLedgerInfo managedLedgerInfo = generateManagedLedgerInfo(ledgerId, 100);

        MetaStoreImpl metaStore;
        try {
            metaStore = new MetaStoreImpl(null, null, new MetadataCompressionConfig(compressionType), null);
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.fail("The managedLedgerInfo compression type is invalid, should fail.");
            }
        } catch (Exception e) {
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
                Assert.assertEquals(
                        "No enum constant org.apache.bookkeeper.mledger.proto.CompressionType."
                                + compressionType, e.getMessage());
                return;
            } else {
                throw e;
            }
        }

        byte[] compressionBytes = metaStore.compressLedgerInfo(managedLedgerInfo);
        log.info("[{}] Uncompressed data size: {}, compressed data size: {}",
                compressionType, managedLedgerInfo.getSerializedSize(), compressionBytes.length);
        if (compressionType == null || compressionType.equals(CompressionType.NONE.name())) {
            Assert.assertEquals(compressionBytes.length, managedLedgerInfo.getSerializedSize());
        }

        // parse compression data and unCompression data, check their results.
        ManagedLedgerInfo info1 = metaStore.parseManagedLedgerInfo(compressionBytes);
        ManagedLedgerInfo info2 = metaStore.parseManagedLedgerInfo(managedLedgerInfo.toByteArray());
        Assert.assertEquals(info1.toByteArray(), info2.toByteArray());
    }

    @Test
    public void testParseEmptyData() throws Exception {
        MetaStoreImpl metaStore = new MetaStoreImpl(null, null);
        ManagedLedgerInfo managedLedgerInfo = metaStore.parseManagedLedgerInfo(new byte[0]);
        Assert.assertEquals(managedLedgerInfo.toByteArray(), new byte[0]);
    }

    @Test(dataProvider = "compressionTypeProvider")
    public void testCompressionThreshold(String compressionType) {
        long ledgerId = 10000;
        int compressThreshold = 512;

        // should not compress
        ManagedLedgerInfo smallInfo = generateManagedLedgerInfo(ledgerId, 0);
        assertTrue(smallInfo.getSerializedSize() < compressThreshold);

        // should compress
        ManagedLedgerInfo bigInfo = generateManagedLedgerInfo(ledgerId, 1000);
        assertTrue(bigInfo.getSerializedSize() > compressThreshold);

        ManagedLedgerInfo managedLedgerInfo = generateManagedLedgerInfo(ledgerId, 100);

        MetaStoreImpl metaStore;
        try {
            MetadataCompressionConfig metadataCompressionConfig =
                    new MetadataCompressionConfig(compressionType, compressThreshold);
            metaStore = new MetaStoreImpl(null, null, metadataCompressionConfig, null);
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.fail("The managedLedgerInfo compression type is invalid, should fail.");
            }
        } catch (Exception e) {
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
                Assert.assertEquals(
                        "No enum constant org.apache.bookkeeper.mledger.proto.CompressionType."
                                + compressionType, e.getMessage());
                return;
            } else {
                throw e;
            }
        }

        byte[] compressionBytes = metaStore.compressLedgerInfo(smallInfo);
        assertEquals(compressionBytes.length, smallInfo.getSerializedSize());

        byte[] compressionBytesBig = metaStore.compressLedgerInfo(bigInfo);
        assertTrue(compressionBytesBig.length != smallInfo.getSerializedSize());
    }
}
