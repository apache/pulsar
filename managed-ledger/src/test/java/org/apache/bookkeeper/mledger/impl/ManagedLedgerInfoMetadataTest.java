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

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    @Test(dataProvider = "compressionTypeProvider")
    public void testEncodeAndDecode(String compressionType) throws IOException {
        long ledgerId = 10000;
        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfoList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder builder = MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder();
            builder.setLedgerId(ledgerId);
            builder.setEntries(RandomUtils.nextInt());
            builder.setSize(RandomUtils.nextLong());
            builder.setTimestamp(System.currentTimeMillis());

            UUID uuid = UUID.randomUUID();
            builder.getOffloadContextBuilder()
                    .setUidMsb(uuid.getMostSignificantBits())
                    .setUidLsb(uuid.getLeastSignificantBits());
            Map<String, String> offloadDriverMetadata = new HashMap<>();
            offloadDriverMetadata.put("bucket", "test-bucket");
            offloadDriverMetadata.put("managedLedgerOffloadDriver", "pulsar-offload-dev");
            offloadDriverMetadata.put("serviceEndpoint", "https://s3.eu-west-1.amazonaws.com");
            offloadDriverMetadata.put("region", "eu-west-1");
            OffloadUtils.setOffloadDriverMetadata(
                    builder,
                    "aws-s3",
                    offloadDriverMetadata
            );

            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = builder.build();
            ledgerInfoList.add(ledgerInfo);
            ledgerId ++;
        }

        MLDataFormats.ManagedLedgerInfo managedLedgerInfo = MLDataFormats.ManagedLedgerInfo.newBuilder()
                .addAllLedgerInfo(ledgerInfoList)
                .build();

        MetaStoreImpl metaStore;
        try {
            metaStore = new MetaStoreImpl(null, null, compressionType);
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.fail("The managedLedgerInfo compression type is invalid, should fail.");
            }
        } catch (Exception e) {
            if ("INVALID_TYPE".equals(compressionType)) {
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
                Assert.assertEquals(
                        "No enum constant org.apache.bookkeeper.mledger.proto.MLDataFormats.CompressionType."
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
        MLDataFormats.ManagedLedgerInfo info1 = metaStore.parseManagedLedgerInfo(compressionBytes);
        MLDataFormats.ManagedLedgerInfo info2 = metaStore.parseManagedLedgerInfo(managedLedgerInfo.toByteArray());
        Assert.assertEquals(info1, info2);
    }

    @Test
    public void testParseEmptyData() throws InvalidProtocolBufferException {
        MetaStoreImpl metaStore = new MetaStoreImpl(null, null);
        MLDataFormats.ManagedLedgerInfo managedLedgerInfo = metaStore.parseManagedLedgerInfo(new byte[0]);
        Assert.assertEquals(managedLedgerInfo.toString(), "");
    }

}
