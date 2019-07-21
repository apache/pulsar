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
package org.apache.bookkeeper.mledger.offload;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.KeyValue;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadContext;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadDriverMetadata;

public final class OffloadUtils {

    private OffloadUtils() {}

    public static Map<String, String> getOffloadDriverMetadata(LedgerInfo ledgerInfo) {
        Map<String, String> metadata = Maps.newHashMap();
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.getPropertiesCount() > 0) {
                    driverMetadata.getPropertiesList().forEach(kv -> metadata.put(kv.getKey(), kv.getValue()));
                }
            }
        }
        return metadata;
    }

    public static Map<String, String> getOffloadDriverMetadata(LedgerInfo ledgerInfo,
                                                               Map<String, String> defaultOffloadDriverMetadata) {
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.getPropertiesCount() > 0) {
                    Map<String, String> metadata = Maps.newHashMap();
                    driverMetadata.getPropertiesList().forEach(kv -> metadata.put(kv.getKey(), kv.getValue()));
                    return metadata;
                }
            }
        }
        return defaultOffloadDriverMetadata;
    }

    public static String getOffloadDriverName(LedgerInfo ledgerInfo, String defaultDriverName) {
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.hasName()) {
                    return driverMetadata.getName();
                }
            }
        }
        return defaultDriverName;
    }

    public static void setOffloadDriverMetadata(LedgerInfo.Builder infoBuilder,
                                                String driverName,
                                                Map<String, String> offloadDriverMetadata) {
        infoBuilder.getOffloadContextBuilder()
            .getDriverMetadataBuilder()
            .setName(driverName);
        offloadDriverMetadata.forEach((k, v) -> {
            infoBuilder.getOffloadContextBuilder()
                .getDriverMetadataBuilder()
                .addProperties(KeyValue.newBuilder()
                    .setKey(k)
                    .setValue(v)
                    .build());
        });
    }

}
