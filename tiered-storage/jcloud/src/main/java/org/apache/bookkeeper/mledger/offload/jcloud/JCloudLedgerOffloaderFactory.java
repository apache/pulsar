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
package org.apache.bookkeeper.mledger.offload.jcloud;

import java.io.IOException;
import java.util.Map;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.LedgerOffloaderStatsDisable;
import org.apache.bookkeeper.mledger.OffloadedReadCacheConfig;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffloadedDataChunkCache;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;

/**
 * A jcloud based offloader factory.
 */
@CustomLog
public class JCloudLedgerOffloaderFactory implements LedgerOffloaderFactory<BlobStoreManagedLedgerOffloader> {
    private final OffsetsCache entryOffsetsCache = new OffsetsCache();
    // Shared by the offloaders, created with the broker's settings of the first offloader; guarded by this
    private OffloadedDataChunkCache dataChunkCache;
    private OffloadedReadCacheConfig dataChunkCacheConfig;
    private boolean closed;

    @Override
    public boolean isDriverSupported(String driverName) {
        return JCloudBlobStoreProvider.driverSupported(driverName);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies, Map<String, String> userMetadata,
                                                  OrderedScheduler scheduler) throws IOException {
        return create(offloadPolicies, userMetadata, scheduler, LedgerOffloaderStatsDisable.INSTANCE);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies, Map<String, String> userMetadata,
                                                  OrderedScheduler scheduler,
                                                  LedgerOffloaderStats offloaderStats) throws IOException {
        // The scheduler reads too, as it did before the offloaders had a read executor
        return create(offloadPolicies, userMetadata, null, scheduler, scheduler, offloaderStats,
                OffloadedReadCacheConfig.DEFAULT);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies, Map<String, String> userMetadata,
                                                  OrderedScheduler scheduler,
                                                  OrderedScheduler readExecutor,
                                                  LedgerOffloaderStats offloaderStats) throws IOException {
        return create(offloadPolicies, userMetadata, null, scheduler, readExecutor, offloaderStats,
                OffloadedReadCacheConfig.DEFAULT);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies, Map<String, String> userMetadata,
                                                  SchemaStorage schemaStorage,
                                                  OrderedScheduler scheduler,
                                                  OrderedScheduler readExecutor,
                                                  LedgerOffloaderStats offloaderStats) throws IOException {
        return create(offloadPolicies, userMetadata, schemaStorage, scheduler, readExecutor, offloaderStats,
                OffloadedReadCacheConfig.DEFAULT);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies, Map<String, String> userMetadata,
                                                  SchemaStorage schemaStorage,
                                                  OrderedScheduler scheduler,
                                                  OrderedScheduler readExecutor,
                                                  LedgerOffloaderStats offloaderStats,
                                                  OffloadedReadCacheConfig readCacheConfig) throws IOException {
        TieredStorageConfiguration config =
                TieredStorageConfiguration.create(offloadPolicies.toProperties());
        return BlobStoreManagedLedgerOffloader.create(config, userMetadata, scheduler, readExecutor, offloaderStats,
                entryOffsetsCache, dataChunkCache(readCacheConfig));
    }

    private synchronized OffloadedDataChunkCache dataChunkCache(OffloadedReadCacheConfig readCacheConfig)
            throws IOException {
        if (closed) {
            throw new IOException("The offloader factory is closed");
        }
        if (dataChunkCache == null) {
            dataChunkCache = new OffloadedDataChunkCache(readCacheConfig);
            dataChunkCacheConfig = readCacheConfig;
            log.debug().attr("config", readCacheConfig).log("Created the cache of offloaded data");
        } else if (!dataChunkCacheConfig.equals(readCacheConfig)) {
            // The broker passes its settings to every offloader, so they only differ for another caller
            log.warn().attr("config", dataChunkCacheConfig).attr("requestedConfig", readCacheConfig)
                    .log("The cache of offloaded data keeps the settings that it was created with");
        }
        return dataChunkCache;
    }

    @Override
    public void close() throws Exception {
        entryOffsetsCache.close();
        synchronized (this) {
            closed = true;
            if (dataChunkCache != null) {
                dataChunkCache.close();
            }
        }
    }
}
