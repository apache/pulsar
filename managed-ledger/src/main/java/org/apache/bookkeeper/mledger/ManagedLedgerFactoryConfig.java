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
package org.apache.bookkeeper.mledger;

public class ManagedLedgerFactoryConfig {
    private static final long MB = 1024 * 1024;

    private long maxCacheSize = 128 * MB;
    private double cacheEvictionWatermark = 0.90;

    private boolean useProtobufBinaryFormatInZK = false;

    public long getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     *
     * @param maxCacheSize
     * @return
     */
    public ManagedLedgerFactoryConfig setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public double getCacheEvictionWatermark() {
        return cacheEvictionWatermark;
    }

    /**
     * The cache eviction watermark is the percentage of the cache size to reach when removing entries from the cache.
     *
     * @param cacheEvictionWatermark
     * @return
     */
    public ManagedLedgerFactoryConfig setCacheEvictionWatermark(double cacheEvictionWatermark) {
        this.cacheEvictionWatermark = cacheEvictionWatermark;
        return this;
    }

    public boolean useProtobufBinaryFormatInZK() {
        return useProtobufBinaryFormatInZK;
    }

    public void setUseProtobufBinaryFormatInZK(boolean useProtobufBinaryFormatInZK) {
        this.useProtobufBinaryFormatInZK = useProtobufBinaryFormatInZK;
    }

}
