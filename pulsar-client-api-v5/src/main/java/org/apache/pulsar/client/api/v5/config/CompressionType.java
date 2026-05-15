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
package org.apache.pulsar.client.api.v5.config;

/**
 * Compression codec used for message payloads.
 */
public enum CompressionType {

    /** No compression. Lowest CPU cost but highest bandwidth and storage usage. */
    NONE,

    /**
     * LZ4 compression. Very fast compression and decompression with moderate compression ratio.
     * A good general-purpose default for latency-sensitive and CPU-sensitive workloads.
     */
    LZ4,

    /**
     * ZLIB compression. Higher compression ratio than LZ4 but significantly slower.
     * Prefer {@link #ZSTD} which achieves better compression ratios with faster performance.
     */
    ZLIB,

    /**
     * Zstandard compression. Best compression ratio of all options with decompression speed
     * close to LZ4. Recommended for throughput and storage-optimized workloads.
     */
    ZSTD,

    /**
     * Snappy compression. Very fast with a compression profile similar to LZ4.
     * Useful for compatibility with ecosystems that standardize on Snappy.
     */
    SNAPPY
}
