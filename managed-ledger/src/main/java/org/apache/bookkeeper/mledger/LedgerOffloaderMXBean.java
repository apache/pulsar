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
package org.apache.bookkeeper.mledger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;


/**
 * Management Bean for a {@link LedgerOffloader}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface LedgerOffloaderMXBean {

    /**
     * The ledger offloader name.
     *
     * @return ledger offloader name.
     */
    String getDriverName();


    /**
     * Refresh offloader stats.
     *
     */
    void refreshStats(long period, TimeUnit unit);



    /**
     * Record the offload total time.
     *
     * @return offload time per topic.
     */
    Map<String, Rate> getOffloadTimes();

    /**
     * Record the offload error count.
     *
     * @return offload errors per topic.
     */
    Map<String, Rate> getOffloadErrors();

    /**
     * Record the offload rate to storage.
     *
     * @return offload rate per topic.
     */
    Map<String, Rate> getOffloadRates();


    /**
     * Record the read ledger latency.
     *
     * @return read ledger latency per topic.
     */
    Map<String, StatsBuckets> getReadLedgerLatencyBuckets();

    /**
     * Record the write latency to tiered storage.
     *
     * @return write to storage latency per topic.
     */
    Map<String, StatsBuckets> getWriteToStorageLatencyBuckets();

    /**
     * Record the write to storage error count.
     *
     * @return write to storage errors per topic.
     */
    Map<String, Rate> getWriteToStorageErrors();

    /**
     * Record read offload index latency.
     *
     * @return read offload index latency per topic.
     */
    Map<String, StatsBuckets> getReadOffloadIndexLatencyBuckets();

    /**
     * Record read offload data latency.
     *
     * @return read offload data latency per topic.
     */
    Map<String, StatsBuckets> getReadOffloadDataLatencyBuckets();

    /**
     * Record read offload method rate.
     *
     * @return read offload data rate.
     */
    Map<String, Rate> getReadOffloadRates();

    /**
     * Record read offload error count.
     *
     * @return read offload data errors.
     */
    Map<String, Rate> getReadOffloadErrors();

    /**
     * Record streaming read offload method rate.
     *
     * @return streaming write to storage rate per topic.
     */
    Map<String, Rate> getStreamingWriteToStorageRates();

    /**
     * Record streaming read offload error count.
     *
     * @return streaming write to storage errors per topic.
     */
    Map<String, Rate> getStreamingWriteToStorageErrors();
}
