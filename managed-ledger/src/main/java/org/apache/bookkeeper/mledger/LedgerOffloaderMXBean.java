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

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

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
    String getName();

    /**
     * Record the offload total time.
     *
     * @return
     */
    Map<String, LongAdder> getOffloadTimes();

    /**
     * Record the offload error count.
     *
     * @return
     */
    Map<String, LongAdder> getOffloadErrors();

    /**
     * Record the write latency to tiered storage.
     *
     * @return
     */
    Map<String, StatsBuckets> getWriteToStorageLatencyBuckets();

    /**
     * Record the write rate to storage.
     *
     * @return
     */
    Map<String, Rate> getWriteToStorageRates();

    /**
     * Record the write to storage error count.
     *
     * @return
     */
    Map<String, LongAdder> getWriteToStorageErrors();

    /**
     * Record build the jclound index latency.
     *
     * @return
     */
    Map<String, StatsBuckets> getBuildJcloundIndexLatency();

    /**
     * @return
     */
    Map<String, LongAdder> getBuildJcloundIndexErrors();

    /**
     * Record read offload method rate.
     *
     * @return
     */
    Map<String, Rate> getReadOffloadRates();

    /**
     * Record read offload error count.
     *
     * @return
     */
    Map<String, LongAdder> getReadOffloadErrors();

    /**
     * Record streaming read offload method rate.
     *
     * @return
     */
    Map<String, Rate> getStreamingWriteToStorageRates();

    /**
     * Record streaming read offload error count.
     *
     * @return
     */
    Map<String, LongAdder> getStreamingWriteToStorageErrors();

}
