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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;


/**
 * Management Bean for a {@link LedgerOffloader}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface LedgerOffloaderStats extends AutoCloseable {

    void recordOffloadError(String topic);

    void recordOffloadBytes(String topic, long size);

    void recordReadLedgerLatency(String topic, long latency, TimeUnit unit);

    void recordWriteToStorageError(String topic);

    void recordReadOffloadError(String topic);

    void recordReadOffloadBytes(String topic, long size);

    void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit);

    void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit);


    LedgerOffloaderStats NOOP = new LedgerOffloaderStats() {
        @Override
        public void recordOffloadError(String topic) {

        }

        @Override
        public void recordOffloadBytes(String topic, long size) {

        }

        @Override
        public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {

        }

        @Override
        public void recordWriteToStorageError(String topic) {

        }

        @Override
        public void recordReadOffloadError(String topic) {

        }

        @Override
        public void recordReadOffloadBytes(String topic, long size) {

        }

        @Override
        public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {

        }

        @Override
        public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {

        }

        @Override
        public void close() throws Exception {

        }
    };
}
