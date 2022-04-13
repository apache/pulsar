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

class LedgerOffloaderStatsDisable implements LedgerOffloaderStats {

    static final LedgerOffloaderStats INSTANCE = new LedgerOffloaderStatsDisable();

    private LedgerOffloaderStatsDisable() {

    }

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
    public void recordDeleteOffloadOps(String topic, boolean succeed) {

    }

    @Override
    public void close() throws Exception {

    }
}
