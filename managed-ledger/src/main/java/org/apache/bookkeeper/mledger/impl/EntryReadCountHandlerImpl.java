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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.ToString;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;

@ToString
public class EntryReadCountHandlerImpl implements EntryReadCountHandler {
    private static final AtomicIntegerFieldUpdater<EntryReadCountHandlerImpl> EXPECTED_READ_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EntryReadCountHandlerImpl.class, "expectedReadCount");

    private volatile int expectedReadCount;

    private EntryReadCountHandlerImpl(int expectedReadCount) {
        this.expectedReadCount = expectedReadCount;
    }

    public int getExpectedReadCount() {
        return expectedReadCount;
    }

    @Override
    public void incrementExpectedReadCount() {
        EXPECTED_READ_COUNT_UPDATER.incrementAndGet(this);
    }

    @Override
    public void markRead() {
        EXPECTED_READ_COUNT_UPDATER.decrementAndGet(this);
    }

    /**
     * Creates an instance of EntryReadCountHandlerImpl if the expected read count is greater than 0.
     * If the expected read count is 0 or less, it returns null.
     *
     * @param expectedReadCount the expected read count for the entry
     * @return an instance of EntryReadCountHandlerImpl or null
     */
    public static EntryReadCountHandlerImpl maybeCreate(int expectedReadCount) {
        return expectedReadCount > 0 ? new EntryReadCountHandlerImpl(expectedReadCount) : null;
    }

    @VisibleForTesting
    public void setExpectedReadCount(int expectedReadCount) {
        this.expectedReadCount = expectedReadCount;
    }
}
