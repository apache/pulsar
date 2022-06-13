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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedTrash;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

public class ManagedTrashDisableImpl implements ManagedTrash {

    private static final CompletableFuture<?> COMPLETABLE_FUTURE =  CompletableFuture.completedFuture(null);

    @Override
    public String name() {
        return "";
    }

    @Override
    public CompletableFuture<?> initialize() {
        return COMPLETABLE_FUTURE;
    }

    @Override
    public void appendLedgerTrashData(long ledgerId, LedgerInfo context, LedgerType type)
            throws ManagedLedgerException {
    }

    @Override
    public CompletableFuture<?> asyncUpdateTrashData() {
        return COMPLETABLE_FUTURE;
    }

    @Override
    public void triggerDeleteInBackground() {
    }

    @Override
    public CompletableFuture<List<Long>> getAllArchiveIndex() {
        return (CompletableFuture<List<Long>>) COMPLETABLE_FUTURE;
    }

    @Override
    public CompletableFuture<Map<ManagedTrashImpl.TrashKey, LedgerInfo>> getArchiveData(long index) {
        return (CompletableFuture<Map<ManagedTrashImpl.TrashKey, LedgerInfo>>) COMPLETABLE_FUTURE;
    }

    @Override
    public long getTrashDataSize() {
        return 0;
    }

    @Override
    public long getToArchiveDataSize() {
        return 0;
    }

    @Override
    public CompletableFuture<?> asyncClose() {
        return COMPLETABLE_FUTURE;
    }

    @Override
    public CompletableFuture<?> asyncCloseAfterAllLedgerDeleteOnce() {
        return COMPLETABLE_FUTURE;
    }
}
