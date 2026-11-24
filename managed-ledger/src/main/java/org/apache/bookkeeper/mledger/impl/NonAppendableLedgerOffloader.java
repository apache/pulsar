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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.util.FutureUtil;

public class NonAppendableLedgerOffloader implements LedgerOffloader {
    private LedgerOffloader delegate;

    public NonAppendableLedgerOffloader(LedgerOffloader delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getOffloadDriverName() {
        return delegate.getOffloadDriverName();
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle ledger,
                                           UUID uid,
                                           Map<String, String> extraMetadata) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
        return delegate.readOffloaded(ledgerId, uid, offloadDriverMetadata);
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        return delegate.deleteOffloaded(ledgerId, uid, offloadDriverMetadata);
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        return delegate.getOffloadPolicies();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isAppendable() {
        return false;
    }
}
