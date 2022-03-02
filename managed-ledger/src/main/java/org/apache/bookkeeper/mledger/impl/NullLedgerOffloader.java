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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * Null implementation that throws an error on any invokation.
 */
public class NullLedgerOffloader implements LedgerOffloader {
    public static final NullLedgerOffloader INSTANCE = new NullLedgerOffloader();

    @Override
    public String getOffloadDriverName() {
        return "NullLedgerOffloader";
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle ledger,
                                           UUID uid,
                                           Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    @Override
    public OffloadPoliciesImpl getOffloadPolicies() {
        return null;
    }

    @Override
    public void close() {

    }
}
