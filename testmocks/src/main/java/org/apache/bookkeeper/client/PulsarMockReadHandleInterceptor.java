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
package org.apache.bookkeeper.client;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;

/**
 * Interceptor interface for intercepting read handle readAsync operations.
 * This is useful for testing purposes, for example for introducing delays.
 */
public interface PulsarMockReadHandleInterceptor {
    /**
     * Intercepts the readAsync operation on a read handle.
     *
     * @param ledgerId ledger id
     * @param firstEntry first entry to read
     * @param lastEntry  last entry to read
     * @param entries    entries that would be returned by the read operation
     * @return CompletableFuture that will complete with the entries to return
     */
    CompletableFuture<LedgerEntries> interceptReadAsync(long ledgerId, long firstEntry, long lastEntry,
                                                        LedgerEntries entries);
}
