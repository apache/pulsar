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
package org.apache.bookkeeper.mledger.util;

import static org.testng.Assert.assertEquals;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.testng.annotations.Test;

public class FuturesTest {

    @Test
    public void testExecuteWithRetryHandlesSynchronousFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<String> result = Futures.executeWithRetry(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new CompletionException(new ManagedLedgerException.MetaStoreException("sync fail"));
            }
            return CompletableFuture.completedFuture("ok");
        }, ManagedLedgerException.MetaStoreException.class, 1);

        assertEquals(result.get(2, TimeUnit.SECONDS), "ok");
        assertEquals(attempts.get(), 2);
    }
}
