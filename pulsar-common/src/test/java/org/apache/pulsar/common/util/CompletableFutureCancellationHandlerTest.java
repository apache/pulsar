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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class CompletableFutureCancellationHandlerTest {

    @Test
    public void callsCancelActionWhenCancelled() {
        // given
        AtomicBoolean called = new AtomicBoolean();
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Object> future = cancellationHandler.createFuture();
        cancellationHandler.setCancelAction(() -> called.set(true));
        // when
        future.cancel(false);
        // then
        assertTrue(called.get());
    }

    @Test
    public void callsCancelActionWhenTimeoutHappens() {
        // given
        AtomicBoolean called = new AtomicBoolean();
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Object> future = cancellationHandler.createFuture();
        cancellationHandler.setCancelAction(() -> called.set(true));
        // when
        future.completeExceptionally(new TimeoutException());
        // then
        assertTrue(called.get());
    }
}
