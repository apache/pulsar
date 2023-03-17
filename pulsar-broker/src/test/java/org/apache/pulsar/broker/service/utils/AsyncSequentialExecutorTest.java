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
package org.apache.pulsar.broker.service.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.AsyncSequentialExecutor;
import org.apache.pulsar.common.util.FutureUtil;
import org.testcontainers.shaded.org.apache.commons.lang3.mutable.MutableInt;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AsyncSequentialExecutorTest {

    ExecutorService executor = Executors.newFixedThreadPool(1000);

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        this.executor.shutdown();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException, TimeoutException {
        MutableInt i = new MutableInt();
        AsyncSequentialExecutor<Void> sequentialExecutor = new AsyncSequentialExecutor<>();

        Supplier<CompletableFuture<Void>> supplier = () -> {
            return CompletableFuture.supplyAsync(() -> {
                i.add(1);
                return null;
            }, executor);
        };

        int N = 10000;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int j = 0; j < N; j++) {
            futures.add(sequentialExecutor.submitTask(supplier));
        }

        FutureUtil.waitForAll(futures).get(5, TimeUnit.SECONDS);

        Assert.assertEquals(N, i.intValue());
    }
}
