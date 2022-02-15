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
package org.apache.pulsar.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.metadata.impl.ExecutorMonitor;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutorMonitorTest {
    @Test
    public void testExecutorMonitorBasic() throws InterruptedException {
        ExecutorService executor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("metadata-store"));

        Thread t = new Thread(() -> executor.execute(() ->{
            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    continue;
                }
            }
        }));
        t.start();
        Thread.sleep(1000);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        ExecutorMonitor monitor = new ExecutorMonitor(executor,
                1,
                timeout -> {
                    callbackInvoked.set(true);
                });
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertTrue(callbackInvoked.get());
        });
    }
}
