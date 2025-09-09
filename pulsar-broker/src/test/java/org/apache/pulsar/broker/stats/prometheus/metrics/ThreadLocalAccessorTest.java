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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import static org.testng.Assert.assertEquals;
import com.yahoo.sketches.quantiles.DoublesUnion;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.Phaser;
import org.jspecify.annotations.Nullable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ThreadLocalAccessorTest {

    @DataProvider
    public static Object[][] provider() {
        return new Object[][] {
                // 1st element: whether the thread is a FastThreadLocalThread
                // 2nd element: the 2nd argument passed to the `ThreadLocalAccessor#record` method
                { true, DoublesUnion.builder().build() },
                { true, null },
                { false, DoublesUnion.builder().build() },
                { false, null },
        };
    }

    @Test(dataProvider = "provider")
    public void testShouldRemoveLocalDataWhenOwnerThreadIsNotAlive(
            boolean fastThreadLocalThread, @Nullable DoublesUnion aggregateFail) throws Exception {
        // given a ThreadLocalAccessor instance
        final var threadLocalAccessor = new ThreadLocalAccessor();
        DoublesUnion aggregateSuccess = DoublesUnion.builder().build();
        // using phaser to synchronize threads
        Phaser phaser = new Phaser(2);
        Thread thread = getThread(fastThreadLocalThread, () -> {
            // Create a new LocalData instance for the current thread.
            threadLocalAccessor.getLocalData();
            // sync point #1, wait and advance at the same time
            phaser.arriveAndAwaitAdvance();
            // sync point #2, wait and advance at the same time
            phaser.arriveAndAwaitAdvance();
        });
        // sync point #1, wait and advance at the same time
        phaser.arriveAndAwaitAdvance();
        // and record is called
        threadLocalAccessor.record(aggregateSuccess, aggregateFail);
        // then LocalData should exist
        assertEquals(threadLocalAccessor.getLocalDataCount(), 1);

        // when thread is not alive anymore
        // sync point #2, wait and advance at the same time
        phaser.arriveAndAwaitAdvance();
        // wait for thread to finish
        thread.join();
        // and record is called
        threadLocalAccessor.record(aggregateSuccess, aggregateFail);
        // then LocalData should be removed
        assertEquals(threadLocalAccessor.getLocalDataCount(), 0);
    }

    @Test(dataProvider = "provider")
    public void testThreadGc(boolean fastThreadLocalThread, @Nullable DoublesUnion aggregateFail) throws Exception {
        final var accessor = new ThreadLocalAccessor();
        getThread(fastThreadLocalThread, accessor::getLocalData).join();
        System.gc();
        // FastThreadLocalThread removes the LocalData from the map when the thread finishes
        assertEquals(accessor.getLocalDataCount(), fastThreadLocalThread ? 0 : 1);
        accessor.record(DoublesUnion.builder().build(), aggregateFail);
        assertEquals(accessor.getLocalDataCount(), 0);
    }

    private static Thread getThread(boolean fastThreadLocalThread, Runnable runnable) {
        final var thread = fastThreadLocalThread ? new FastThreadLocalThread(runnable) : new Thread(runnable);
        thread.start(); // when LocalData is created
        return thread;
    }
}
