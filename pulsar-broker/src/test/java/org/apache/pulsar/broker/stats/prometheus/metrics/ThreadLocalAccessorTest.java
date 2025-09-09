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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ThreadLocalAccessorTest {

    @DataProvider(name = "fastThreadLocalThread")
    public Object[][] booleanProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "fastThreadLocalThread")
    public void testShouldRemoveLocalDataWhenOwnerThreadIsNotAlive(boolean fastThreadLocalThread) throws Exception {
        // given a ThreadLocalAccessor instance
        final var threadLocalAccessor = new DataSketchesOpStatsLogger.ThreadLocalAccessor();
        DoublesUnion aggregateSuccess = DoublesUnion.builder().build();
        DoublesUnion aggregateFail = DoublesUnion.builder().build();
        // using phaser to synchronize threads
        Phaser phaser = new Phaser(2);
        Thread thread = getThread(fastThreadLocalThread, threadLocalAccessor, phaser);
        // sync point #1, wait and advance at the same time
        phaser.arriveAndAwaitAdvance();
        // and record is called
        threadLocalAccessor.record(aggregateSuccess, aggregateFail);
        // then LocalData should exist
        assertEquals(threadLocalAccessor.map.keySet().size(), 1);

        // when thread is not alive anymore
        // sync point #2, wait and advance at the same time
        phaser.arriveAndAwaitAdvance();
        // wait for thread to finish
        thread.join();
        // and record is called
        threadLocalAccessor.record(aggregateSuccess, aggregateFail);
        // then LocalData should be removed
        assertEquals(threadLocalAccessor.map.keySet().size(), 0);
    }

    @Test(dataProvider = "fastThreadLocalThread")
    public void testThreadGc(boolean fastThreadLocalThread) throws Exception {
        final var accessor = createAccessor(fastThreadLocalThread);
        System.gc();
        // FastThreadLocalThread removes the LocalData from the map when the thread finishes
        assertEquals(accessor.map.keySet().size(), fastThreadLocalThread ? 0 : 1);
        accessor.record(DoublesUnion.builder().build(), DoublesUnion.builder().build());
        assertEquals(accessor.map.keySet().size(), 0);
    }

    private static Thread getThread(boolean fastThreadLocalThread,
                                    DataSketchesOpStatsLogger.ThreadLocalAccessor threadLocalAccessor,
                                    Phaser phaser) {
        Runnable runnable = () -> {
            // Create a new LocalData instance for the current thread.
            threadLocalAccessor.localData.get();
            // sync point #1, wait and advance at the same time
            phaser.arriveAndAwaitAdvance();
            // sync point #2, wait and advance at the same time
            phaser.arriveAndAwaitAdvance();
        };
        Thread thread = fastThreadLocalThread ? new FastThreadLocalThread(runnable) : new Thread(runnable);

        // when LocalData is created
        thread.start();
        return thread;
    }

    private static DataSketchesOpStatsLogger.ThreadLocalAccessor createAccessor(boolean fastThreadLocalThread)
            throws Exception {
        final var accessor = new DataSketchesOpStatsLogger.ThreadLocalAccessor();
        final var thread = fastThreadLocalThread ? new FastThreadLocalThread(accessor.localData::get) :
                new Thread(accessor.localData::get);
        thread.start();
        thread.join();
        return accessor;
    }
}
