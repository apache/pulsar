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
package org.apache.pulsar.testclient;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.testng.annotations.Test;

public class LoadSimulationControllerTest {

    @Test
    public void shouldNotLeakThreadsAfterClose() throws Exception {
        LoadSimulationController controller = new LoadSimulationController();
        Field threadPoolField = LoadSimulationController.class.getDeclaredField("threadPool");
        threadPoolField.setAccessible(true);
        ExecutorService threadPool = (ExecutorService) threadPoolField.get(controller);

        threadPool.submit(() -> {});

        controller.close();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            long poolThreadCount = threads.keySet().stream()
                    .filter(Thread::isAlive)
                    .filter(t -> !t.isDaemon())
                    .filter(t -> t.getName().startsWith("LoadSimulationController"))
                    .count();

            assertTrue(poolThreadCount == 0,
                    String.format("Found %d alive non-daemon LoadSimulationController threads after close",
                            poolThreadCount));
        });

    }
}



