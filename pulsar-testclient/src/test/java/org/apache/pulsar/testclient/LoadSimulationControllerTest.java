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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadSimulationControllerTest {

    @Test
    public void shouldLeakThreadsWhenExecutorIsNotShutdown() throws Exception {
        // Access the private static field threadPool using reflection
        Field threadPoolField = LoadSimulationController.class.getDeclaredField("threadPool");
        threadPoolField.setAccessible(true);
        ExecutorService threadPool = (ExecutorService) threadPoolField.get(null);

        // Count alive, non-daemon threads BEFORE submitting work
        Map<Thread, StackTraceElement[]> threadsBefore = Thread.getAllStackTraces();
        long countBefore = threadsBefore.keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> !t.isDaemon())
                .count();

        // Submit a task to force thread creation
        threadPool.submit(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Wait to ensure the thread starts
        Thread.sleep(100);

        // Count alive, non-daemon threads AFTER submitting work
        Map<Thread, StackTraceElement[]> threadsAfter = Thread.getAllStackTraces();
        long countAfter = threadsAfter.keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> !t.isDaemon())
                .count();

        // Assert that thread count increased (proving leak)
        assertTrue(countAfter > countBefore,
                String.format("Thread leak detected: thread count increased from %d to %d", countBefore, countAfter));

        // Additional signal: verify at least one thread with executor-style name exists
        boolean foundPoolThread = threadsAfter.keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> !t.isDaemon())
                .anyMatch(t -> t.getName().startsWith("pool-"));
        assertTrue(foundPoolThread, "Found executor thread with pool- prefix");
    }
}

