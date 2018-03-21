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
package org.apache.pulsar.functions.worker.executor;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test {@link MockExecutorController}.
 */
public class MockExecutorControllerTest {

    private static final int MAX_SCHEDULES = 5;

    private ScheduledExecutorService executor;
    private MockExecutorController mockExecutorControl;

    @BeforeMethod
    public void setup() {
        this.executor = mock(ScheduledExecutorService.class);
        this.mockExecutorControl = new MockExecutorController()
            .controlExecute(executor)
            .controlSubmit(executor)
            .controlSchedule(executor)
            .controlScheduleAtFixedRate(executor, MAX_SCHEDULES);
    }

    @Test
    public void testSubmit() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        executor.submit(task);
        verify(task, times(1)).run();
    }

    @Test
    public void testExecute() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        executor.execute(task);
        verify(task, times(1)).run();
    }

    @Test
    public void testDelay() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        executor.schedule(task, 10, TimeUnit.MILLISECONDS);
        mockExecutorControl.advance(Duration.ofMillis(5));
        verify(task, times(0)).run();
        mockExecutorControl.advance(Duration.ofMillis(10));
        verify(task, times(1)).run();
    }

    @Test
    public void testScheduleAtFixedRate() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        executor.scheduleAtFixedRate(task, 5, 10, TimeUnit.MILLISECONDS);

        // first delay
        mockExecutorControl.advance(Duration.ofMillis(2));
        verify(task, times(0)).run();
        mockExecutorControl.advance(Duration.ofMillis(3));
        verify(task, times(1)).run();

        // subsequent delays
        for (int i = 1; i < MAX_SCHEDULES; i++) {
            mockExecutorControl.advance(Duration.ofMillis(2));
            verify(task, times(i)).run();
            mockExecutorControl.advance(Duration.ofMillis(8));
            verify(task, times(i + 1)).run();
        }

        // no more invocations
        mockExecutorControl.advance(Duration.ofMillis(500));
        verify(task, times(MAX_SCHEDULES)).run();
    }

}
