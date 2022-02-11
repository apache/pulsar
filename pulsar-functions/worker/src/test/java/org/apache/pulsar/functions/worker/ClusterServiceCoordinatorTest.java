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
package org.apache.pulsar.functions.worker;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.pulsar.functions.worker.executor.MockExecutorController;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ClusterServiceCoordinator}.
 */
public class ClusterServiceCoordinatorTest {

    private LeaderService leaderService;
    private ClusterServiceCoordinator coordinator;
    private ScheduledExecutorService mockExecutor;
    private MockExecutorController mockExecutorController;
    private Supplier<Boolean> checkIsStillLeader;

    @BeforeMethod
    public void setup() throws Exception {

        this.mockExecutor = mock(ScheduledExecutorService.class);
        this.mockExecutorController = new MockExecutorController()
            .controlScheduleAtFixedRate(mockExecutor, 10);

        this.leaderService = mock(LeaderService.class);
        this.checkIsStillLeader = () -> leaderService.isLeader();
        this.coordinator = new ClusterServiceCoordinator("test-coordinator", leaderService, checkIsStillLeader);
        Whitebox.setInternalState(coordinator, "executor", mockExecutor);
    }


    @AfterMethod(alwaysRun = true)
    public void teardown() {
        coordinator.close();

        verify(mockExecutor, times(1)).shutdown();
    }

    @Test
    public void testRunTask() {
        Runnable mockTask = mock(Runnable.class);
        long interval = 100;

        coordinator.addTask("mock-task", interval, mockTask);

        coordinator.start();
        verify(mockExecutor, times(1))
            .scheduleAtFixedRate(any(Runnable.class), eq(interval), eq(interval), eq(TimeUnit.MILLISECONDS));

        // when task is executed, it is the leader
        when(leaderService.isLeader()).thenReturn(true);
        mockExecutorController.advance(Duration.ofMillis(interval));

        verify(leaderService, times(1)).isLeader();
        verify(mockTask, times(1)).run();

        // when task is executed, it is not the leader
        when(leaderService.isLeader()).thenReturn(false);
        mockExecutorController.advance(Duration.ofMillis(interval));

        // `isLeader` is called twice, however the task is only executed once (when it was leader)
        verify(leaderService, times(2)).isLeader();
        verify(mockTask, times(1)).run();
    }

}
