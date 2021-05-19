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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class ClusterServiceCoordinator implements AutoCloseable {

    @Getter
    @Setter
    private static class TimerTaskInfo {
        private long interval;
        private Runnable task;

        public TimerTaskInfo(long interval, Runnable task) {
            this.interval = interval;
            this.task = task;
        }
    }

    private final String workerId;
    private final Map<String, TimerTaskInfo> tasks = new HashMap<>();
    private final ScheduledExecutorService executor;
    private final LeaderService leaderService;
    private final Supplier<Boolean> isLeader;

    public ClusterServiceCoordinator(String workerId, LeaderService leaderService, Supplier<Boolean> isLeader) {
        this.workerId = workerId;
        this.leaderService = leaderService;
        this.isLeader = isLeader;
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("cluster-service-coordinator-timer").build());
    }

    public void addTask(String taskName, long interval, Runnable task) {
        tasks.put(taskName, new TimerTaskInfo(interval, task));
    }

    public void start() {
        log.info("/** Starting cluster service coordinator **/");
        for (Map.Entry<String, TimerTaskInfo> entry : this.tasks.entrySet()) {
            TimerTaskInfo timerTaskInfo = entry.getValue();
            String taskName = entry.getKey();
            this.executor.scheduleAtFixedRate(() -> {
                if (isLeader.get()) {
                    try {
                        timerTaskInfo.getTask().run();
                    } catch (Exception e) {
                        log.error("Cluster timer task {} failed with exception.", taskName, e);
                    }
                }
            }, timerTaskInfo.getInterval(), timerTaskInfo.getInterval(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        log.info("Stopping Cluster Service Coordinator for worker {}", this.workerId);
        this.executor.shutdown();
        log.info("Stopped Cluster Service Coordinator for worker {}", this.workerId);
    }
}
