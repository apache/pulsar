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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private final Timer timer;
    private final LeaderElector leaderElector;

    public ClusterServiceCoordinator(String workerId, String coordinationTopic, PulsarClient client) {
        this.workerId = workerId;
        this.timer = new Timer();
        try {
            this.leaderElector = new LeaderElector(this.workerId, coordinationTopic, client);
        } catch (PulsarClientException e) {
            log.error("Error creating leader elector", e);
            throw new RuntimeException(e);
        }
    }

    public void addTask(String taskName, long interval, Runnable task) {
        tasks.put(taskName, new TimerTaskInfo(interval, task));
    }

    public void start() {
        for (Map.Entry<String, TimerTaskInfo> entry : this.tasks.entrySet()) {
            TimerTaskInfo timerTaskInfo = entry.getValue();
            String taskName = entry.getKey();
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    boolean isLeader = false;
                    try {
                        isLeader = leaderElector.becomeLeader().get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        log.warn("Failed to attempt becoming leader", e);
                    }

                    if (isLeader) {
                        try {
                            timerTaskInfo.getTask().run();
                        } catch (Exception e) {
                            log.error("Cluster timer task {} failed with exception.", taskName, e);
                        }
                    }
                }
            }, timerTaskInfo.getInterval(), timerTaskInfo.getInterval());
        }
    }

    @Override
    public void close() {
        log.info("Stopping Cluster Service Coordinator for worker {}", this.workerId);
        try {
            this.leaderElector.close();
            this.timer.cancel();
        } catch (PulsarClientException e) {
            log.error("Failed to stop Cluster Service Coordinator for worker {}", this.workerId, e);
        }
        log.info("Stopped Cluster Service Coordinator for worker", this.workerId);
    }
}
