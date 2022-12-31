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
package org.apache.pulsar.common.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jctools.maps.NonBlockingHashMapLong;

public class ThreadMonitor {
    @Data
    @AllArgsConstructor
    public static class ThreadMonitorState {
        private volatile long lastActiveTimestamp;
        private volatile boolean runningTask;
        private String name;
    }

    public static final NonBlockingHashMapLong<ThreadMonitorState> THREAD_ID_TO_STATE =
            new NonBlockingHashMapLong<>(4096, false);

    public static final String THREAD_ACTIVE_TIMESTAMP_GAUGE_NAME = "pulsar_thread_last_active_timestamp_ms";

    public static class Ping implements Runnable {
        @Override
        public void run() {
            refreshThreadState(Thread.currentThread(), true);
        }
    }

    public static void refreshThreadState(Thread t, boolean runningTask) {
        if (!ThreadPoolMonitor.isEnabled()) {
            return;
        }

        long id = t.getId();

        ThreadMonitorState threadMonitorState = THREAD_ID_TO_STATE.get(id);
        if (threadMonitorState != null) {
            threadMonitorState.lastActiveTimestamp = System.currentTimeMillis();
            threadMonitorState.runningTask = runningTask;
            return;
        }

        THREAD_ID_TO_STATE.putIfAbsent(id,
                new ThreadMonitorState(System.currentTimeMillis(), runningTask, t.getName()));
    }

    public static void remove(Thread t) {
        long id = t.getId();
        THREAD_ID_TO_STATE.remove(id);
    }

    public static Runnable ping() {
        return new Ping();
    }
}
