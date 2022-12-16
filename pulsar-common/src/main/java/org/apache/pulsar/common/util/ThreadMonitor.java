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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThreadMonitor {
    public static final ConcurrentMap<Long, Long> THREAD_LAST_ACTIVE_TIMESTAMP =
            new ConcurrentHashMap<>(Runtime.getRuntime().availableProcessors(),
                    0.75f, 1024);

    public static final ConcurrentMap<Long, String> THREAD_ID_TO_NAME =
            new ConcurrentHashMap<>(Runtime.getRuntime().availableProcessors(),
                    0.75f, 1024);

    public static final String THREAD_ACTIVE_TIMESTAMP_GAUGE_NAME = "pulsar_thread_last_active_timestamp_ms";

    public static class Ping implements Runnable {
        @Override
        public void run() {
            Thread t = Thread.currentThread();
            long id = t.getId();
            String name = t.getName();
            THREAD_ID_TO_NAME.put(id, name);
            THREAD_LAST_ACTIVE_TIMESTAMP.put(id, System.currentTimeMillis());
        }
    }

    public static Runnable ping() {
        return new Ping();
    }
}
