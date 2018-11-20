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
package org.apache.pulsar.functions.instance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class InstanceCache {

    private static InstanceCache instance;

    public final ScheduledExecutorService executor;

    private InstanceCache() {
        executor = Executors.newSingleThreadScheduledExecutor();;
    }

    public static InstanceCache getInstanceCache() {
        synchronized (InstanceCache.class) {
            if (instance == null) {
                instance = new InstanceCache();
            }
        }
        return instance;
    }

    public static void shutdown() {
        synchronized (InstanceCache.class) {
            if (instance != null) {
                instance.executor.shutdown();
            }
            instance = null;
        }
    }
}
