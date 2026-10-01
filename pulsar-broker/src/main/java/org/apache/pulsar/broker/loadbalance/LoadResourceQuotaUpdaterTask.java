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
package org.apache.pulsar.broker.loadbalance;

import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;

/**
 * LoadManager namespace bundle quota update task.
 */
@CustomLog
public class LoadResourceQuotaUpdaterTask implements Runnable {
    private final AtomicReference<LoadManager> loadManager;

    public LoadResourceQuotaUpdaterTask(AtomicReference<LoadManager> loadManager) {
        this.loadManager = loadManager;
    }

    @Override
    public void run() {
        try {
            this.loadManager.get().writeResourceQuotasToZooKeeper();
        } catch (Exception e) {
            log.warn().exception(e).log("Error write resource quota");
        }
    }
}
