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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessCoordinator {

    private static final AtomicInteger COORDINATOR = new AtomicInteger();

    public static void stop() {
        COORDINATOR.set(Integer.MAX_VALUE);
    }

    public static void start() {
        COORDINATOR.set(0);
    }

    public static void waitAndChangeStep(int toStep){
        if (COORDINATOR.get() >= toStep){
            return;
        }
        while (!COORDINATOR.compareAndSet(toStep - 1, toStep)){
            log.info("Process coordinator {} -> {}", toStep - 1, toStep);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }
    }

    public static int getCurrentStep(){
        return COORDINATOR.get();
    }
}
