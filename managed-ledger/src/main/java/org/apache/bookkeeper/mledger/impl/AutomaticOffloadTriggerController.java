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

/**
 * Coalesces repeated automatic offload triggers into at most one active run and one follow-up run.
 */
final class AutomaticOffloadTriggerController {
    private static final int IDLE = 0;
    private static final int RUNNING = 1;
    private static final int RUNNING_WITH_PENDING_TRIGGER = 2;

    private final AtomicInteger state = new AtomicInteger(IDLE);

    /**
     * Records an automatic offload trigger.
     *
     * @return true when the caller must start a new automatic offload run
     */
    boolean requestRun() {
        while (true) {
            int current = state.get();
            switch (current) {
                case IDLE:
                    if (state.compareAndSet(IDLE, RUNNING)) {
                        return true;
                    }
                    break;
                case RUNNING:
                    if (state.compareAndSet(RUNNING, RUNNING_WITH_PENDING_TRIGGER)) {
                        return false;
                    }
                    break;
                case RUNNING_WITH_PENDING_TRIGGER:
                    return false;
                default:
                    throw new IllegalStateException("Unknown automatic offload trigger state: " + current);
            }
        }
    }

    /**
     * Records completion of the current automatic offload run.
     *
     * @return true when the caller must immediately start one coalesced follow-up run
     */
    boolean completeRun() {
        while (true) {
            int current = state.get();
            switch (current) {
                case IDLE:
                    return false;
                case RUNNING:
                    if (state.compareAndSet(RUNNING, IDLE)) {
                        return false;
                    }
                    break;
                case RUNNING_WITH_PENDING_TRIGGER:
                    if (state.compareAndSet(RUNNING_WITH_PENDING_TRIGGER, RUNNING)) {
                        return true;
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown automatic offload trigger state: " + current);
            }
        }
    }
}
