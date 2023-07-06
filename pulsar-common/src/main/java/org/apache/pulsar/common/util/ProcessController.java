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

import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessController {

    private static AtomicReference<Step> currentStep = new AtomicReference<>();

    public static void waitAndSet(int waitSeconds, Step step) {
        try {
            Thread.sleep(1000 * waitSeconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        compareAndSet(step);
    }

    public static void compareAndSet(Step step, Object...contextsToTrace) {
        Step previous = step.previousStep();
        log.info("step: {}, wait to set to {}", currentStep.get(), step);
        if (currentStep.get() == null) {
            currentStep.set(step);
            return;
        }
        if (currentStep.get().ordinal() >= step.ordinal()) {
            return;
        }
        while (!currentStep.compareAndSet(previous, step)){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info("next try, step: {}, wait to set to {}", currentStep.get(), step);
        }
    }

    public static Step getCurrentStep() {
        return currentStep.get();
    }
}
