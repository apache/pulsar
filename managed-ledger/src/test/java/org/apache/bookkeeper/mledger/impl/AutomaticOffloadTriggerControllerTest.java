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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class AutomaticOffloadTriggerControllerTest {

    @Test
    public void triggersCoalesceWhileRunIsActive() {
        AutomaticOffloadTriggerController controller = new AutomaticOffloadTriggerController();

        assertThat(controller.requestRun()).isTrue();
        assertThat(controller.requestRun()).isFalse();
        assertThat(controller.requestRun()).isFalse();
    }

    @Test
    public void pendingTriggerSchedulesOneFollowUpRun() {
        AutomaticOffloadTriggerController controller = new AutomaticOffloadTriggerController();

        assertThat(controller.requestRun()).isTrue();
        assertThat(controller.requestRun()).isFalse();

        assertThat(controller.completeRun()).isTrue();
        assertThat(controller.completeRun()).isFalse();
        assertThat(controller.requestRun()).isTrue();
    }

    @Test(timeOut = 30000)
    public void concurrentTriggerAndCompletionAlwaysReserveOneFollowUpRun() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 1000; i++) {
                AutomaticOffloadTriggerController controller = new AutomaticOffloadTriggerController();
                assertThat(controller.requestRun()).isTrue();

                // Completion and a new trigger can race; exactly one side must reserve the follow-up run.
                CyclicBarrier barrier = new CyclicBarrier(3);
                Future<Boolean> completeResult = executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return controller.completeRun();
                });
                Future<Boolean> triggerResult = executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return controller.requestRun();
                });

                barrier.await(5, TimeUnit.SECONDS);
                boolean followUpReservedByComplete = completeResult.get(5, TimeUnit.SECONDS);
                boolean followUpReservedByTrigger = triggerResult.get(5, TimeUnit.SECONDS);

                assertThat(followUpReservedByComplete)
                        .as("iteration %s must reserve exactly one follow-up run", i)
                        .isNotEqualTo(followUpReservedByTrigger);
                assertThat(controller.completeRun()).isFalse();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
