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
package org.apache.pulsar.tests.performance.tools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.testng.annotations.Test;

public class IotScenarioTest {
    @Test
    public void calculatesRateLimitedWarmupAndMeasurementCounts() {
        IotScenario scenario = scenario(20, 0, 1000, 0);

        assertThat(scenario.warmupMessageCount()).isEqualTo(20_000);
        assertThat(scenario.measurementMessageCount()).isEqualTo(120_000);
        assertThat(scenario.messageCount()).isEqualTo(140_000);
    }

    @Test
    public void calculatesUnrestrictedWarmupAndMeasurementCounts() {
        IotScenario scenario = scenario(0, 1_000_000, 0, 5_000_000);

        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000_000);
        assertThat(scenario.measurementMessageCount()).isEqualTo(5_000_000);
        assertThat(scenario.messageCount()).isEqualTo(6_000_000);
    }

    @Test
    public void calculatesMultipleWarmupRounds() {
        IotScenario scenario = scenario(0, 500_000, 2, 5, 0, 5_000_000);

        assertThat(scenario.warmupMessageCountPerRound()).isEqualTo(500_000);
        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000_000);
        assertThat(scenario.messageCount()).isEqualTo(6_000_000);
        assertThat(scenario.warmupRoundDelaySeconds()).isEqualTo(5);
    }

    @Test
    public void defaultsMissingWarmupRoundsToOne() {
        IotScenario scenario = scenario(0, 1_000, 0, 0, 0, 5_000);

        assertThat(scenario.warmupRounds()).isEqualTo(1);
        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000);
    }

    @Test
    public void rejectsTimeBasedWarmupWithoutRateLimit() {
        assertThatThrownBy(() -> scenario(20, 0, 0, 5_000_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsTwoWarmupLimits() {
        assertThatThrownBy(() -> scenario(20, 1_000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static IotScenario scenario(int warmupSeconds, long warmupMessages, int rate, long numberOfMessages) {
        return scenario(warmupSeconds, warmupMessages, 1, 0, rate, numberOfMessages);
    }

    private static IotScenario scenario(int warmupSeconds, long warmupMessages, int warmupRounds,
                                        int warmupRoundDelaySeconds, int rate, long numberOfMessages) {
        return new IotScenario("pulsar://localhost:6650", "persistent://public/default/iot-", "app-",
                120, warmupSeconds, warmupMessages, warmupRounds, warmupRoundDelaySeconds,
                rate, numberOfMessages, 64, 1_000, 10, 2, 1, 2,
                2, 2, 100, true, true, 300, 0, 0);
    }
}
