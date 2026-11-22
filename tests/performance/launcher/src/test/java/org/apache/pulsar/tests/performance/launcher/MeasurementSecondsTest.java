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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class MeasurementSecondsTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void takesTheSecondsWithoutANumberOfMessages() throws Exception {
        assertThat(seconds("{\"rate\": 1000, \"measurement\": {\"seconds\": 120, \"messages\": 0}}"))
                .isEqualTo(120);
    }

    @Test
    public void takesTheMessagesAtTheRateOverTheSeconds() throws Exception {
        // The gateways send the messages whatever the seconds, as the workload counts them
        assertThat(seconds("{\"rate\": 1000, \"measurement\": {\"seconds\": 60, \"messages\": 180000}}"))
                .isEqualTo(180);
    }

    @Test
    public void doesntKnowTheLengthOfMessagesWithoutARate() throws Exception {
        assertThat(seconds("{\"rate\": 0, \"measurement\": {\"seconds\": 60, \"messages\": 180000}}"))
                .isEqualTo(-1);
    }

    private double seconds(String workload) throws Exception {
        return PerformanceLauncher.measurementSeconds(mapper.readTree(workload));
    }
}
