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
import org.testng.annotations.Test;

public class TelemetryMessageTest {
    @Test
    public void preservesMeasurementMarker() {
        TelemetryMessage.Decoded warmup = TelemetryMessage.decode(
                TelemetryMessage.encode(3, 7, false, TelemetryMessage.HEADER_BYTES));
        TelemetryMessage.Decoded measurement = TelemetryMessage.decode(
                TelemetryMessage.encode(3, 8, true, TelemetryMessage.HEADER_BYTES + 10));

        assertThat(warmup.deviceId()).isEqualTo(3);
        assertThat(warmup.sequence()).isEqualTo(7);
        assertThat(warmup.measurement()).isFalse();
        assertThat(measurement.deviceId()).isEqualTo(3);
        assertThat(measurement.sequence()).isEqualTo(8);
        assertThat(measurement.measurement()).isTrue();
    }
}
