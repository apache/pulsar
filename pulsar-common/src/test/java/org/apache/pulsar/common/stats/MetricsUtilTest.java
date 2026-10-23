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
package org.apache.pulsar.common.stats;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class MetricsUtilTest {

    @Test
    public void testConvertToSeconds() {
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.HOURS)).isEqualTo(3600.0);
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.MINUTES)).isEqualTo(60.0);
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.SECONDS)).isEqualTo(1.0);
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.MILLISECONDS)).isEqualTo(0.001);
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.MICROSECONDS)).isEqualTo(0.000_001);
        assertThat(MetricsUtil.convertToSeconds(1, TimeUnit.NANOSECONDS)).isEqualTo(0.000_000_001);
    }

}
