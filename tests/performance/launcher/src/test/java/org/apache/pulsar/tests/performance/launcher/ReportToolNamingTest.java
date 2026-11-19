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
import org.apache.pulsar.tests.integration.profiling.JonoffcpuAgent;
import org.apache.pulsar.tests.performance.report.OffCpuFlamegraphs;
import org.testng.annotations.Test;

/** The report tool does not depend on the integration tests, so the file names it shares with them are checked here. */
public class ReportToolNamingTest {
    @Test
    public void reportToolFindsTheAgentsCaptureStream() {
        assertThat(OffCpuFlamegraphs.CAPTURE_SUFFIX).isEqualTo(JonoffcpuAgent.CAPTURE_SUFFIX);
    }
}
