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
import org.testng.annotations.Test;
import picocli.CommandLine;

public class LauncherOptionsTest {
    private static PerformanceLauncher parse(String... arguments) {
        PerformanceLauncher launcher = new PerformanceLauncher();
        new CommandLine(launcher).parseArgs(arguments);
        return launcher;
    }

    @Test
    public void deletesTheLauncherLogOfASuccessfulRunUnlessToldToKeepIt() {
        assertThat(parse("--scenario", "iot-telemetry.yaml").keepLauncherLog).isFalse();
        assertThat(parse("--scenario", "iot-telemetry.yaml", "--keep-launcher-log").keepLauncherLog).isTrue();
    }
}
