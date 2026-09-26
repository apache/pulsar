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
import java.nio.file.Path;
import java.time.ZonedDateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RunDirectoryTest {
    @Test
    public void laysOutRunsByDayBranchAndName() {
        Path run = RunDirectory.resolve(Path.of("/reports"), ZonedDateTime.parse("2026-09-25T06:42:59+03:00"),
                "lh-use-jonoffcpu-profiler", "e232-ab");

        assertThat(run).isEqualTo(Path.of("/reports/2026-09-25/lh-use-jonoffcpu-profiler/e232-ab/09-25-06-42-59"));
    }

    @DataProvider
    public Object[][] branches() {
        return new Object[][] {
                {"lh-use-jonoffcpu-profiler", "abc", "lh-use-jonoffcpu-profiler"},
                {"feat/graceful-broker-shutdown", "abc", "feat-graceful-broker-shutdown"},
                {"HEAD", "0123456789abcdef0123", "detached-0123456789ab"},
                {"", "", "no-git"},
                {"..", "", "run"},
        };
    }

    @Test(dataProvider = "branches")
    public void namesBranchDirectories(String branch, String commit, String expected) {
        assertThat(RunDirectory.branchDirectory(branch, commit)).isEqualTo(expected);
    }
}
