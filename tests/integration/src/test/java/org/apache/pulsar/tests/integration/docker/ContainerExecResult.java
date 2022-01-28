/**
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
package org.apache.pulsar.tests.integration.docker;

import static org.testng.Assert.assertTrue;
import lombok.Data;

/**
 * Represents the result of executing a command.
 */
@Data(staticConstructor = "of")
public class ContainerExecResult {

    private final long exitCode;
    private final String stdout;
    private final String stderr;

    public void assertNoOutput() {
        assertNoStdout();
        assertNoStderr();
    }

    public void assertNoStdout() {
        assertTrue(stdout.isEmpty(),
                "stdout should be empty, but was '" + stdout + "'");
    }

    public void assertNoStderr() {
        assertTrue(stderr.isEmpty(),
                "stderr should be empty, but was '" + stderr + "'");
    }
}
