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
package org.apache.pulsar.internal;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

public class CommanderFactoryTest {

    @Test
    public void testEnvironmentVariablesFooterOnUnix() {
        CommandLine commander = new CommandLine(CommandSpec.create());
        CommanderFactory.addEnvironmentVariablesFooter(commander, false);
        assertThat(commander.getUsageMessage()).contains("PULSAR_EXTRA_CLASSPATH", "PULSAR_MEM", "PULSAR_GC",
                "(default: $PULSAR_LOG_LEVEL)", "conf/pulsar_tools_env.sh");
    }

    @Test
    public void testEnvironmentVariablesFooterOnWindows() {
        CommandLine commander = new CommandLine(CommandSpec.create());
        CommanderFactory.addEnvironmentVariablesFooter(commander, true);
        assertThat(commander.getUsageMessage())
                .contains("PULSAR_CLASSPATH", "PULSAR_EXTRA_OPTS", "(default: %PULSAR_LOG_LEVEL%)")
                .doesNotContain("PULSAR_EXTRA_CLASSPATH", "PULSAR_MEM", "PULSAR_GC", "pulsar_tools_env.sh");
    }
}
