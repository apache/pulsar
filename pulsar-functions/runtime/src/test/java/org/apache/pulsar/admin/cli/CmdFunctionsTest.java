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

package org.apache.pulsar.admin.cli;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.net.URL;
import org.apache.pulsar.admin.cli.CmdFunctions.LocalRunner;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link CmdFunctions}.
 */
public class CmdFunctionsTest {

    @Rule
    public final TestName runtime = new TestName();

    @Test
    public void testLocalRunnerCmdNoArguments() throws Exception {
        PulsarAdmin admin = mock(PulsarAdmin.class);
        CmdFunctions cmd = new CmdFunctions(admin);
        cmd.run(new String[] { "run" });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getName());
        assertNull(runner.getSourceTopicName());
        assertNull(runner.getSinkTopicName());
        assertNull(runner.getFnConfigFile());
    }

    @Test
    public void testLocalRunnerCmdSettings() throws Exception {
        PulsarAdmin admin = mock(PulsarAdmin.class);
        CmdFunctions cmd = new CmdFunctions(admin);
        String fnName = runtime.getMethodName() + "-function";
        String sourceTopicName = runtime.getMethodName() + "-source-topic";
        String sinkTopicName = runtime.getMethodName() + "-sink-topic";
        cmd.run(new String[] {
            "localrun",
            "--name", fnName,
            "--source-topic", sourceTopicName,
            "--sink-topic", sinkTopicName
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertEquals(fnName, runner.getName());
        assertEquals(sourceTopicName, runner.getSourceTopicName());
        assertEquals(sinkTopicName, runner.getSinkTopicName());
        assertNull(runner.getFnConfigFile());
    }

    @Test
    public void testLocalRunnerCmdYaml() throws Exception {
        PulsarAdmin admin = mock(PulsarAdmin.class);
        CmdFunctions cmd = new CmdFunctions(admin);
        URL yamlUrl = getClass().getClassLoader().getResource("test_function_config.yml");
        String configFile = yamlUrl.getPath();
        cmd.run(new String[] {
            "localrun",
            "--function-config", configFile
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getName());
        assertNull(runner.getSourceTopicName());
        assertNull(runner.getSinkTopicName());
        assertEquals(configFile, runner.getFnConfigFile());
    }
}
