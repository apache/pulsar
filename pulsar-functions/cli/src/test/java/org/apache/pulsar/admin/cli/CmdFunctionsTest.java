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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.io.File;
import java.net.URI;
import java.net.URL;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.LocalRunner;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarFunctionsAdmin;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.api.utils.Utf8StringSerDe;
import org.apache.pulsar.functions.utils.Reflections;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link CmdFunctions}.
 */
@PrepareForTest({ CmdFunctions.class, Reflections.class })
@PowerMockIgnore("javax.management.*")
public class CmdFunctionsTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TEST_NAME = "test_name";

    private PulsarFunctionsAdmin admin;
    private Functions functions;
    private CmdFunctions cmd;

    @BeforeMethod
    public void setup() throws Exception {
        this.admin = mock(PulsarFunctionsAdmin.class);
        this.functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.getServiceUrl()).thenReturn(URI.create("http://localhost:1234").toURL());
        when(admin.getClientConf()).thenReturn(new ClientConfiguration());
        this.cmd = new CmdFunctions(admin);

        // mock reflections
        mockStatic(Reflections.class);
        when(Reflections.classExistsInJar(any(File.class), anyString())).thenReturn(true);
        when(Reflections.classExists(anyString())).thenReturn(true);
        when(Reflections.classInJarImplementsIface(any(File.class), anyString(), eq(PulsarFunction.class)))
            .thenReturn(true);
        when(Reflections.classImplementsIface(anyString(), any())).thenReturn(true);
    }

    @Test
    public void testLocalRunnerCmdNoArguments() throws Exception {
        cmd.run(new String[] { "run" });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getFunctionName());
        assertNull(runner.getSourceTopicNames());
        assertNull(runner.getSinkTopicName());
        assertNull(runner.getFnConfigFile());
    }

    @Test
    public void testLocalRunnerCmdSettings() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
            "localrun",
            "--function-name", fnName,
            "--source-topics", sourceTopicName,
            "--sink-topic", sinkTopicName
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertEquals(fnName, runner.getFunctionName());
        assertEquals(sourceTopicName, runner.getSourceTopicNames());
        assertEquals(sinkTopicName, runner.getSinkTopicName());
        assertNull(runner.getFnConfigFile());
    }

    /*
    TODO(sijie):- Can we fix this?
    @Test
    public void testLocalRunnerCmdYaml() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_function_config.yml");
        String configFile = yamlUrl.getPath();
        cmd.run(new String[] {
            "localrun",
            "--function-config", configFile
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getFunctionName());
        assertNull(runner.getSourceTopicNames());
        assertNull(runner.getSinkTopicName());
        assertEquals(configFile, runner.getFnConfigFile());
    }
    */

    @Test
    public void testCreateFunction() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
            "create",
            "--function-name", fnName,
            "--source-topics", sourceTopicName,
            "--sink-topic", sinkTopicName,
            "--input-serde-classnames", Utf8StringSerDe.class.getName(),
            "--output-serde-classname", Utf8StringSerDe.class.getName(),
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--function-classname", "MyClass",
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(sourceTopicName, creater.getSourceTopicNames());
        assertEquals(sinkTopicName, creater.getSinkTopicName());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void testGetFunction() throws Exception {
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";

        cmd.run(new String[] {
            "get",
            "--function-name", fnName,
            "--tenant", tenant,
            "--namespace", namespace
        });

        GetFunction getter = cmd.getGetter();
        assertEquals(fnName, getter.getFunctionName());
        assertEquals(tenant, getter.getTenant());
        assertEquals(namespace, getter.getNamespace());

        verify(functions, times(1)).getFunction(eq(tenant), eq(namespace), eq(fnName));
    }

    @Test
    public void testDeleteFunction() throws Exception {
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";

        cmd.run(new String[] {
            "delete",
            "--function-name", fnName,
            "--tenant", tenant,
            "--namespace", namespace
        });

        DeleteFunction deleter = cmd.getDeleter();
        assertEquals(fnName, deleter.getFunctionName());
        assertEquals(tenant, deleter.getTenant());
        assertEquals(namespace, deleter.getNamespace());

        verify(functions, times(1)).deleteFunction(eq(tenant), eq(namespace), eq(fnName));
    }

    @Test
    public void testUpdateFunction() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";



        cmd.run(new String[] {
            "update",
            "--function-name", fnName,
            "--source-topics", sourceTopicName,
            "--sink-topic", sinkTopicName,
            "--input-serde-classnames", Utf8StringSerDe.class.getName(),
            "--output-serde-classname", Utf8StringSerDe.class.getName(),
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--function-classname", "MyClass",
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(sourceTopicName, updater.getSourceTopicNames());
        assertEquals(sinkTopicName, updater.getSinkTopicName());

        verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testListFunctions() throws Exception {
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";

        cmd.run(new String[] {
            "list",
            "--tenant", tenant,
            "--namespace", namespace
        });

        ListFunctions lister = cmd.getLister();
        assertEquals(tenant, lister.getTenant());
        assertEquals(namespace, lister.getNamespace());

        verify(functions, times(1)).getFunctions(eq(tenant), eq(namespace));
    }
}
