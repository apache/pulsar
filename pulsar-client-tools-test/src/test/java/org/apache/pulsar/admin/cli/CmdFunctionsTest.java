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


import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunctionStatus;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.RestartFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.StateGetter;
import org.apache.pulsar.admin.cli.CmdFunctions.StopFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link CmdFunctions}.
 */
@Slf4j
@PrepareForTest({ CmdFunctions.class, Reflections.class, StorageClientBuilder.class, FunctionCommon.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class CmdFunctionsTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TEST_NAME = "test_name";
    private static final String JAR_NAME = CmdFunctionsTest.class.getClassLoader().getResource("dummyexamples.jar").getFile();
    private static final String GO_EXEC_FILE_NAME = "test-go-function-with-url";
    private static final String PYTHON_FILE_NAME = "test-go-function-with-url";
    private static final String URL = "file:" + JAR_NAME;
    private static final String URL_WITH_GO = "file:" + GO_EXEC_FILE_NAME;
    private static final String URL_WITH_PY = "file:" + PYTHON_FILE_NAME;
    private static final String FN_NAME = TEST_NAME + "-function";
    private static final String INPUT_TOPIC_NAME = TEST_NAME + "-input-topic";
    private static final String OUTPUT_TOPIC_NAME = TEST_NAME + "-output-topic";
    private static final String TENANT = TEST_NAME + "-tenant";
    private static final String NAMESPACE = TEST_NAME + "-namespace";
    private static final String PACKAGE_URL = "function://sample/ns1/jardummyexamples@1";
    private static final String PACKAGE_GO_URL = "function://sample/ns1/godummyexamples@1";
    private static final String PACKAGE_PY_URL = "function://sample/ns1/pydummyexamples@1";
    private static final String PACKAGE_INVALID_URL = "functionsample.jar";

    private PulsarAdmin admin;
    private Functions functions;
    private CmdFunctions cmd;
    private CmdSinks cmdSinks;
    private CmdSources cmdSources;

    public static class DummyFunction implements Function<String, String> {

        public DummyFunction() {

        }

        @Override
        public String process(String input, Context context) throws Exception {
            return null;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.admin = mock(PulsarAdmin.class);
        this.functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.getServiceUrl()).thenReturn("http://localhost:1234");
        this.cmd = new CmdFunctions(() -> admin);
        this.cmdSinks = new CmdSinks(() -> admin);
        this.cmdSources = new CmdSources(() -> admin);

        // mock reflections
        mockStatic(Reflections.class);
        when(Reflections.classExistsInJar(any(File.class), anyString())).thenReturn(true);
        when(Reflections.classExists(anyString())).thenReturn(true);
        when(Reflections.classInJarImplementsIface(any(File.class), anyString(), eq(Function.class)))
            .thenReturn(true);
        when(Reflections.classImplementsIface(anyString(), any())).thenReturn(true);
        when(Reflections.createInstance(eq(DummyFunction.class.getName()), any(File.class))).thenReturn(new DummyFunction());
    }

//    @Test
//    public void testLocalRunnerCmdNoArguments() throws Exception {
//        cmd.run(new String[] { "run" });
//
//        LocalRunner runner = cmd.getLocalRunner();
//        assertNull(runner.getFunctionName());
//        assertNull(runner.getInputs());
//        assertNull(runner.getOutput());
//        assertNull(runner.getFnConfigFile());
//    }

    /*
    TODO(sijie):- Can we fix this?
    @Test
    public void testLocalRunnerCmdSettings() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = TEST_NAME + "-source-topic";
        String output = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
            "localrun",
            "--name", fnName,
            "--source-topics", sourceTopicName,
            "--output", output
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertEquals(fnName, runner.getFunctionName());
        assertEquals(sourceTopicName, runner.getInputs());
        assertEquals(output, runner.getOutput());
        assertNull(runner.getFnConfigFile());
    }

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
        assertNull(runner.getInputs());
        assertNull(runner.getOutput());
        assertEquals(configFile, runner.getFnConfigFile());
    }
    */

    @Test
    public void testCreateFunction() throws Exception {
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
            "--jar", JAR_NAME,
            "--auto-ack", "false",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
            "--dead-letter-topic", "test-dead-letter-topic",
            "--custom-runtime-options", "custom-runtime-options"
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(Boolean.FALSE, creater.getAutoAck());
        assertEquals("test-dead-letter-topic", creater.getDeadLetterTopic());
        assertEquals("custom-runtime-options", creater.getCustomRuntimeOptions());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void restartFunction() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(FN_NAME, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, FN_NAME, instanceId);
    }

    @Test
    public void restartFunctionInstances() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(FN_NAME, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, FN_NAME);
    }

    @Test
    public void stopFunction() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        StopFunction stop = cmd.getStopper();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, FN_NAME, instanceId);
    }

    @Test
    public void stopFunctionInstances() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        StopFunction stop = cmd.getStopper();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, FN_NAME);
    }

    @Test
    public void startFunction() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, FN_NAME, instanceId);
    }

    @Test
    public void startFunctionInstances() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, FN_NAME);
    }


    @Test
    public void testGetFunctionStatus() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "getstatus", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        GetFunctionStatus status = cmd.getStatuser();
        assertEquals(FN_NAME, status.getFunctionName());

        verify(functions, times(1)).getFunctionStatus(tenant, namespace, FN_NAME, instanceId);
    }

    @Test
    public void testCreateFunctionWithFileUrl() throws Exception {
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
            "--jar", URL,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateGoFunctionWithFileUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", "test-go-function",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--go", URL_WITH_GO,
                "--tenant", "sample",
                "--namespace", "ns1",
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("test-go-function", creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreatePyFunctionWithFileUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", "test-py-function",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--py", URL_WITH_PY,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", "process_python_function",
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("test-py-function", creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithPackageUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", PACKAGE_URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateGoFunctionWithPackageUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", "test-go-function",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--go", PACKAGE_GO_URL,
                "--tenant", "sample",
                "--namespace", "ns1",
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("test-go-function", creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreatePyFunctionWithPackageUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", "test-py-function",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--py", PACKAGE_PY_URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", "process_python_function",
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("test-py-function", creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithInvalidPackageUrl() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", PACKAGE_INVALID_URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(0)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithoutBasicArguments() throws Exception {
        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--className", IdentityFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("IdentityFunction", creater.getFunctionConfig().getName());
        assertEquals("public", creater.getFunctionConfig().getTenant());
        assertEquals("default", creater.getFunctionConfig().getNamespace());

        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithTopicPatterns() throws Exception {
        String topicPatterns = "persistent://tenant/ns/topicPattern*";
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--topicsPattern", topicPatterns,
            "--output", OUTPUT_TOPIC_NAME,
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(topicPatterns, creater.getTopicsPattern());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void testCreateUsingFullyQualifiedFunctionName() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        String functionName = "func";
        String fqfn = String.format("%s/%s/%s", tenant, namespace, functionName);

        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--fqfn", fqfn,
                "--jar", JAR_NAME,
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(tenant, creater.getFunctionConfig().getTenant());
        assertEquals(namespace, creater.getFunctionConfig().getNamespace());
        assertEquals(functionName, creater.getFunctionConfig().getName());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateWithoutOutputTopicWithSkipFlag() throws Exception {
        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
                "--jar", JAR_NAME,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertNull(creater.getFunctionConfig().getOutput());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }


    @Test
    public void testCreateWithoutOutputTopic() {

        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
                "--jar", JAR_NAME,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        consoleOutputCapturer.stop();
        assertNull(creater.getFunctionConfig().getOutput());
        assertTrue(consoleOutputCapturer.getStdout().contains("Created successfully"));
    }

    @Test
    public void testGetFunction() throws Exception {
        cmd.run(new String[] {
            "get",
            "--name", FN_NAME,
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        GetFunction getter = cmd.getGetter();
        assertEquals(FN_NAME, getter.getFunctionName());
        assertEquals(TENANT, getter.getTenant());
        assertEquals(NAMESPACE, getter.getNamespace());

        verify(functions, times(1)).getFunction(eq(TENANT), eq(NAMESPACE), eq(FN_NAME));
    }

    @Test
    public void testDeleteFunction() throws Exception {
        cmd.run(new String[] {
            "delete",
            "--name", FN_NAME,
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        DeleteFunction deleter = cmd.getDeleter();
        assertEquals(FN_NAME, deleter.getFunctionName());
        assertEquals(TENANT, deleter.getTenant());
        assertEquals(NAMESPACE, deleter.getNamespace());

        verify(functions, times(1)).deleteFunction(eq(TENANT), eq(NAMESPACE), eq(FN_NAME));
    }

    @Test
    public void testUpdateFunction() throws Exception {
        cmd.run(new String[] {
            "update",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString(), eq(new UpdateOptionsImpl()));
    }

    @Test
    public void testListFunctions() throws Exception {
        cmd.run(new String[] {
            "list",
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        ListFunctions lister = cmd.getLister();
        assertEquals(TENANT, lister.getTenant());
        assertEquals(NAMESPACE, lister.getNamespace());

        verify(functions, times(1)).getFunctions(eq(TENANT), eq(NAMESPACE));
    }

    @Test
    public void testStateGetter() throws Exception {
        String key = TEST_NAME + "-key";

        cmd.run(new String[] {
            "querystate",
            "--tenant", TENANT,
            "--namespace", NAMESPACE,
            "--name", FN_NAME,
            "--key", key
        });

        StateGetter stateGetter = cmd.getStateGetter();

        assertEquals(TENANT, stateGetter.getTenant());
        assertEquals(NAMESPACE, stateGetter.getNamespace());
        assertEquals(FN_NAME, stateGetter.getFunctionName());

        verify(functions, times(1)).getFunctionState(eq(TENANT), eq(NAMESPACE), eq(FN_NAME), eq(key));
    }

    @Test
    public void testStateGetterWithoutKey() throws Exception {
        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();
        cmd.run(new String[] {
                "querystate",
                "--tenant", TENANT,
                "--namespace", NAMESPACE,
                "--name", FN_NAME,
        });
        consoleOutputCapturer.stop();
        String output = consoleOutputCapturer.getStderr();
        assertTrue(output.replace("\n", "").contains("State key needs to be specified"));
        StateGetter stateGetter = cmd.getStateGetter();
        assertEquals(TENANT, stateGetter.getTenant());
        assertEquals(NAMESPACE, stateGetter.getNamespace());
        assertEquals(FN_NAME, stateGetter.getFunctionName());
        verify(functions, times(0)).getFunctionState(any(), any(), any(), any());
    }

    @Test
    public void testCreateFunctionWithCpu() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 5.0, 0);
        // Disk/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithRam() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(5656565656L));
        // cpu/disk should be default
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0, 0);
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithDisk() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0, 0);
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }


    @Test
    public void testUpdateFunctionWithCpu() throws Exception {
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 5.0, 0);
        // Disk/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptionsImpl()));
    }

    @Test
    public void testUpdateFunctionWithRam() throws Exception {
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(5656565656L));
        // cpu/disk should be default
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0, 0);
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptionsImpl()));
    }

    @Test
    public void testUpdateFunctionWithDisk() throws Exception {
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0, 0);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptionsImpl()));
    }

    @Test
    public void testUpdateAuthData() throws Exception {
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080",
                "--update-auth-data"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0, 0);
        UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(true);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(updateOptions));
    }


    public static class ConsoleOutputCapturer {
        private ByteArrayOutputStream stdout;
        private ByteArrayOutputStream stderr;
        private PrintStream previous;
        private boolean capturing;

        public void start() {
            if (capturing) {
                return;
            }

            capturing = true;
            previous = System.out;
            stdout = new ByteArrayOutputStream();
            stderr = new ByteArrayOutputStream();

            OutputStream outputStreamCombinerstdout =
                    new OutputStreamCombiner(Arrays.asList(previous, stdout));
            PrintStream stdoutStream = new PrintStream(outputStreamCombinerstdout);

            OutputStream outputStreamCombinerStderr =
                    new OutputStreamCombiner(Arrays.asList(previous, stderr));
            PrintStream stderrStream = new PrintStream(outputStreamCombinerStderr);

            System.setOut(stdoutStream);
            System.setErr(stderrStream);
        }

        public void stop() {
            if (!capturing) {
                return;
            }

            System.setOut(previous);

            previous = null;
            capturing = false;
        }

        public String getStdout() {
            return stdout.toString();
        }

        public String getStderr() {
            return stderr.toString();
        }

        private static class OutputStreamCombiner extends OutputStream {
            private List<OutputStream> outputStreams;

            public OutputStreamCombiner(List<OutputStream> outputStreams) {
                this.outputStreams = outputStreams;
            }

            public void write(int b) throws IOException {
                for (OutputStream os : outputStreams) {
                    os.write(b);
                }
            }

            public void flush() throws IOException {
                for (OutputStream os : outputStreams) {
                    os.flush();
                }
            }

            public void close() throws IOException {
                for (OutputStream os : outputStreams) {
                    os.close();
                }
            }
        }
    }
}
