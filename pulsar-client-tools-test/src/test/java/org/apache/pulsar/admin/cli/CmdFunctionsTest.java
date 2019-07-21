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
import java.util.LinkedList;
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
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.utils.Reflections;
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
        when(admin.getClientConfigData()).thenReturn(new ClientConfigurationData());
        this.cmd = new CmdFunctions(admin);
        this.cmdSinks = new CmdSinks(admin);
        this.cmdSources = new CmdSources(admin);

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
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
            "--jar", JAR_NAME,
            "--auto-ack", "false",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(new Boolean(false), creater.getAutoAck());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void restartFunction() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(fnName, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, fnName, instanceId);
    }

    @Test
    public void restartFunctionInstances() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(fnName, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, fnName);
    }

    @Test
    public void stopFunction() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        StopFunction stop = cmd.getStopper();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, fnName, instanceId);
    }

    @Test
    public void stopFunctionInstances() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        StopFunction stop = cmd.getStopper();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, fnName);
    }

    @Test
    public void startFunction() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, fnName, instanceId);
    }

    @Test
    public void startFunctionInstances() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, fnName);
    }


    @Test
    public void testGetFunctionStatus() throws Exception {
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "getstatus", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        GetFunctionStatus status = cmd.getStatuser();
        assertEquals(fnName, status.getFunctionName());

        verify(functions, times(1)).getFunctionStatus(tenant, namespace, fnName, instanceId);
    }

    @Test
    public void testCreateFunctionWithFileUrl() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
            "--jar", url,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithoutBasicArguments() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--className", IdentityFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("IdentityFunction", creater.getFunctionConfig().getName());
        assertEquals("public", creater.getFunctionConfig().getTenant());
        assertEquals("default", creater.getFunctionConfig().getNamespace());

        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithTopicPatterns() throws Exception {
        String fnName = TEST_NAME + "-function";
        String topicPatterns = "persistent://tenant/ns/topicPattern*";
        String outputTopicName = TEST_NAME + "-output-topic";
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--topicsPattern", topicPatterns,
            "--output", outputTopicName,
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(topicPatterns, creater.getTopicsPattern());
        assertEquals(outputTopicName, creater.getOutput());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void testCreateUsingFullyQualifiedFunctionName() throws Exception {
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";
        String tenant = "sample";
        String namespace = "ns1";
        String functionName = "func";
        String fqfn = String.format("%s/%s/%s", tenant, namespace, functionName);

        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--output", outputTopicName,
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
        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
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

        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
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
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";

        cmd.run(new String[] {
            "get",
            "--name", fnName,
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
            "--name", fnName,
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
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        cmd.run(new String[] {
            "update",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
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

    @Test
    public void testStateGetter() throws Exception {
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";
        String key = TEST_NAME + "-key";

        cmd.run(new String[] {
            "querystate",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", fnName,
            "--key", key
        });

        StateGetter stateGetter = cmd.getStateGetter();

        assertEquals(tenant, stateGetter.getTenant());
        assertEquals(namespace, stateGetter.getNamespace());
        assertEquals(fnName, stateGetter.getFunctionName());

        verify(functions, times(1)).getFunctionState(eq(tenant), eq(namespace), eq(fnName), eq(key));
    }

    private static final String fnName = TEST_NAME + "-function";
    private static final String inputTopicName = TEST_NAME + "-input-topic";
    private static final String outputTopicName = TEST_NAME + "-output-topic";

    private void testValidateFunctionsConfigs(String[] correctArgs, String[] incorrectArgs,
                                              String errMessageCheck) throws Exception {

        String[] cmds = {"create", "update", "localrun"};

        for (String type : cmds) {
            List<String> correctArgList = new LinkedList<>();
            List<String> incorrectArgList = new LinkedList<>();
            correctArgList.add(type);
            incorrectArgList.add(type);

            correctArgList.addAll(Arrays.asList(correctArgs));
            incorrectArgList.addAll(Arrays.asList(incorrectArgs));
            cmd.run(correctArgList.toArray(new String[correctArgList.size()]));

            if (type.equals("create")) {
                CreateFunction creater = cmd.getCreater();
                assertEquals(fnName, creater.getFunctionName());
                assertEquals(inputTopicName, creater.getInputs());
                assertEquals(outputTopicName, creater.getOutput());
            } else if (type.equals("update")){
                UpdateFunction updater = cmd.getUpdater();
                assertEquals(fnName, updater.getFunctionName());
                assertEquals(inputTopicName, updater.getInputs());
                assertEquals(outputTopicName, updater.getOutput());
            } else {
                CmdFunctions.LocalRunner localRunner = cmd.getLocalRunner();
                assertEquals(fnName, localRunner.getFunctionName());
                assertEquals(inputTopicName, localRunner.getInputs());
                assertEquals(outputTopicName, localRunner.getOutput());
            }

            if (type.equals("create")) {
                verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
            } else if (type.equals("update")) {
                verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString());
            }

            setup();
            ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
            consoleOutputCapturer.start();
            cmd.run(incorrectArgList.toArray(new String[incorrectArgList.size()]));

            consoleOutputCapturer.stop();
            String output = consoleOutputCapturer.getStderr();
            assertTrue(output.replace("\n", "").contains(errMessageCheck));
        }
    }

    @Test
    public void testCreateFunctionWithCpu() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithRam() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(5656565656l));
        // cpu/disk should be default
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithDisk() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(8080808080808080l));
        // cpu/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0);
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }


    @Test
    public void testUpdateFunctionWithCpu() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
    }

    @Test
    public void testUpdateFunctionWithRam() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(5656565656l));
        // cpu/disk should be default
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
    }

    @Test
    public void testUpdateFunctionWithDisk() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(8080808080808080l));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
    }

    @Test
    public void testUpdateAuthData() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080",
                "--update-auth-data"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(8080808080808080l));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        UpdateOptions updateOptions = new UpdateOptions();
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
