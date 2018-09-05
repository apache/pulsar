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

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunctionStatus;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.RestartFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.StopFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.admin.cli.CmdSinks.CreateSink;
import org.apache.pulsar.admin.cli.CmdSources.CreateSource;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 * Unit test of {@link CmdFunctions}.
 */
@Slf4j
@PrepareForTest({ CmdFunctions.class, Reflections.class, StorageClientBuilder.class, Utils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class CmdFunctionsTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TEST_NAME = "test_name";

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

    private String generateCustomSerdeInputs(String topic, String serde) {
        Map<String, String> map = new HashMap<>();
        map.put(topic, serde);
        return new Gson().toJson(map);
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
        PowerMockito.stub(PowerMockito.method(Utils.class, "fileExists")).toReturn(true);
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
            "--jar", "SomeJar.jar",
            "--auto-ack", "false",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(false, creater.isAutoAck());

        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());

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
    public void testCreateFunctionWithHttpUrl() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

        final String url = "http://localhost:1234/test";
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

        consoleOutputCapturer.stop();
        String output = consoleOutputCapturer.getStderr();

        assertTrue(output.contains("Failed to download jar"));
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
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

        final String url = "file:/usr/temp/myfile.jar";
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
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateSink() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";


        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

        final String url = "http://localhost:1234/test";
        cmdSinks.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--archive", url,
            "--tenant", "sample",
            "--namespace", "ns1"
        });

        CreateSink creater = cmdSinks.getCreateSink();

        consoleOutputCapturer.stop();
        String output = consoleOutputCapturer.getStderr();

        assertTrue(output.contains("Failed to download archive"));
        assertEquals(url, creater.archive);
    }

    @Test
    public void testCreateSource() throws Exception {
        String fnName = TEST_NAME + "-function";

        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

        final String url = "http://localhost:1234/test";
        cmdSources.run(new String[] {
            "create",
            "--name", fnName,
            "--archive", url,
            "--tenant", "sample",
            "--namespace", "ns1",
        });

        CreateSource creater = cmdSources.getCreateSource();

        consoleOutputCapturer.stop();
        String output = consoleOutputCapturer.getStderr();

        assertTrue(output.contains("Failed to download archive"));
        assertEquals(url, creater.archive);
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
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(topicPatterns, creater.getTopicsPattern());
        assertEquals(outputTopicName, creater.getOutput());

        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());

    }

    @Test
    public void testCreateWithoutTenant() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = "persistent://tenant/standalone/namespace/input-topic";
        String outputTopicName = "persistent://tenant/standalone/namespace/output-topic";
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("public", creater.getFunctionConfig().getTenant());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutNamespace() throws Exception {
        String fnName = TEST_NAME + "-function";
        String inputTopicName = "persistent://tenant/standalone/namespace/input-topic";
        String outputTopicName = "persistent://tenant/standalone/namespace/output-topic";
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("public", creater.getFunctionConfig().getTenant());
        assertEquals("default", creater.getFunctionConfig().getNamespace());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
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
                "--jar", "SomeJar.jar",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(tenant, creater.getFunctionConfig().getTenant());
        assertEquals(namespace, creater.getFunctionConfig().getNamespace());
        assertEquals(functionName, creater.getFunctionConfig().getName());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutFunctionName() throws Exception {
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("CmdFunctionsTest$DummyFunction", creater.getFunctionConfig().getName());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutOutputTopicWithSkipFlag() throws Exception {
        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertNull(creater.getFunctionConfig().getOutput());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());

    }

    
    @Test
    public void testCreateWithoutOutputTopic() {

        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--jar", "SomeJar.jar",
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
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionDetails.class), anyString());
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
        String tenant = TEST_NAME + "_tenant";
        String namespace = TEST_NAME + "_namespace";
        String fnName = TEST_NAME + "_function";

        mockStatic(StorageClientBuilder.class);

        StorageClientBuilder builder = mock(StorageClientBuilder.class);
        when(builder.withSettings(any(StorageClientSettings.class))).thenReturn(builder);
        when(builder.withNamespace(eq(tenant + "_" + namespace))).thenReturn(builder);
        StorageClient client = mock(StorageClient.class);
        when(builder.build()).thenReturn(client);

        PowerMockito.when(StorageClientBuilder.class, "newBuilder")
            .thenReturn(builder);

        Table<ByteBuf, ByteBuf> table = mock(Table.class);
        when(client.openTable(eq(fnName))).thenReturn(FutureUtils.value(table));
        AtomicReference<ByteBuf> keyHolder = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgumentAt(0, ByteBuf.class);
            keyHolder.set(buf);
            return FutureUtils.value(null);
        }).when(table).getKv(any(ByteBuf.class));

        cmd.run(new String[] {
            "querystate",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", fnName,
            "--key", "test-key",
            "--storage-service-url", "bk://127.0.0.1:4181"
        });

        assertEquals(
            "test-key",
            new String(ByteBufUtil.getBytes(keyHolder.get()), UTF_8));
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
                verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
            } else if (type.equals("update")) {
                verify(functions, times(1)).updateFunction(any(FunctionDetails.class), anyString());
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
    public void TestCreateFunctionParallelism() throws Exception{

        String[] correctArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--parallelism", "1"
        };

        String[] incorrectArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--parallelism", "-1"
        };

        testValidateFunctionsConfigs(correctArgs, incorrectArgs, "Field 'parallelism' must be a Positive Number");

    }

    @Test
    public void TestCreateTopicName() throws Exception {

        String[] correctArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        };

        String wrongOutputTopicName = TEST_NAME + "-output-topic/test:";
        String[] incorrectArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", wrongOutputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        };

        testValidateFunctionsConfigs(correctArgs, incorrectArgs, "The topic name " + wrongOutputTopicName + " is invalid for field 'output'");
    }

    @Test
    public void TestCreateClassName() throws Exception {

        String[] correctArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        };

        String cannotLoadClass = "com.test.Function";
        String[] incorrectArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", cannotLoadClass,
        };

        testValidateFunctionsConfigs(correctArgs, incorrectArgs, "Cannot find/load class " + cannotLoadClass);
    }

    @Test
    public void TestCreateSameInOutTopic() throws Exception {

        String[] correctArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        };

        String[] incorrectArgs = new String[]{
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", inputTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        };

        testValidateFunctionsConfigs(correctArgs, incorrectArgs,
                "Output topic " + inputTopicName
                        + " is also being used as an input topic (topics must be one or the other)");

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
