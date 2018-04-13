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
import static org.testng.Assert.assertNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.LocalRunner;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.shaded.io.netty.buffer.ByteBuf;
import org.apache.pulsar.functions.shaded.io.netty.buffer.ByteBufUtil;
import org.apache.pulsar.functions.utils.Reflections;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import com.google.gson.Gson;

/**
 * Unit test of {@link CmdFunctions}.
 */
@PrepareForTest({ CmdFunctions.class, Reflections.class, StorageClientBuilder.class })
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class CmdFunctionsTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TEST_FUNCTION_NAME = "test-function";
    private static final String TEST_INPUT_TOPIC = "persistent://tenant/standalone/namespace/input-topic";
    private static final String TEST_OUTPUT_TOPIC = "persistent://tenant/standalone/namespace/output-topic";
    private static final String TEST_JAR = "src/test/resources/pulsar-functions-api-examples.jar";
    private static final String TEST_CLASS_NAME = "org.apache.pulsar.functions.api.examples.ExclamationFunction";

    private Functions functions;
    private CmdFunctions cmd;

    private String generateCustomSerdeInputs(String topic, String serde) {
        Map<String, String> map = new HashMap<>();
        map.put(topic, serde);
        return new Gson().toJson(map);
    }

    @BeforeMethod
    public void setup() throws Exception {
        PulsarAdmin admin = mock(PulsarAdmin.class);
        this.functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.getServiceUrl()).thenReturn("http://localhost:1234");
        when(admin.getClientConfigData()).thenReturn(new ClientConfigurationData());
        this.cmd = new CmdFunctions(admin);

        // mock reflections
        mockStatic(Reflections.class);
        when(Reflections.classExistsInJar(any(File.class), anyString())).thenReturn(true);
        when(Reflections.classExists(anyString())).thenReturn(true);
        when(Reflections.classInJarImplementsIface(any(File.class), anyString(), eq(Function.class)))
            .thenReturn(true);
        when(Reflections.classImplementsIface(anyString(), any())).thenReturn(true);
        when(Reflections.createInstance(eq(DefaultSerDe.class.getName()), any(File.class))).thenReturn(new DefaultSerDe(String.class));
    }

    @Test
    public void testLocalRunnerCmdNoArguments() throws Exception {
        cmd.run(new String[] { "localrun" });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getTenant());
        assertNull(runner.getNamespace());
        assertNull(runner.getFunctionName());
        assertNull(runner.getInputs());
        assertNull(runner.getOutput());
        assertNull(runner.getFnConfigFile());
        assertNull(runner.getClassName());
    }

    /*
    TODO(sijie):- Can we fix this?
    @Test
    public void testLocalRunnerCmdSettings() throws Exception {
        String fnName = TEST_FUNCTION_NAME + "-function";
        String sourceTopicName = TEST_FUNCTION_NAME + "-source-topic";
        String output = TEST_FUNCTION_NAME + "-sink-topic";
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
        String tenant = "sample";
        String namespace = "ns1";

        cmd.run(new String[] {
            "create",
            "--name", TEST_FUNCTION_NAME,
            "--inputs", TEST_INPUT_TOPIC,
            "--output", TEST_OUTPUT_TOPIC,
            "--jar", TEST_JAR,
            "--tenant", tenant,
            "--namespace", namespace,
            "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(namespace, creater.getNamespace());
        assertEquals(TEST_FUNCTION_NAME, creater.getFunctionName());
        assertEquals(TEST_INPUT_TOPIC, creater.getInputs());
        assertEquals(TEST_OUTPUT_TOPIC, creater.getOutput());
        assertEquals(TEST_JAR, creater.getJarFilePath());
        assertEquals(TEST_CLASS_NAME, creater.getClassName());

        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());

    }

    @Test
    public void testCreateWithoutTenant() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", TEST_FUNCTION_NAME,
                "--inputs", TEST_INPUT_TOPIC,
                "--output", TEST_OUTPUT_TOPIC,
                "--jar", TEST_JAR,
                "--namespace", "ns1",
                "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("tenant", creater.getTenant());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutNamespace() throws Exception {
        cmd.run(new String[] {
                "create",
                "--name", TEST_FUNCTION_NAME,
                "--inputs", TEST_INPUT_TOPIC,
                "--output", TEST_OUTPUT_TOPIC,
                "--jar", TEST_JAR,
                "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("tenant", creater.getTenant());
        assertEquals("namespace", creater.getNamespace());
        assertEquals(TEST_FUNCTION_NAME, creater.getFunctionName());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateUsingFullyQualifiedFunctionName() throws Exception {
        String tenant = "sample";
        String namespace = "ns1";
        String functionName = "func";
        String fqfn = String.format("%s/%s/%s", tenant, namespace, functionName);

        cmd.run(new String[] {
                "create",
                "--inputs", TEST_INPUT_TOPIC,
                "--output", TEST_OUTPUT_TOPIC,
                "--fqfn", fqfn,
                "--jar", TEST_JAR,
                "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(tenant, creater.getTenant());
        assertEquals(namespace, creater.getNamespace());
        assertEquals(functionName, creater.getFunctionName());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutFunctionName() throws Exception {
        cmd.run(new String[] {
                "create",
                "--inputs", TEST_INPUT_TOPIC,
                "--output", TEST_OUTPUT_TOPIC,
                "--jar", TEST_JAR,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("ExclamationFunction", creater.getFunctionName());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testCreateWithoutOutputTopic() throws Exception {
        cmd.run(new String[] {
                "create",
                "--inputs", TEST_INPUT_TOPIC,
                "--jar", TEST_JAR,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", TEST_CLASS_NAME,
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(String.format("%s-ExclamationFunction-output", TEST_INPUT_TOPIC), creater.getOutput());
        verify(functions, times(1)).createFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testGetFunction() throws Exception {
        String tenant = TEST_FUNCTION_NAME + "-tenant";
        String namespace = TEST_FUNCTION_NAME + "-namespace";

        cmd.run(new String[] {
            "get",
            "--name", TEST_FUNCTION_NAME,
            "--tenant", tenant,
            "--namespace", namespace
        });

        GetFunction getter = cmd.getGetter();
        assertEquals(TEST_FUNCTION_NAME, getter.getFunctionName());
        assertEquals(tenant, getter.getTenant());
        assertEquals(namespace, getter.getNamespace());

        verify(functions, times(1)).getFunction(eq(tenant), eq(namespace), eq(fnName));
    }

    @Test
    public void testDeleteFunction() throws Exception {
        String tenant = TEST_FUNCTION_NAME + "-tenant";
        String namespace = TEST_FUNCTION_NAME + "-namespace";

        cmd.run(new String[] {
            "delete",
            "--name", TEST_FUNCTION_NAME,
            "--tenant", tenant,
            "--namespace", namespace
        });

        DeleteFunction deleter = cmd.getDeleter();
        assertEquals(TEST_FUNCTION_NAME, deleter.getFunctionName());
        assertEquals(tenant, deleter.getTenant());
        assertEquals(namespace, deleter.getNamespace());

        verify(functions, times(1)).deleteFunction(eq(tenant), eq(namespace), eq(fnName));
    }

    @Test
    public void testUpdateFunction() throws Exception {
        cmd.run(new String[] {
            "update",
            "--name", TEST_FUNCTION_NAME,
            "--inputs", TEST_INPUT_TOPIC,
            "--output", TEST_OUTPUT_TOPIC,
            "--jar", TEST_JAR,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", TEST_CLASS_NAME,
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(TEST_FUNCTION_NAME, updater.getFunctionName());
        assertEquals(TEST_INPUT_TOPIC, updater.getInputs());
        assertEquals(TEST_OUTPUT_TOPIC, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionDetails.class), anyString());
    }

    @Test
    public void testListFunctions() throws Exception {
        String tenant = TEST_FUNCTION_NAME + "-tenant";
        String namespace = TEST_FUNCTION_NAME + "-namespace";

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
    public void testNonExistentJarFails() throws IllegalArgumentException {
        String tenant = TEST_FUNCTION_NAME + "-tenant";
        String namespace = TEST_FUNCTION_NAME + "-namespace";

        cmd.run(new String[] {
                "create",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", TEST_FUNCTION_NAME,
                "--inputs", TEST_INPUT_TOPIC,
                "--output", TEST_OUTPUT_TOPIC,
                "--jar", "non-existent-jar.jar",
                "--className", "org.apache.pulsar.functions.api.examples.DoesNotExist"
        });
    }

    @Test
    public void testStateGetter() throws Exception {
        String tenant = TEST_FUNCTION_NAME + "_tenant";
        String namespace = TEST_FUNCTION_NAME + "_namespace";
        String fnName = TEST_FUNCTION_NAME + "_function";

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
            "--storage-service-url", "127.0.0.1:4181"
        });

        assertEquals(
            "test-key",
            new String(ByteBufUtil.getBytes(keyHolder.get()), UTF_8));
    }
}
