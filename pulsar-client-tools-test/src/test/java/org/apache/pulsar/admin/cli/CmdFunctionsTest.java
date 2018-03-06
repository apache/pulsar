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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.io.File;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.LocalRunner;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminWithFunctions;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

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

    private static final String TEST_NAME = "test_name";

    private PulsarAdminWithFunctions admin;
    private Functions functions;
    private CmdFunctions cmd;

    public class DummyFunction implements PulsarFunction<String, String> {
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
        this.admin = mock(PulsarAdminWithFunctions.class);
        this.functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.getServiceUrl()).thenReturn(URI.create("http://localhost:1234").toURL());
        when(admin.getClientConf()).thenReturn(new ClientConfigurationData());
        this.cmd = new CmdFunctions(admin);

        // mock reflections
        mockStatic(Reflections.class);
        when(Reflections.classExistsInJar(any(File.class), anyString())).thenReturn(true);
        when(Reflections.classExists(anyString())).thenReturn(true);
        when(Reflections.classInJarImplementsIface(any(File.class), anyString(), eq(PulsarFunction.class)))
            .thenReturn(true);
        when(Reflections.classImplementsIface(anyString(), any())).thenReturn(true);
        when(Reflections.createInstance(eq(DummyFunction.class.getName()), any(File.class))).thenReturn(new DummyFunction());
        when(Reflections.createInstance(eq(DefaultSerDe.class.getName()), any(File.class))).thenReturn(new DefaultSerDe(String.class));
    }

    @Test
    public void testLocalRunnerCmdNoArguments() throws Exception {
        cmd.run(new String[] { "run" });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getFunctionName());
        assertNull(runner.getInputs());
        assertNull(runner.getOutput());
        assertNull(runner.getFnConfigFile());
    }

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
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", sourceTopicName,
            "--output", sinkTopicName,
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(sourceTopicName, creater.getInputs());
        assertEquals(sinkTopicName, creater.getOutput());

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void testCreateWithoutTenant() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = "persistent://tenant/standalone/namespace/source-topic";
        String sinkTopicName = "persistent://tenant/standalone/namespace/sink-topic";
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", sourceTopicName,
                "--output", sinkTopicName,
                "--jar", "SomeJar.jar",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("tenant", creater.getFunctionConfig().getTenant());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateWithoutNamespace() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = "persistent://tenant/standalone/namespace/source-topic";
        String sinkTopicName = "persistent://tenant/standalone/namespace/sink-topic";
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", sourceTopicName,
                "--output", sinkTopicName,
                "--jar", "SomeJar.jar",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("tenant", creater.getFunctionConfig().getTenant());
        assertEquals("namespace", creater.getFunctionConfig().getNamespace());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateWithoutFunctionName() throws Exception {
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", sourceTopicName,
                "--output", sinkTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals("CmdFunctionsTest$DummyFunction", creater.getFunctionConfig().getName());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateWithoutSinkTopic() throws Exception {
        String sourceTopicName = TEST_NAME + "-source-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", sourceTopicName,
                "--jar", "SomeJar.jar",
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(sourceTopicName + "-" + "CmdFunctionsTest$DummyFunction" + "-output", creater.getFunctionConfig().getOutput());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
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
        String sourceTopicName = TEST_NAME + "-source-topic";
        String sinkTopicName = TEST_NAME + "-sink-topic";



        cmd.run(new String[] {
            "update",
            "--name", fnName,
            "--inputs", sourceTopicName,
            "--output", sinkTopicName,
            "--jar", "SomeJar.jar",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        UpdateFunction updater = cmd.getUpdater();
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(sourceTopicName, updater.getInputs());
        assertEquals(sinkTopicName, updater.getOutput());

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
            "--storage-service-url", "127.0.0.1:4181"
        });

        assertEquals(
            "test-key",
            new String(ByteBufUtil.getBytes(keyHolder.get()), UTF_8));
    }
}
