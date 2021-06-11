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

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

@Slf4j
@PrepareForTest({CmdFunctions.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.core.*", "org.apache.pulsar.functions.api.*" })
public class TestCmdSinks {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TENANT = "test-tenant";
    private static final String NAMESPACE = "test-namespace";
    private static final String NAME = "test";
    private static final String CLASS_NAME = "SomeRandomClassName";
    private static final String INPUTS = "test-src1,test-src2";
    private static final List<String> INPUTS_LIST;
    static {
        INPUTS_LIST = new LinkedList<>();
        INPUTS_LIST.add("test-src1");
        INPUTS_LIST.add("test-src2");
    }
    private static final String TOPIC_PATTERN = "test-src*";
    private static final String CUSTOM_SERDE_INPUT_STRING = "{\"test_src3\": \"\"}";
    private static final Map<String, String> CUSTOM_SERDE_INPUT_MAP;
    static {
        CUSTOM_SERDE_INPUT_MAP = new HashMap<>();
        CUSTOM_SERDE_INPUT_MAP.put("test_src3", "");
    }
    private static final FunctionConfig.ProcessingGuarantees PROCESSING_GUARANTEES
            = FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE;
    private static final Integer PARALLELISM = 1;
    private static final String JAR_FILE_NAME = "dummy.nar";
    private String JAR_FILE_PATH;
    private String WRONG_JAR_PATH;
    private static final Double CPU = 100.0;
    private static final Long RAM = 1024L * 1024L;
    private static final Long DISK = 1024L * 1024L * 1024L;
    private static final String SINK_CONFIG_STRING = "{\"created_at\":\"Mon Jul 02 00:33:15 +0000 2018\",\"int\":1000,\"int_string\":\"1000\",\"float\":1000.0,\"float_string\":\"1000.0\"}";

    private PulsarAdmin pulsarAdmin;
    private Sinks sink;
    private CmdSinks cmdSinks;
    private CmdSinks.CreateSink createSink;
    private CmdSinks.UpdateSink updateSink;
    private CmdSinks.LocalSinkRunner localSinkRunner;
    private CmdSinks.DeleteSink deleteSink;
    private ClassLoader oldContextClassLoader;
    private ClassLoader jarClassLoader;


    @BeforeMethod
    public void setup() throws Exception {

        pulsarAdmin = mock(PulsarAdmin.class);
        sink = mock(Sinks.class);
        when(pulsarAdmin.sinks()).thenReturn(sink);

        cmdSinks = spy(new CmdSinks(() -> pulsarAdmin));
        createSink = spy(cmdSinks.getCreateSink());
        updateSink = spy(cmdSinks.getUpdateSink());
        localSinkRunner = spy(cmdSinks.getLocalSinkRunner());
        deleteSink = spy(cmdSinks.getDeleteSink());

        mockStatic(CmdFunctions.class);
        PowerMockito.doNothing().when(localSinkRunner).runCmd();
        URL file = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME);
        if (file == null)  {
            throw new RuntimeException("Failed to file required test archive: " + JAR_FILE_NAME);
        }
        JAR_FILE_PATH = file.getFile();
        jarClassLoader = ClassLoaderUtils.loadJar(new File(JAR_FILE_PATH));
        oldContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(jarClassLoader);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws IOException {
        if (jarClassLoader != null && jarClassLoader instanceof Closeable) {
            ((Closeable) jarClassLoader).close();
            jarClassLoader = null;
        }
        if (oldContextClassLoader != null) {
            Thread.currentThread().setContextClassLoader(oldContextClassLoader);
            oldContextClassLoader = null;
        }
    }

    public SinkConfig getSinkConfig() throws JsonProcessingException {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(TENANT);
        sinkConfig.setNamespace(NAMESPACE);
        sinkConfig.setName(NAME);

        sinkConfig.setInputs(INPUTS_LIST);
        sinkConfig.setTopicToSerdeClassName(CUSTOM_SERDE_INPUT_MAP);
        sinkConfig.setTopicsPattern(TOPIC_PATTERN);

        sinkConfig.setProcessingGuarantees(PROCESSING_GUARANTEES);
        sinkConfig.setParallelism(PARALLELISM);
        sinkConfig.setArchive(JAR_FILE_PATH);
        sinkConfig.setResources(new Resources(CPU, RAM, DISK));
        sinkConfig.setConfigs(createSink.parseConfigs(SINK_CONFIG_STRING));
        return sinkConfig;
    }

    @Test
    public void testCliCorrect() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingInput() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setInputs(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                null,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingCustomSerdeInput() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setTopicToSerdeClassName(null);
        sinkConfig.setTopicToSchemaType(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                null,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingTopicPattern() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setTopicsPattern(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                null,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingProcessingGuarantees() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setProcessingGuarantees(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                null,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Sink archive not specfied")
    public void testMissingArchive() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setArchive(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                null,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Sink Archive file /tmp/foo.jar" +
            " does not exist")
    public void testInvalidJar() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        String fakeJar = "/tmp/foo.jar";
        sinkConfig.setArchive(fakeJar);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                fakeJar,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingConfig() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setConfigs(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                null,
                sinkConfig
        );
    }

    public void testCmdSinkCliMissingArgs(
            String tenant,
            String namespace,
            String name,
            String inputs,
            String topicPattern,
            String customSerdeInputString,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String sinkConfigString,
            SinkConfig sinkConfig) throws Exception {

        // test create sink
        createSink.tenant = tenant;
        createSink.namespace = namespace;
        createSink.name = name;
        createSink.inputs = inputs;
        createSink.topicsPattern = topicPattern;
        createSink.customSerdeInputString = customSerdeInputString;
        createSink.processingGuarantees = processingGuarantees;
        createSink.parallelism = parallelism;
        createSink.archive = jarFile;
        createSink.cpu = cpu;
        createSink.ram = ram;
        createSink.disk = disk;
        createSink.sinkConfigString = sinkConfigString;

        createSink.processArguments();

        createSink.runCmd();

        // test update sink
        updateSink.tenant = tenant;
        updateSink.namespace = namespace;
        updateSink.name = name;
        updateSink.inputs = inputs;
        updateSink.topicsPattern = topicPattern;
        updateSink.customSerdeInputString = customSerdeInputString;
        updateSink.processingGuarantees = processingGuarantees;
        updateSink.parallelism = parallelism;
        updateSink.archive = jarFile;
        updateSink.cpu = cpu;
        updateSink.ram = ram;
        updateSink.disk = disk;
        updateSink.sinkConfigString = sinkConfigString;

        updateSink.processArguments();

        updateSink.runCmd();

        // test local runner
        localSinkRunner.tenant = tenant;
        localSinkRunner.namespace = namespace;
        localSinkRunner.name = name;
        localSinkRunner.inputs = inputs;
        localSinkRunner.topicsPattern = topicPattern;
        localSinkRunner.customSerdeInputString = customSerdeInputString;
        localSinkRunner.processingGuarantees = processingGuarantees;
        localSinkRunner.parallelism = parallelism;
        localSinkRunner.archive = jarFile;
        localSinkRunner.cpu = cpu;
        localSinkRunner.ram = ram;
        localSinkRunner.disk = disk;
        localSinkRunner.sinkConfigString = sinkConfigString;

        localSinkRunner.processArguments();

        localSinkRunner.runCmd();

        verify(createSink).validateSinkConfigs(eq(sinkConfig));
        verify(updateSink).validateSinkConfigs(eq(sinkConfig));
        verify(localSinkRunner).validateSinkConfigs(eq(sinkConfig));
    }


    @Test
    public void testCmdSinkConfigFileCorrect() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        testCmdSinkConfigFile(sinkConfig, sinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingTopicToSerdeClassName() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();

        SinkConfig expectedSinkConfig = getSinkConfig();
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingTopicsPattern() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();

        SinkConfig expectedSinkConfig = getSinkConfig();
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingConfig() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setConfigs(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setConfigs(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingProcessingGuarantees() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setProcessingGuarantees(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setProcessingGuarantees(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingResources() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setResources(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setResources(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Sink archive not specfied")
    public void testCmdSinkConfigFileMissingJar() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setArchive(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setArchive(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Sink Archive file /tmp/foo.jar does not exist")
    public void testCmdSinkConfigFileInvalidJar() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setArchive("/tmp/foo.jar");

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setArchive("/tmp/foo.jar");
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    public void testCmdSinkConfigFile(SinkConfig testSinkConfig, SinkConfig expectedSinkConfig) throws Exception {

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testSinkConfig);

        Assert.assertEquals(testSinkConfig, CmdUtils.loadConfig(file.getAbsolutePath(), SinkConfig.class));

        // test create sink
        createSink.sinkConfigFile = file.getAbsolutePath();

        createSink.processArguments();
        createSink.runCmd();

        // test update sink
        updateSink.sinkConfigFile = file.getAbsolutePath();

        updateSink.processArguments();
        updateSink.runCmd();

        // test local runner
        localSinkRunner.sinkConfigFile = file.getAbsolutePath();

        localSinkRunner.processArguments();
        localSinkRunner.runCmd();

        verify(createSink).validateSinkConfigs(eq(expectedSinkConfig));
        verify(updateSink).validateSinkConfigs(eq(expectedSinkConfig));
        verify(localSinkRunner).validateSinkConfigs(eq(expectedSinkConfig));
    }


    @Test
    public void testCliOverwriteConfigFile() throws Exception {

        SinkConfig testSinkConfig = new SinkConfig();
        testSinkConfig.setTenant(TENANT + "-prime");
        testSinkConfig.setNamespace(NAMESPACE + "-prime");
        testSinkConfig.setName(NAME + "-prime");

        testSinkConfig.setTopicToSerdeClassName(new HashMap<>());
        testSinkConfig.getTopicToSerdeClassName().put("test-src-prime", "");
        testSinkConfig.getTopicToSerdeClassName().put("test_src3-prime", "");
        testSinkConfig.setTopicsPattern(TOPIC_PATTERN + "-prime");

        testSinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        testSinkConfig.setParallelism(PARALLELISM + 1);
        testSinkConfig.setArchive(JAR_FILE_PATH + "-prime");
        testSinkConfig.setResources(new Resources(CPU + 1, RAM + 1, DISK + 1));
        testSinkConfig.setConfigs(createSink.parseConfigs("{\"created_at-prime\":\"Mon Jul 02 00:33:15 +0000 2018\", \"otherConfigProperties\":{\"property1.value\":\"value1\",\"property2.value\":\"value2\"}}"));


        SinkConfig expectedSinkConfig = getSinkConfig();

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testSinkConfig);

        Assert.assertEquals(testSinkConfig, CmdUtils.loadConfig(file.getAbsolutePath(), SinkConfig.class));

        testMixCliAndConfigFile(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                file.getAbsolutePath(),
                expectedSinkConfig
        );
    }

    public void testMixCliAndConfigFile(
            String tenant,
            String namespace,
            String name,
            String className,
            String inputs,
            String topicPattern,
            String customSerdeInputString,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String sinkConfigString,
            String sinkConfigFile,
            SinkConfig sinkConfig
    ) throws Exception {


        // test create sink
        createSink.tenant = tenant;
        createSink.namespace = namespace;
        createSink.name = name;
        createSink.inputs = inputs;
        createSink.topicsPattern = topicPattern;
        createSink.customSerdeInputString = customSerdeInputString;
        createSink.processingGuarantees = processingGuarantees;
        createSink.parallelism = parallelism;
        createSink.archive = jarFile;
        createSink.cpu = cpu;
        createSink.ram = ram;
        createSink.disk = disk;
        createSink.sinkConfigString = sinkConfigString;
        createSink.sinkConfigFile = sinkConfigFile;

        createSink.processArguments();

        createSink.runCmd();

        // test update sink
        updateSink.tenant = tenant;
        updateSink.namespace = namespace;
        updateSink.name = name;
        updateSink.inputs = inputs;
        updateSink.topicsPattern = topicPattern;
        updateSink.customSerdeInputString = customSerdeInputString;
        updateSink.processingGuarantees = processingGuarantees;
        updateSink.parallelism = parallelism;
        updateSink.archive = jarFile;
        updateSink.cpu = cpu;
        updateSink.ram = ram;
        updateSink.disk = disk;
        updateSink.sinkConfigString = sinkConfigString;
        updateSink.sinkConfigFile = sinkConfigFile;

        updateSink.processArguments();

        updateSink.runCmd();

        // test local runner
        localSinkRunner.tenant = tenant;
        localSinkRunner.namespace = namespace;
        localSinkRunner.name = name;
        localSinkRunner.inputs = inputs;
        localSinkRunner.topicsPattern = topicPattern;
        localSinkRunner.customSerdeInputString = customSerdeInputString;
        localSinkRunner.processingGuarantees = processingGuarantees;
        localSinkRunner.parallelism = parallelism;
        localSinkRunner.archive = jarFile;
        localSinkRunner.cpu = cpu;
        localSinkRunner.ram = ram;
        localSinkRunner.disk = disk;
        localSinkRunner.sinkConfigString = sinkConfigString;
        localSinkRunner.sinkConfigFile = sinkConfigFile;


        localSinkRunner.processArguments();

        localSinkRunner.runCmd();

        verify(createSink).validateSinkConfigs(eq(sinkConfig));
        verify(updateSink).validateSinkConfigs(eq(sinkConfig));
        verify(localSinkRunner).validateSinkConfigs(eq(sinkConfig));
    }

    @Test
    public void testDeleteMissingTenant() throws Exception {
        deleteSink.tenant = null;
        deleteSink.namespace = NAMESPACE;
        deleteSink.sinkName = NAME;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(sink).deleteSink(eq(PUBLIC_TENANT), eq(NAMESPACE), eq(NAME));
    }

    @Test
    public void testDeleteMissingNamespace() throws Exception {
        deleteSink.tenant = TENANT;
        deleteSink.namespace = null;
        deleteSink.sinkName = NAME;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(sink).deleteSink(eq(TENANT), eq(DEFAULT_NAMESPACE), eq(NAME));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "You must specify a name for the sink")
    public void testDeleteMissingName() throws Exception {
        deleteSink.tenant = TENANT;
        deleteSink.namespace = NAMESPACE;
        deleteSink.sinkName = null;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(sink).deleteSink(eq(TENANT), eq(NAMESPACE), null);
    }

    @Test
    public void testUpdateSink() throws Exception {

        updateSink.name = "my-sink";

        updateSink.archive = "new-archive";

        updateSink.processArguments();

        updateSink.runCmd();

        verify(sink).updateSink(eq(SinkConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateSink.name)
                .archive(updateSink.archive)
                .build()), eq(updateSink.archive), eq(new UpdateOptionsImpl()));


        updateSink.archive = null;

        updateSink.parallelism = 2;

        updateSink.processArguments();

        updateSink.updateAuthData = true;

        updateSink.runCmd();

        UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(true);

        verify(sink).updateSink(eq(SinkConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateSink.name)
                .parallelism(2)
                .build()), eq(null), eq(updateOptions));



    }

    @Test
    public void testParseConfigs() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        Map<String, Object> config = testSinkConfig.getConfigs();
        Assert.assertEquals(config.get("int"), 1000);
        Assert.assertEquals(config.get("int_string"), "1000");
        Assert.assertEquals(config.get("float"), 1000.0);
        Assert.assertEquals(config.get("float_string"), "1000.0");
        Assert.assertEquals(config.get("created_at"), "Mon Jul 02 00:33:15 +0000 2018");
    }
}
