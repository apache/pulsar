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

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Resources;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@Slf4j
@PrepareForTest({CmdFunctions.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.core.*" })
public class TestCmdSinks {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TENANT = "test-tenant";
    private static final String NAMESPACE = "test-namespace";
    private static final String NAME = "test";
    private static final String CLASS_NAME = CassandraStringSink.class.getName();
    private static final String INPUTS = "test-src1,test-src2";
    private static final String TOPIC_PATTERN = "test-src*";
    private static final String CUSTOM_SERDE_INPUT_STRING = "{\"test_src3\": \"\"}";
    private static final FunctionConfig.ProcessingGuarantees PROCESSING_GUARANTEES
            = FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE;
    private static final Integer PARALLELISM = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private String JAR_FILE_PATH;
    private static final Double CPU = 100.0;
    private static final Long RAM = 1024L * 1024L;
    private static final Long DISK = 1024L * 1024L * 1024L;
    private static final String SINK_CONFIG_STRING = "{\"created_at\":\"Mon Jul 02 00:33:15 +0000 2018\"}";

    private PulsarAdmin pulsarAdmin;
    private Functions functions;
    private CmdSinks cmdSinks;
    private CmdSinks.CreateSink createSink;
    private CmdSinks.UpdateSink updateSink;
    private CmdSinks.LocalSinkRunner localSinkRunner;
    private CmdSinks.DeleteSink deleteSink;

    @BeforeMethod
    public void setup() throws Exception {

        pulsarAdmin = mock(PulsarAdmin.class);
        functions = mock(Functions.class);
        when(pulsarAdmin.functions()).thenReturn(functions);

        cmdSinks = spy(new CmdSinks(pulsarAdmin));
        createSink = spy(cmdSinks.getCreateSink());
        updateSink = spy(cmdSinks.getUpdateSink());
        localSinkRunner = spy(cmdSinks.getLocalSinkRunner());
        deleteSink = spy(cmdSinks.getDeleteSink());

        mockStatic(CmdFunctions.class);
        PowerMockito.doNothing().when(CmdFunctions.class, "startLocalRun", Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME).getFile();
        Thread.currentThread().setContextClassLoader(Reflections.loadJar(new File(JAR_FILE_PATH)));
    }

    public SinkConfig getSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(TENANT);
        sinkConfig.setNamespace(NAMESPACE);
        sinkConfig.setName(NAME);

        Map<String, String> topicsToSerDeClassName = new HashMap<>();
        createSink.parseInputs(INPUTS, topicsToSerDeClassName);
        createSink.parseCustomSerdeInput(CUSTOM_SERDE_INPUT_STRING, topicsToSerDeClassName);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);

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
                sinkConfig
        );
    }
    
    @Test
    public void testMissingTenant() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setTenant(PUBLIC_TENANT);
        testCmdSinkCliMissingArgs(
                null,
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
                sinkConfig
        );
    }

    @Test
    public void testMissingNamespace() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setNamespace(DEFAULT_NAMESPACE);
        testCmdSinkCliMissingArgs(
                TENANT,
                null,
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
                sinkConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'name' cannot be null!")
    public void testMissingName() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setName(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                null,
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
                sinkConfig
        );
    }

    @Test
    public void testMissingInput() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        Map<String, String> topicsToSerDeClassName = new HashMap<>();
        createSink.parseCustomSerdeInput(CUSTOM_SERDE_INPUT_STRING, topicsToSerDeClassName);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
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
        Map<String, String> topicsToSerDeClassName = new HashMap<>();
        createSink.parseInputs(INPUTS, topicsToSerDeClassName);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
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
                CLASS_NAME,
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

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Must specify at least one topic of input via inputs, customSerdeInputs, or topicPattern")
    public void testMissingAllInputTopics() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setTopicsPattern(null);
        sinkConfig.setTopicToSerdeClassName(new HashMap<>());
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
                null,
                null,
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
    public void testMissingProcessingGuarantees() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setProcessingGuarantees(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
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

    @Test
    public void testMissingParallelism() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setParallelism(1);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                null,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testNegativeParallelism() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setParallelism(-1);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                -1,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testZeroParallelism() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setParallelism(0);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
                INPUTS,
                TOPIC_PATTERN,
                CUSTOM_SERDE_INPUT_STRING,
                PROCESSING_GUARANTEES,
                0,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Sink jar not specfied")
    public void testMissingArchive() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setArchive(null);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
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

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Archive file /tmp/foo.jar does not exist")
    public void testInvalidJar() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        String fakeJar = "/tmp/foo.jar";
        sinkConfig.setArchive(fakeJar);
        testCmdSinkCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                CLASS_NAME,
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
    public void testMissingCpu() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setResources(new Resources(null, RAM, DISK));
        testCmdSinkCliMissingArgs(
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
                null,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingRam() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setResources(new Resources(CPU, null, DISK));
        testCmdSinkCliMissingArgs(
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
                null,
                DISK,
                SINK_CONFIG_STRING,
                sinkConfig
        );
    }

    @Test
    public void testMissingDisk() throws Exception {
        SinkConfig sinkConfig = getSinkConfig();
        sinkConfig.setResources(new Resources(CPU, RAM, null));
        testCmdSinkCliMissingArgs(
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
                null,
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
                null,
                sinkConfig
        );
    }

    public void testCmdSinkCliMissingArgs(
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
    public void testCmdSinkConfigFileMissingTenant() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setTenant(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setTenant(PUBLIC_TENANT);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingNamespace() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setNamespace(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setNamespace(DEFAULT_NAMESPACE);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'name' cannot be null!")
    public void testCmdSinkConfigFileMissingName() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setName(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setName(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingTopicToSerdeClassName() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setTopicToSerdeClassName(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setTopicToSerdeClassName(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test
    public void testCmdSinkConfigFileMissingTopicsPattern() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setTopicsPattern(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setTopicsPattern(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Must specify at least one topic of input via inputs, customSerdeInputs, or topicPattern")
    public void testCmdSinkConfigFileMissingAllInput() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setTopicsPattern(null);
        testSinkConfig.setTopicToSerdeClassName(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setTopicsPattern(null);
        expectedSinkConfig.setTopicToSerdeClassName(null);
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

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testCmdSinkConfigFileZeroParallelism() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setParallelism(0);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setParallelism(0);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testCmdSinkConfigFileNegativeParallelism() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setParallelism(-1);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setParallelism(-1);
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
        expectedSinkConfig.setResources(new Resources());
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Sink jar not specfied")
    public void testCmdSinkConfigFileMissingJar() throws Exception {
        SinkConfig testSinkConfig = getSinkConfig();
        testSinkConfig.setArchive(null);

        SinkConfig expectedSinkConfig = getSinkConfig();
        expectedSinkConfig.setArchive(null);
        testCmdSinkConfigFile(testSinkConfig, expectedSinkConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Archive file /tmp/foo.jar does not exist")
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

        Map<String, String> topicsToSerDeClassName = new HashMap<>();
        createSink.parseInputs(INPUTS + ",test-src-prime", topicsToSerDeClassName);
        createSink.parseCustomSerdeInput("{\"test_src3-prime\": \"\"}", topicsToSerDeClassName);
        testSinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);

        testSinkConfig.setTopicsPattern(TOPIC_PATTERN + "-prime");
        testSinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        testSinkConfig.setParallelism(PARALLELISM + 1);
        testSinkConfig.setArchive(JAR_FILE_PATH + "-prime");
        testSinkConfig.setResources(new Resources(CPU + 1, RAM + 1, DISK + 1));
        testSinkConfig.setConfigs(createSink.parseConfigs("{\"created_at-prime\":\"Mon Jul 02 00:33:15 +0000 2018\"}"));
        

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
        deleteSink.name = NAME;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(functions).deleteFunction(eq(PUBLIC_TENANT), eq(NAMESPACE), eq(NAME));
    }

    @Test
    public void testDeleteMissingNamespace() throws Exception {
        deleteSink.tenant = TENANT;
        deleteSink.namespace = null;
        deleteSink.name = NAME;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(functions).deleteFunction(eq(TENANT), eq(DEFAULT_NAMESPACE), eq(NAME));
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "You must specify a name for the sink")
    public void testDeleteMissingName() throws Exception {
        deleteSink.tenant = TENANT;
        deleteSink.namespace = NAMESPACE;
        deleteSink.name = null;

        deleteSink.processArguments();

        deleteSink.runCmd();

        verify(functions).deleteFunction(eq(TENANT), eq(NAMESPACE), null);
    }
}
