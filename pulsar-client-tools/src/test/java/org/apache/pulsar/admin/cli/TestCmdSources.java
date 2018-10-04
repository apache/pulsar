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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.nio.file.Files;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Resources;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

@Slf4j
@PrepareForTest({CmdFunctions.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.core.*" })
public class TestCmdSources {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TENANT = "test-tenant";
    private static final String NAMESPACE = "test-namespace";
    private static final String NAME = "test";
    private static final String TOPIC_NAME = "src_topic_1";
    private static final String SERDE_CLASS_NAME = "";
    private static final FunctionConfig.ProcessingGuarantees PROCESSING_GUARANTEES
            = FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE;
    private static final Integer PARALLELISM = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-twitter.nar";
    private static final String WRONG_JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private String JAR_FILE_PATH;
    private String WRONG_JAR_PATH;
    private static final Double CPU = 100.0;
    private static final Long RAM = 1024L * 1024L;
    private static final Long DISK = 1024L * 1024L * 1024L;
    private static final String SINK_CONFIG_STRING = "{\"created_at\":\"Mon Jul 02 00:33:15 +0000 2018\"}";

    private PulsarAdmin pulsarAdmin;
    private Functions functions;
    private CmdSources CmdSources;
    private CmdSources.CreateSource createSource;
    private CmdSources.UpdateSource updateSource;
    private CmdSources.LocalSourceRunner localSourceRunner;
    private CmdSources.DeleteSource deleteSource;

    @BeforeMethod
    public void setup() throws Exception {

        pulsarAdmin = mock(PulsarAdmin.class);
        functions = mock(Functions.class);
        when(pulsarAdmin.functions()).thenReturn(functions);

        CmdSources = spy(new CmdSources(pulsarAdmin));
        createSource = spy(CmdSources.getCreateSource());
        updateSource = spy(CmdSources.getUpdateSource());
        localSourceRunner = spy(CmdSources.getLocalSourceRunner());
        deleteSource = spy(CmdSources.getDeleteSource());

        mockStatic(CmdFunctions.class);
        PowerMockito.doNothing().when(CmdFunctions.class, "startLocalRun", Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME).getFile();
        WRONG_JAR_PATH = Thread.currentThread().getContextClassLoader().getResource(WRONG_JAR_FILE_NAME).getFile();
        Thread.currentThread().setContextClassLoader(Reflections.loadJar(new File(JAR_FILE_PATH)));
    }

    public SourceConfig getSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(TENANT);
        sourceConfig.setNamespace(NAMESPACE);
        sourceConfig.setName(NAME);

        sourceConfig.setTopicName(TOPIC_NAME);
        sourceConfig.setSerdeClassName(SERDE_CLASS_NAME);
        sourceConfig.setProcessingGuarantees(PROCESSING_GUARANTEES);
        sourceConfig.setParallelism(PARALLELISM);
        sourceConfig.setArchive(JAR_FILE_PATH);
        sourceConfig.setResources(new Resources(CPU, RAM, DISK));
        sourceConfig.setConfigs(createSource.parseConfigs(SINK_CONFIG_STRING));
        return sourceConfig;
    }

    @Test
    public void testCliCorrect() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingTenant() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setTenant(PUBLIC_TENANT);
        testCmdSourceCliMissingArgs(
                null,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingNamespace() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setNamespace(DEFAULT_NAMESPACE);
        testCmdSourceCliMissingArgs(
                TENANT,
                null,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'name' cannot be null!")
    public void testMissingName() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setName(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                null,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'topicName' cannot be null!")
    public void testMissingTopicName() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setTopicName(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                null,
                SERDE_CLASS_NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingSerdeClassName() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setSerdeClassName(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME,
                null,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingProcessingGuarantees() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setProcessingGuarantees(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, null,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingParallelism() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setParallelism(1);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                null,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testNegativeParallelism() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setParallelism(-1);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                -1,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testZeroParallelism() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setParallelism(0);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                0,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Source archive not specfied")
    public void testMissingArchive() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setArchive(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                null,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Archive file /tmp/foo.jar does not exist")
    public void testInvalidJar() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        String fakeJar = "/tmp/foo.jar";
        sourceConfig.setArchive(fakeJar);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                fakeJar,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testInvalidJarWithNoSource() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setArchive(WRONG_JAR_PATH);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                WRONG_JAR_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingCpu() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setResources(new Resources(null, RAM, DISK));
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                null,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingRam() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setResources(new Resources(CPU, null, DISK));
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                null,
                DISK,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingDisk() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setResources(new Resources(CPU, RAM, null));
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                null,
                SINK_CONFIG_STRING,
                sourceConfig
        );
    }

    @Test
    public void testMissingConfig() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        sourceConfig.setConfigs(null);
        testCmdSourceCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME, SERDE_CLASS_NAME, PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                null,
                sourceConfig
        );
    }

    public void testCmdSourceCliMissingArgs(
            String tenant,
            String namespace,
            String name,
            String topicName, String serdeClassName,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String sourceConfigString,
            SourceConfig sourceConfig) throws Exception {

        // test create source
        createSource.tenant = tenant;
        createSource.namespace = namespace;
        createSource.name = name;
        createSource.destinationTopicName = topicName;
        createSource.deserializationClassName  = serdeClassName;
        createSource.processingGuarantees = processingGuarantees;
        createSource.parallelism = parallelism;
        createSource.archive = jarFile;
        createSource.cpu = cpu;
        createSource.ram = ram;
        createSource.disk = disk;
        createSource.sourceConfigString = sourceConfigString;

        createSource.processArguments();

        createSource.runCmd();

        // test update source
        updateSource.tenant = tenant;
        updateSource.namespace = namespace;
        updateSource.name = name;
        updateSource.destinationTopicName = topicName;
        updateSource.deserializationClassName  = serdeClassName;
        updateSource.processingGuarantees = processingGuarantees;
        updateSource.parallelism = parallelism;
        updateSource.archive = jarFile;
        updateSource.cpu = cpu;
        updateSource.ram = ram;
        updateSource.disk = disk;
        updateSource.sourceConfigString = sourceConfigString;

        updateSource.processArguments();

        updateSource.runCmd();

        // test local runner
        localSourceRunner.tenant = tenant;
        localSourceRunner.namespace = namespace;
        localSourceRunner.name = name;
        localSourceRunner.destinationTopicName = topicName;
        localSourceRunner.deserializationClassName  = serdeClassName;
        localSourceRunner.processingGuarantees = processingGuarantees;
        localSourceRunner.parallelism = parallelism;
        localSourceRunner.archive = jarFile;
        localSourceRunner.cpu = cpu;
        localSourceRunner.ram = ram;
        localSourceRunner.disk = disk;
        localSourceRunner.sourceConfigString = sourceConfigString;

        localSourceRunner.processArguments();

        localSourceRunner.runCmd();

        verify(createSource).validateSourceConfigs(eq(sourceConfig));
        verify(updateSource).validateSourceConfigs(eq(sourceConfig));
        verify(localSourceRunner).validateSourceConfigs(eq(sourceConfig));
    }


    @Test
    public void testCmdSourceConfigFileCorrect() throws Exception {
        SourceConfig sourceConfig = getSourceConfig();
        testCmdSourceConfigFile(sourceConfig, sourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingTenant() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setTenant(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setTenant(PUBLIC_TENANT);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingNamespace() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setNamespace(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setNamespace(DEFAULT_NAMESPACE);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'name' cannot be null!")
    public void testCmdSourceConfigFileMissingName() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setName(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setName(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'topicName' cannot be null!")
    public void testCmdSourceConfigFileMissingTopicName() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setTopicName(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setTopicName(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingSerdeClassname() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setSerdeClassName(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setSerdeClassName(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingConfig() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setConfigs(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setConfigs(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testCmdSourceConfigFileZeroParallelism() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setParallelism(0);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setParallelism(0);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Field 'parallelism' must be a Positive Number")
    public void testCmdSourceConfigFileNegativeParallelism() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setParallelism(-1);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setParallelism(-1);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingProcessingGuarantees() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setProcessingGuarantees(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setProcessingGuarantees(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testCmdSourceConfigFileMissingResources() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setResources(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setResources(new Resources());
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Source archive not specfied")
    public void testCmdSourceConfigFileMissingJar() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setArchive(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setArchive(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Archive file /tmp/foo.jar does not exist")
    public void testCmdSourceConfigFileInvalidJar() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setArchive("/tmp/foo.jar");

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setArchive("/tmp/foo.jar");
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testCmdSourceConfigFileInvalidJarNoSource() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setArchive(WRONG_JAR_PATH);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setArchive(WRONG_JAR_PATH);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    public void testCmdSourceConfigFile(SourceConfig testSourceConfig, SourceConfig expectedSourceConfig) throws Exception {

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testSourceConfig);

        Assert.assertEquals(testSourceConfig, CmdUtils.loadConfig(file.getAbsolutePath(), SourceConfig.class));

        // test create source
        createSource.sourceConfigFile = file.getAbsolutePath();

        createSource.processArguments();
        createSource.runCmd();

        // test update source
        updateSource.sourceConfigFile = file.getAbsolutePath();

        updateSource.processArguments();
        updateSource.runCmd();

        // test local runner
        localSourceRunner.sourceConfigFile = file.getAbsolutePath();

        localSourceRunner.processArguments();
        localSourceRunner.runCmd();

        verify(createSource).validateSourceConfigs(eq(expectedSourceConfig));
        verify(updateSource).validateSourceConfigs(eq(expectedSourceConfig));
        verify(localSourceRunner).validateSourceConfigs(eq(expectedSourceConfig));
    }


    @Test
    public void testCliOverwriteConfigFile() throws Exception {

        SourceConfig testSourceConfig = new SourceConfig();
        testSourceConfig.setTenant(TENANT + "-prime");
        testSourceConfig.setNamespace(NAMESPACE + "-prime");
        testSourceConfig.setName(NAME + "-prime");
        testSourceConfig.setTopicName(TOPIC_NAME + "-prime");
        testSourceConfig.setSerdeClassName(SERDE_CLASS_NAME + "-prime");
        testSourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        testSourceConfig.setParallelism(PARALLELISM + 1);
        testSourceConfig.setArchive(JAR_FILE_PATH + "-prime");
        testSourceConfig.setResources(new Resources(CPU + 1, RAM + 1, DISK + 1));
        testSourceConfig.setConfigs(createSource.parseConfigs("{\"created_at-prime\":\"Mon Jul 02 00:33:15 +0000 2018\"}"));


        SourceConfig expectedSourceConfig = getSourceConfig();

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testSourceConfig);

        Assert.assertEquals(testSourceConfig, CmdUtils.loadConfig(file.getAbsolutePath(), SourceConfig.class));

        testMixCliAndConfigFile(
                TENANT,
                NAMESPACE,
                NAME,
                TOPIC_NAME,
                SERDE_CLASS_NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                SINK_CONFIG_STRING,
                file.getAbsolutePath(),
                expectedSourceConfig
        );
    }

    public void testMixCliAndConfigFile(
            String tenant,
            String namespace,
            String name,
            String topicName,
            String serdeClassName,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String sourceConfigString,
            String sourceConfigFile,
            SourceConfig sourceConfig
    ) throws Exception {


        // test create source
        createSource.tenant = tenant;
        createSource.namespace = namespace;
        createSource.name = name;
        createSource.destinationTopicName = topicName;
        createSource.deserializationClassName = serdeClassName;
        createSource.processingGuarantees = processingGuarantees;
        createSource.parallelism = parallelism;
        createSource.archive = jarFile;
        createSource.cpu = cpu;
        createSource.ram = ram;
        createSource.disk = disk;
        createSource.sourceConfigString = sourceConfigString;
        createSource.sourceConfigFile = sourceConfigFile;

        createSource.processArguments();

        createSource.runCmd();

        // test update source
        updateSource.tenant = tenant;
        updateSource.namespace = namespace;
        updateSource.name = name;
        updateSource.destinationTopicName = topicName;
        updateSource.deserializationClassName = serdeClassName;
        updateSource.processingGuarantees = processingGuarantees;
        updateSource.parallelism = parallelism;
        updateSource.archive = jarFile;
        updateSource.cpu = cpu;
        updateSource.ram = ram;
        updateSource.disk = disk;
        updateSource.sourceConfigString = sourceConfigString;
        updateSource.sourceConfigFile = sourceConfigFile;


        updateSource.processArguments();

        updateSource.runCmd();

        // test local runner
        localSourceRunner.tenant = tenant;
        localSourceRunner.namespace = namespace;
        localSourceRunner.name = name;
        localSourceRunner.destinationTopicName = topicName;
        localSourceRunner.deserializationClassName = serdeClassName;
        localSourceRunner.processingGuarantees = processingGuarantees;
        localSourceRunner.parallelism = parallelism;
        localSourceRunner.archive = jarFile;
        localSourceRunner.cpu = cpu;
        localSourceRunner.ram = ram;
        localSourceRunner.disk = disk;
        localSourceRunner.sourceConfigString = sourceConfigString;
        localSourceRunner.sourceConfigFile = sourceConfigFile;


        localSourceRunner.processArguments();

        localSourceRunner.runCmd();

        verify(createSource).validateSourceConfigs(eq(sourceConfig));
        verify(updateSource).validateSourceConfigs(eq(sourceConfig));
        verify(localSourceRunner).validateSourceConfigs(eq(sourceConfig));
    }

    @Test
    public void testDeleteMissingTenant() throws Exception {
        deleteSource.tenant = null;
        deleteSource.namespace = NAMESPACE;
        deleteSource.name = NAME;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(functions).deleteFunction(eq(PUBLIC_TENANT), eq(NAMESPACE), eq(NAME));
    }

    @Test
    public void testDeleteMissingNamespace() throws Exception {
        deleteSource.tenant = TENANT;
        deleteSource.namespace = null;
        deleteSource.name = NAME;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(functions).deleteFunction(eq(TENANT), eq(DEFAULT_NAMESPACE), eq(NAME));
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "You must specify a name for the source")
    public void testDeleteMissingName() throws Exception {
        deleteSource.tenant = TENANT;
        deleteSource.namespace = NAMESPACE;
        deleteSource.name = null;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(functions).deleteFunction(eq(TENANT), eq(NAMESPACE), null);
    }
}