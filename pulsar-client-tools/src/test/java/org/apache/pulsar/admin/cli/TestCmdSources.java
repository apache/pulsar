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
import static org.mockito.ArgumentMatchers.eq;
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
import java.nio.file.Files;
import java.util.Map;

import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.SourceConfig;
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
    private static final String JAR_FILE_NAME = "dummy.nar";
    private String JAR_FILE_PATH;
    private static final Double CPU = 100.0;
    private static final Long RAM = 1024L * 1024L;
    private static final Long DISK = 1024L * 1024L * 1024L;
    private static final String SINK_CONFIG_STRING =
            "{\"created_at\":\"Mon Jul 02 00:33:15 +0000 2018\",\"int\":1000,\"int_string\":\"1000\",\"float\":1000.0,\"float_string\":\"1000.0\"}";
    private static final String BATCH_SOURCE_CONFIG_STRING = "{ \"discoveryTriggererClassName\" : \"org.apache.pulsar.io.batchdiscovery.CronTriggerer\","
			+ "\"discoveryTriggererConfig\": {\"cron\": \"5 0 0 0 0 *\"} }";

    private PulsarAdmin pulsarAdmin;
    private Sources source;
    private CmdSources CmdSources;
    private CmdSources.CreateSource createSource;
    private CmdSources.UpdateSource updateSource;
    private CmdSources.LocalSourceRunner localSourceRunner;
    private CmdSources.DeleteSource deleteSource;
    private ClassLoader oldContextClassLoader;
    private ClassLoader jarClassLoader;

    @BeforeMethod
    public void setup() throws Exception {

        pulsarAdmin = mock(PulsarAdmin.class);
        source = mock(Sources.class);
        when(pulsarAdmin.sources()).thenReturn(source);

        CmdSources = spy(new CmdSources(() -> pulsarAdmin));
        createSource = spy(CmdSources.getCreateSource());
        updateSource = spy(CmdSources.getUpdateSource());
        localSourceRunner = spy(CmdSources.getLocalSourceRunner());
        deleteSource = spy(CmdSources.getDeleteSource());

        mockStatic(CmdFunctions.class);
        PowerMockito.doNothing().when(localSourceRunner).runCmd();
        JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME).getFile();
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

    public SourceConfig getSourceConfig() throws JsonProcessingException {
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
    
    public BatchSourceConfig getBatchSourceConfig() {
    	return createSource.parseBatchSourceConfigs(BATCH_SOURCE_CONFIG_STRING);
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

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Source archive not specified")
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Source Archive /tmp/foo.jar does not exist")
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

    private void testCmdSourceCliMissingArgs(
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
        expectedSourceConfig.setResources(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Source archive not specified")
    public void testCmdSourceConfigFileMissingJar() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setArchive(null);

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setArchive(null);
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Source Archive /tmp/foo.jar does not exist")
    public void testCmdSourceConfigFileInvalidJar() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        testSourceConfig.setArchive("/tmp/foo.jar");

        SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setArchive("/tmp/foo.jar");
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }

    @Test
    public void testBatchSourceConfigCorrect() throws Exception {
    	SourceConfig testSourceConfig = getSourceConfig();
    	testSourceConfig.setBatchSourceConfig(getBatchSourceConfig());
    	
    	SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setBatchSourceConfig(getBatchSourceConfig());
        testCmdSourceConfigFile(testSourceConfig, expectedSourceConfig);
    }
    
    /*
     * Test where the DiscoveryTriggererClassName is null
     */
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Discovery Triggerer not specified")
    public void testBatchSourceConfigMissingDiscoveryTriggererClassName() throws Exception {
    	SourceConfig testSourceConfig = getSourceConfig();
    	BatchSourceConfig batchSourceConfig = getBatchSourceConfig();
    	batchSourceConfig.setDiscoveryTriggererClassName(null);
    	testSourceConfig.setBatchSourceConfig(batchSourceConfig);
    	
    	SourceConfig expectedSourceConfig = getSourceConfig();
        expectedSourceConfig.setBatchSourceConfig(batchSourceConfig);
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
        testSourceConfig.setConfigs(createSource.parseConfigs("{\"created_at-prime\":\"Mon Jul 02 00:33:15 +0000 2018\", \"otherProperties\":{\"property1.value\":\"value1\",\"property2.value\":\"value2\"}}"));


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

    private void testMixCliAndConfigFile(
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
        deleteSource.sourceName = NAME;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(source).deleteSource(eq(PUBLIC_TENANT), eq(NAMESPACE), eq(NAME));
    }

    @Test
    public void testDeleteMissingNamespace() throws Exception {
        deleteSource.tenant = TENANT;
        deleteSource.namespace = null;
        deleteSource.sourceName = NAME;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(source).deleteSource(eq(TENANT), eq(DEFAULT_NAMESPACE), eq(NAME));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "You must specify a name for the source")
    public void testDeleteMissingName() throws Exception {
        deleteSource.tenant = TENANT;
        deleteSource.namespace = NAMESPACE;
        deleteSource.sourceName = null;

        deleteSource.processArguments();

        deleteSource.runCmd();

        verify(source).deleteSource(eq(TENANT), eq(NAMESPACE), null);
    }

    @Test
    public void testUpdateSource() throws Exception {

        updateSource.name = "my-source";

        updateSource.archive = "new-archive";

        updateSource.processArguments();

        updateSource.runCmd();

        verify(source).updateSource(eq(SourceConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateSource.name)
                .archive(updateSource.archive)
                .build()), eq(updateSource.archive), eq(new UpdateOptionsImpl()));


        updateSource.archive = null;

        updateSource.parallelism = 2;

        updateSource.processArguments();

        updateSource.updateAuthData = true;

        UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(true);

        updateSource.runCmd();

        verify(source).updateSource(eq(SourceConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateSource.name)
                .parallelism(2)
                .build()), eq(null), eq(updateOptions));



    }

    @Test
    public void testParseConfigs() throws Exception {
        SourceConfig testSourceConfig = getSourceConfig();
        Map<String, Object> config = testSourceConfig.getConfigs();
        Assert.assertEquals(config.get("int"), 1000);
        Assert.assertEquals(config.get("int_string"), "1000");
        Assert.assertEquals(config.get("float"), 1000.0);
        Assert.assertEquals(config.get("float_string"), "1000.0");
        Assert.assertEquals(config.get("created_at"), "Mon Jul 02 00:33:15 +0000 2018");
    }
}
