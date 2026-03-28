/*
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
package org.apache.pulsar.functions.runtime.process;

import static org.apache.pulsar.functions.runtime.RuntimeUtils.FUNCTIONS_INSTANCE_CLASSPATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.util.JsonFormat;
import io.kubernetes.client.openapi.models.V1PodSpec;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.ConsumerSpec;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntime;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.FunctionsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ThreadRuntime}.
 */
public class ProcessRuntimeTest {
    private String narExtractionDirectory = "/tmp/foo";
    private String defaultWebServiceUrl = "http://localhost:8080";

    class TestSecretsProviderConfigurator implements SecretsProviderConfigurator {

        @Override
        public String getSecretsProviderClassName(FunctionDetails functionDetails) {
            if (functionDetails.getRuntime() == FunctionDetails.Runtime.JAVA) {
                return ClearTextSecretsProvider.class.getName();
            } else {
                return "secretsprovider.ClearTextSecretsProvider";
            }
        }

        @Override
        public Map<String, String> getSecretsProviderConfig(FunctionDetails functionDetails) {
            Map<String, String> config = new HashMap<>();
            config.put("Config", "Value");
            return config;
        }

        @Override
        public void configureKubernetesRuntimeSecretsProvider(V1PodSpec podSpec, String functionsContainerName,
                                                              FunctionDetails functionDetails) {
        }

        @Override
        public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder,
                                                           FunctionDetails functionDetails) {
        }

        @Override
        public Type getSecretObjectType() {
            return TypeToken.get(String.class).getType();
        }

    }

    private static final String TEST_TENANT = "test-function-tenant";
    private static final String TEST_NAMESPACE = "test-function-namespace";
    private static final String TEST_NAME = "test-function-container";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    private static final Map<String, ConsumerSpec> topicsToSchema = new HashMap<>();
    static {
        topicsToSerDeClassName.put("test_src", "");
        topicsToSchema.put("test_src",
                ConsumerSpec.newBuilder().setSerdeClassName("").setIsRegexPattern(false).build());
    }

    private ProcessRuntimeFactory factory;
    private final String userJarFile;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String pulsarServiceUrl;
    private final String stateStorageServiceUrl;
    private final String logDirectory;

    public ProcessRuntimeTest() {
        this.userJarFile = "/Users/user/UserJar.jar";
        this.javaInstanceJarFile = "/Users/user/JavaInstance.jar";
        this.pythonInstanceFile = "/Users/user/PythonInstance.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.stateStorageServiceUrl = "bk://localhost:4181";
        this.logDirectory = "Users/user/logs";
    }

    @BeforeClass
    public void setup() {
        System.setProperty(FUNCTIONS_INSTANCE_CLASSPATH, "/pulsar/lib/*");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (null != this.factory) {
            this.factory.close();
        }
    }

    private ProcessRuntimeFactory createProcessRuntimeFactory(String extraDependenciesDir) {
        return createProcessRuntimeFactory(extraDependenciesDir, null, false, null);
    }

    private ProcessRuntimeFactory createProcessRuntimeFactory(String extraDependenciesDir, String webServiceUrl,
                                                              boolean exposePulsarAdminClientEnabled,
                                                              AuthenticationConfig authConfig) {
        ProcessRuntimeFactory processRuntimeFactory = new ProcessRuntimeFactory();

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setPulsarServiceUrl(pulsarServiceUrl);
        workerConfig.setStateStorageServiceUrl(stateStorageServiceUrl);
        workerConfig.setAuthenticationEnabled(false);
        workerConfig.setNarExtractionDirectory(narExtractionDirectory);
        if (webServiceUrl != null) {
            workerConfig.setPulsarWebServiceUrl(webServiceUrl);
        }
        workerConfig.setExposeAdminClientEnabled(exposePulsarAdminClientEnabled);

        ProcessRuntimeFactoryConfig processRuntimeFactoryConfig = new ProcessRuntimeFactoryConfig();
        processRuntimeFactoryConfig.setJavaInstanceJarLocation(javaInstanceJarFile);
        processRuntimeFactoryConfig.setPythonInstanceLocation(pythonInstanceFile);
        processRuntimeFactoryConfig.setLogDirectory(logDirectory);
        processRuntimeFactoryConfig.setExtraFunctionDependenciesDir(extraDependenciesDir);

        workerConfig.setFunctionRuntimeFactoryClassName(ProcessRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(processRuntimeFactoryConfig, Map.class));
        processRuntimeFactory.initialize(workerConfig, authConfig, new TestSecretsProviderConfigurator(),
                Mockito.mock(ConnectorsManager.class), Mockito.mock(FunctionsManager.class), Optional.empty(),
                Optional.empty());

        return processRuntimeFactory;
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(runtime);
        functionDetailsBuilder.setTenant(TEST_TENANT);
        functionDetailsBuilder.setNamespace(TEST_NAMESPACE);
        functionDetailsBuilder.setName(TEST_NAME);
        functionDetailsBuilder.setClassName("org.apache.pulsar.functions.utils.functioncache.AddFunction");
        functionDetailsBuilder.setSink(Function.SinkSpec.newBuilder()
                .setTopic(TEST_NAME + "-output")
                .setSerDeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer")
                .setClassName("org.pulsar.pulsar.TestSink")
                .setTypeClassName(String.class.getName())
                .build());
        functionDetailsBuilder.setLogTopic(TEST_NAME + "-log");
        functionDetailsBuilder.setSource(Function.SourceSpec.newBuilder()
                .setSubscriptionType(Function.SubscriptionType.FAILOVER)
                .putAllInputSpecs(topicsToSchema)
                .setClassName("org.pulsar.pulsar.TestSource")
                .setTypeClassName(String.class.getName()));
        return functionDetailsBuilder.build();
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(runtime));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(0);
        config.setMaxBufferedTuples(1024);
        config.setMaxPendingAsyncRequests(200);
        config.setClusterName("standalone");

        return config;
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime, boolean exposePulsarAdminClientEnabled) {
        InstanceConfig config = createJavaInstanceConfig(runtime);
        config.setExposePulsarAdminClientEnabled(exposePulsarAdminClientEnabled);
        return config;
    }

    @Test
    public void testJavaConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory(null);

        verifyJavaInstance(config);
    }

    @Test
    public void testJavaConstructorWithEmptyExtraDepsDirString() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory("");

        verifyJavaInstance(config);
    }

    @Test
    public void testJavaConstructorWithNoneExistentDir() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory("/path/to/non-existent/dir");

        verifyJavaInstance(config, Paths.get("/path/to/non-existent/dir"));
    }

    @Test
    public void testJavaConstructorWithEmptyDir() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        Path dir = Files.createTempDirectory("test-empty-dir");
        assertTrue(Files.exists(dir));
        try {
            factory = createProcessRuntimeFactory(dir.toAbsolutePath().toString());

            verifyJavaInstance(config, dir);
        } finally {
            MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    @Test
    public void testJavaConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        Path dir = Files.createTempDirectory("test-empty-dir");
        assertTrue(Files.exists(dir));
        try {
            int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                Path file = Files.createFile(Paths.get(dir.toAbsolutePath().toString(), "file-" + i));
                assertTrue(Files.exists(file));
            }

            factory = createProcessRuntimeFactory(dir.toAbsolutePath().toString());

            verifyJavaInstance(config, dir);
        } finally {
            MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    private void verifyJavaInstance(InstanceConfig config) throws Exception {
        verifyJavaInstance(config, null, null);
    }

    private void verifyJavaInstance(InstanceConfig config, Path depsDir) throws Exception {
        verifyJavaInstance(config, depsDir, null);
    }

    private void verifyJavaInstance(InstanceConfig config, Path depsDir, String webServiceUrl) throws Exception {
        List<String> args;
        try (MockedStatic<SystemUtils> systemUtils = Mockito.mockStatic(SystemUtils.class,
                Mockito.CALLS_REAL_METHODS)) {
            systemUtils.when(() -> SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)).thenReturn(true);
            ProcessRuntime container = factory.createContainer(config, userJarFile, userJarFile,
                    userJarFile, userJarFile, 30L);
            args = container.getProcessArgs();
        }

        String classpath = javaInstanceJarFile;
        String extraDepsEnv;
        int portArg;
        int metricsPortArg;
        int totalArgCount = 54;
        if (webServiceUrl != null && config.isExposePulsarAdminClientEnabled()) {
            totalArgCount += 3;
        }
        if (null != depsDir) {
            assertEquals(args.size(), totalArgCount);
            extraDepsEnv = " -Dpulsar.functions.extra.dependencies.dir=" + depsDir;
            classpath = classpath + ":" + depsDir + "/*";
            portArg = 37;
            metricsPortArg = 39;
        } else {
            assertEquals(args.size(), totalArgCount - 1);
            extraDepsEnv = "";
            portArg = 36;
            metricsPortArg = 38;
        }
        if (webServiceUrl != null && config.isExposePulsarAdminClientEnabled()) {
            portArg += 3;
            metricsPortArg += 3;
        }

        String pulsarAdminArg = webServiceUrl != null && config.isExposePulsarAdminClientEnabled()
                ? " --web_serviceurl " + webServiceUrl + " --expose_pulsaradmin" : "";

        String expectedArgs = "java -cp " + classpath
                + extraDepsEnv
                + " -Dpulsar.functions.instance.classpath=/pulsar/lib/*"
                + " -Dlog4j.configurationFile=java_instance_log4j2.xml "
                + "-Dpulsar.function.log.dir=" + logDirectory + "/functions/"
                + FunctionCommon.getFullyQualifiedName(config.getFunctionDetails())
                + " -Dpulsar.function.log.file=" + config.getFunctionDetails().getName() + "-" + config.getInstanceId()
                + " -Dio.netty.tryReflectionSetAccessible=true"
                + " -Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true"
                + " -Dio.grpc.netty.shaded.io.netty.tryReflectionSetAccessible=true"
                + " --add-opens java.base/java.nio=ALL-UNNAMED"
                + " --add-opens java.base/jdk.internal.misc=ALL-UNNAMED"
                + " --add-opens java.base/java.util.zip=ALL-UNNAMED"
                + " org.apache.pulsar.functions.instance.JavaInstanceMain"
                + " --jar " + userJarFile
                + " --transform_function_jar " + userJarFile
                + " --transform_function_id " +  config.getTransformFunctionId()
                + " --instance_id " + config.getInstanceId()
                + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '"
                + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + pulsarAdminArg
                + " --max_buffered_tuples 1024 --port " + args.get(portArg) + " --metrics_port "
                + args.get(metricsPortArg)
                + " --pending_async_requests 200"
                + " --state_storage_serviceurl " + stateStorageServiceUrl
                + " --expected_healthcheck_interval 30"
                + " --secrets_provider org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider"
                + " --secrets_provider_config '{\"Config\":\"Value\"}'"
                + " --cluster_name standalone --nar_extraction_directory " + narExtractionDirectory;
        assertEquals(String.join(" ", args), expectedArgs);
    }

    @Test
    public void testPythonConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        factory = createProcessRuntimeFactory(null);

        verifyPythonInstance(config, null);
    }

    @Test
    public void testPythonConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        String extraDeps = "/path/to/extra/deps";

        factory = createProcessRuntimeFactory(extraDeps);

        verifyPythonInstance(config, extraDeps);
    }

    private void verifyPythonInstance(InstanceConfig config, String extraDepsDir) throws Exception {
        ProcessRuntime container = factory.createContainer(config, userJarFile, null, null,
                null, 30L);
        List<String> args = container.getProcessArgs();

        int totalArgs = 36;
        int portArg = 23;
        int metricsPortArg = 25;
        String pythonPath = "";
        int configArg = 9;

        assertEquals(args.size(), totalArgs);
        String expectedArgs = pythonPath + "python3 " + pythonInstanceFile
                + " --py " + userJarFile + " --logging_directory "
                + logDirectory + "/functions" + " --logging_file " + config.getFunctionDetails().getName()
                + " --logging_config_file " + args.get(configArg) + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '"
                + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(portArg)
                + " --metrics_port " + args.get(metricsPortArg)
                + " --state_storage_serviceurl bk://localhost:4181"
                + " --expected_healthcheck_interval 30"
                + " --secrets_provider secretsprovider.ClearTextSecretsProvider"
                + " --secrets_provider_config '{\"Config\":\"Value\"}'"
                + " --cluster_name standalone";
        assertEquals(String.join(" ", args), expectedArgs);
    }

    @Test
    public void testGoConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.GO);

        factory = createProcessRuntimeFactory(null, null, false,
                AuthenticationConfig.builder()
                        .clientAuthenticationPlugin("com.MyAuth")
                        .clientAuthenticationParameters("{\"authParam1\": \"authParamValue1\"}").build());

        verifyGoInstance(config);
    }

    private void verifyGoInstance(InstanceConfig config) throws Exception {
        String goExec = "/usr/bin/exec";
        ProcessRuntime container = factory.createContainer(config, goExec, null, null, null,30l);
        List<String> args = container.getProcessArgs();

        int totalArgs = 3;

        assertEquals(args.size(), totalArgs);
        assertEquals(args.get(0), goExec);
        assertEquals(args.get(1), "-instance-conf");
        String functionDetails =
                JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails());

        HashMap goInstanceConfig = new ObjectMapper().readValue(args.get(2), HashMap.class);
        assertEquals(goInstanceConfig.get("pulsarServiceURL"), pulsarServiceUrl);
        assertEquals(goInstanceConfig.get("stateStorageServiceUrl"), stateStorageServiceUrl);
        assertEquals(goInstanceConfig.get("pulsarWebServiceUrl"), "");
        assertEquals(goInstanceConfig.get("instanceID"), config.getInstanceId());
        assertEquals(goInstanceConfig.get("funcID"), config.getFunctionId());
        assertEquals(goInstanceConfig.get("funcVersion"), config.getFunctionVersion());
        assertEquals(goInstanceConfig.get("maxBufTuples"), config.getMaxBufferedTuples());
        assertEquals(goInstanceConfig.get("port"), config.getPort());
        assertEquals(goInstanceConfig.get("clusterName"), config.getClusterName());
        assertEquals(goInstanceConfig.get("killAfterIdleMs"), 0);
        assertEquals(goInstanceConfig.get("expectedHealthCheckInterval"), 0);
        assertEquals(goInstanceConfig.get("tenant"), TEST_TENANT);
        assertEquals(goInstanceConfig.get("nameSpace"), TEST_NAMESPACE);
        assertEquals(goInstanceConfig.get("name"), TEST_NAME);
        assertEquals(goInstanceConfig.get("className"), "");
        assertEquals(goInstanceConfig.get("logTopic"), TEST_NAME + "-log");
        assertEquals(goInstanceConfig.get("processingGuarantees"), config.getFunctionDetails().getProcessingGuarantees().getNumber());
        assertEquals(goInstanceConfig.get("secretsMap"), config.getFunctionDetails().getSecretsMap());
        assertEquals(goInstanceConfig.get("userConfig"), config.getFunctionDetails().getUserConfig());
        assertEquals(goInstanceConfig.get("clientAuthenticationPlugin"), "com.MyAuth");
        assertEquals(goInstanceConfig.get("clientAuthenticationParameters"), "{\"authParam1\": \"authParamValue1\"}");
        assertEquals(goInstanceConfig.get("tlsTrustCertsFilePath"), "");
        assertEquals(goInstanceConfig.get("tlsHostnameVerificationEnable"), false);
        assertEquals(goInstanceConfig.get("tlsAllowInsecureConnection"), false);
        assertEquals(goInstanceConfig.get("runtime"), FunctionDetails.Runtime.GO.getNumber());
        assertEquals(goInstanceConfig.get("autoAck"), config.getFunctionDetails().getAutoAck());
        assertEquals(goInstanceConfig.get("parallelism"), config.getFunctionDetails().getParallelism());
        assertEquals(goInstanceConfig.get("subscriptionType"), 0);
        assertEquals(goInstanceConfig.get("timeoutMs"), 0);
        assertEquals(goInstanceConfig.get("subscriptionName"), config.getFunctionDetails().getSource().getSubscriptionName());
        assertEquals(goInstanceConfig.get("cleanupSubscription"), config.getFunctionDetails().getSource().getCleanupSubscription());
        assertEquals(goInstanceConfig.get("subscriptionPosition"), config.getFunctionDetails().getSource().getSubscriptionPosition().getNumber());
        assertEquals(goInstanceConfig.get("sourceSpecsTopic"), "test_src");
        assertEquals(goInstanceConfig.get("sourceSchemaType"), "");
        assertEquals(goInstanceConfig.get("receiverQueueSize"), 0);
        assertEquals(goInstanceConfig.get("sinkSpecsTopic"), "test-function-container-output");
        assertEquals(goInstanceConfig.get("sinkSchemaType"), "");
        assertEquals(goInstanceConfig.get("cpu"), 0.0);
        assertEquals(goInstanceConfig.get("ram"), 0);
        assertEquals(goInstanceConfig.get("disk"), 0);
        assertEquals(goInstanceConfig.get("maxMessageRetries"), 0);
        assertEquals(goInstanceConfig.get("deadLetterTopic"), "");
        assertEquals(goInstanceConfig.get("metricsPort"), config.getMetricsPort());
        assertEquals(goInstanceConfig.get("functionDetails"), functionDetails);
    }

    @Test
    public void testJavaConstructorWithWebServiceUrlAndExposePulsarAdminClientEnabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, true);

        factory = createProcessRuntimeFactory(null, null, true, null);

        verifyJavaInstance(config, null, defaultWebServiceUrl);
    }

    @Test
    public void testJavaConstructorWithWebServiceUrlAndExposePulsarAdminClientDisabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createProcessRuntimeFactory(null, defaultWebServiceUrl, false, null);

        verifyJavaInstance(config, null, defaultWebServiceUrl);
    }

    @Test
    public void testJavaConstructorWithoutWebServiceUrlAndExposePulsarAdminClientEnabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, true);

        factory = createProcessRuntimeFactory(null, null, true, null);

        verifyJavaInstance(config, null, null);
    }

    @Test
    public void testJavaConstructorWithoutWebServiceUrlAndExposePulsarAdminClientDisabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createProcessRuntimeFactory(null, null, false, null);

        verifyJavaInstance(config, null, null);
    }

}
