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

package org.apache.pulsar.functions.runtime.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.JsonFormat;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1Toleration;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.ConsumerSpec;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntime;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.pulsar.functions.runtime.RuntimeUtils.FUNCTIONS_INSTANCE_CLASSPATH;
import static org.apache.pulsar.functions.utils.FunctionCommon.roundDecimal;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Unit test of {@link ThreadRuntime}.
 */
public class KubernetesRuntimeTest {

    private static final String TEST_TENANT = "tenant";
    private static final String TEST_NAMESPACE = "namespace";
    private static final String TEST_NAME = "container";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    private static final Map<String, ConsumerSpec> topicsToSchema = new HashMap<>();
    private static final Function.Resources RESOURCES = Function.Resources.newBuilder()
            .setRam(1000L).setCpu(1).setDisk(10000L).build();
    private static final String narExtractionDirectory = "/tmp/foo";

    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", "");
        topicsToSchema.put("persistent://sample/standalone/ns1/test_src",
                ConsumerSpec.newBuilder().setSerdeClassName("").setIsRegexPattern(false).build());
    }

    public class TestKubernetesCustomManifestCustomizer implements KubernetesManifestCustomizer {

        @Override
        public V1StatefulSet customizeStatefulSet(Function.FunctionDetails funcDetails, V1StatefulSet statefulSet) {
            assertEquals(funcDetails.getCustomRuntimeOptions(), "custom-service-account");
            statefulSet.getSpec().getTemplate().getSpec().serviceAccountName("my-service-account");
            return statefulSet;
        }

        @Override
        public void initialize(Map<String, Object> config) {

        }
    }

    class TestSecretProviderConfigurator implements SecretsProviderConfigurator {

        @Override
        public void init(Map<String, String> config) {

        }

        @Override
        public String getSecretsProviderClassName(FunctionDetails functionDetails) {
            if (!StringUtils.isEmpty(functionDetails.getSecretsMap())) {
                if (functionDetails.getRuntime() == FunctionDetails.Runtime.JAVA) {
                    return ClearTextSecretsProvider.class.getName();
                } else {
                    return "secretsprovider.ClearTextSecretsProvider";
                }
            } else {
                return null;
            }
        }

        @Override
        public Map<String, String> getSecretsProviderConfig(FunctionDetails functionDetails) {
            HashMap<String, String> map = new HashMap<>();
            map.put("Somevalue", "myvalue");
            return map;
        }

        @Override
        public void configureKubernetesRuntimeSecretsProvider(V1PodSpec podSpec, String functionsContainerName, FunctionDetails functionDetails) {

        }

        @Override
        public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, FunctionDetails functionDetails) {

        }

        @Override
        public Type getSecretObjectType() {
            return null;
        }

        @Override
        public void doAdmissionChecks(AppsV1Api appsV1Api, CoreV1Api coreV1Api, String jobNamespace, String jobName, FunctionDetails functionDetails) {

        }
    }

    private KubernetesRuntimeFactory factory;
    private final String userJarFile;
    private final String pulsarRootDir;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String pulsarServiceUrl;
    private final String pulsarAdminUrl;
    private final String stateStorageServiceUrl;
    private final String logDirectory;

    public KubernetesRuntimeTest() throws Exception {
        this.userJarFile = "UserJar.jar";
        this.pulsarRootDir = "/pulsar";
        this.javaInstanceJarFile = "/pulsar/instances/java-instance.jar";
        this.pythonInstanceFile = "/pulsar/instances/python-instance/python_instance_main.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.pulsarAdminUrl = "http://localhost:8080";
        this.stateStorageServiceUrl = "bk://localhost:4181";
        this.logDirectory = "logs/functions";
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

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding,
                                                            double cpuOverCommitRatio, double memoryOverCommitRatio,
                                                            Optional<RuntimeCustomizer> manifestCustomizer,
                                                            String downloadDirectory) throws Exception {

        KubernetesRuntimeFactory factory = spy(new KubernetesRuntimeFactory());
        doNothing().when(factory).setupClient();

        WorkerConfig workerConfig = new WorkerConfig();
        KubernetesRuntimeFactoryConfig kubernetesRuntimeFactoryConfig = new KubernetesRuntimeFactoryConfig();
        kubernetesRuntimeFactoryConfig.setK8Uri(null);
        kubernetesRuntimeFactoryConfig.setJobNamespace(null);
        kubernetesRuntimeFactoryConfig.setJobName(null);
        kubernetesRuntimeFactoryConfig.setPulsarDockerImageName(null);
        kubernetesRuntimeFactoryConfig.setFunctionDockerImages(null);
        kubernetesRuntimeFactoryConfig.setImagePullPolicy(null);
        kubernetesRuntimeFactoryConfig.setPulsarRootDir(pulsarRootDir);
        kubernetesRuntimeFactoryConfig.setSubmittingInsidePod(false);
        kubernetesRuntimeFactoryConfig.setInstallUserCodeDependencies(true);
        kubernetesRuntimeFactoryConfig.setPythonDependencyRepository("myrepo");
        kubernetesRuntimeFactoryConfig.setPythonExtraDependencyRepository("anotherrepo");
        kubernetesRuntimeFactoryConfig.setExtraFunctionDependenciesDir(extraDepsDir);
        kubernetesRuntimeFactoryConfig.setCustomLabels(null);
        kubernetesRuntimeFactoryConfig.setPercentMemoryPadding(percentMemoryPadding);
        kubernetesRuntimeFactoryConfig.setCpuOverCommitRatio(cpuOverCommitRatio);
        kubernetesRuntimeFactoryConfig.setMemoryOverCommitRatio(memoryOverCommitRatio);
        kubernetesRuntimeFactoryConfig.setPulsarServiceUrl(pulsarServiceUrl);
        kubernetesRuntimeFactoryConfig.setPulsarAdminUrl(pulsarAdminUrl);
        kubernetesRuntimeFactoryConfig.setChangeConfigMapNamespace(null);
        kubernetesRuntimeFactoryConfig.setChangeConfigMap(null);
        kubernetesRuntimeFactoryConfig.setGrpcPort(4332);
        kubernetesRuntimeFactoryConfig.setMetricsPort(4331);
        kubernetesRuntimeFactoryConfig.setNarExtractionDirectory(narExtractionDirectory);
        workerConfig.setFunctionRuntimeFactoryClassName(KubernetesRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(kubernetesRuntimeFactoryConfig, Map.class));
        workerConfig.setFunctionInstanceMinResources(null);
        workerConfig.setStateStorageServiceUrl(stateStorageServiceUrl);
        workerConfig.setAuthenticationEnabled(false);
        workerConfig.setDownloadDirectory(downloadDirectory);

        manifestCustomizer.ifPresent(runtimeCustomizer -> runtimeCustomizer.initialize(Optional.ofNullable(workerConfig.getRuntimeCustomizerConfig()).orElse(Collections.emptyMap())));

        factory.initialize(workerConfig, null, new TestSecretProviderConfigurator(), Mockito.mock(ConnectorsManager.class), Optional.empty(), manifestCustomizer);
        return factory;
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding,
                                                            double cpuOverCommitRatio, double memoryOverCommitRatio,
                                                            Optional<RuntimeCustomizer> manifestCustomizer) throws Exception {
        return createKubernetesRuntimeFactory(extraDepsDir, percentMemoryPadding, cpuOverCommitRatio, memoryOverCommitRatio, manifestCustomizer, null);
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding,
                                                            double cpuOverCommitRatio, double memoryOverCommitRatio) throws Exception {
        return createKubernetesRuntimeFactory(extraDepsDir, percentMemoryPadding, cpuOverCommitRatio, memoryOverCommitRatio, Optional.empty());
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime, boolean addSecrets) {
        return createFunctionDetails(runtime, addSecrets, (fb) -> fb);
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime, boolean addSecrets, java.util.function.Function<FunctionDetails.Builder, FunctionDetails.Builder> customize) {
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
        if (addSecrets) {
            functionDetailsBuilder.setSecretsMap("SomeMap");
        }
        functionDetailsBuilder.setResources(RESOURCES);
        return customize.apply(functionDetailsBuilder).build();
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime, boolean addSecrets,
                                            boolean exposePulsarAdminClientEnabled) {
        InstanceConfig config = createJavaInstanceConfig(runtime, addSecrets);
        config.setExposePulsarAdminClientEnabled(exposePulsarAdminClientEnabled);
        return config;
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime, boolean addSecrets) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(runtime, addSecrets));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(0);
        config.setMaxBufferedTuples(1024);
        config.setMaxPendingAsyncRequests(200);
        config.setClusterName("standalone");

        return config;
    }

    @Test
    public void testRamPadding() throws Exception {
        verifyRamPadding(0, 1000, 1000);
        verifyRamPadding(5, 1000, 1050);
        verifyRamPadding(10, 1000, 1100);
    }

    private void verifyRamPadding(int percentMemoryPadding, long ram, long expectedRamWithPadding) throws Exception {
        factory = createKubernetesRuntimeFactory(null, percentMemoryPadding, 1.0, 1.0);
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, true);

        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);

        Function.Resources resources = Function.Resources.newBuilder().setRam(ram).build();

        V1Container containerSpec = container.getFunctionContainer(Collections.emptyList(), resources);
        Assert.assertEquals(containerSpec.getResources().getLimits().get("memory").getNumber().longValue(), expectedRamWithPadding);
        Assert.assertEquals(containerSpec.getResources().getRequests().get("memory").getNumber().longValue(), expectedRamWithPadding);
    }

    @Test
    public void testJavaConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    @Test
    public void testJavaConstructorWithSecrets() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, true);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", true);
    }

    @Test
    public void testJavaConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        String extraDepsDir = "/path/to/deps/dir";

        factory = createKubernetesRuntimeFactory(extraDepsDir, 10, 1.0, 1.0);

        verifyJavaInstance(config, extraDepsDir, false);
    }

    @Test
    public void testResources() throws Exception {

        // test overcommit
    	testResources(1, 1000, 1.0, 1.0);
    	testResources(1, 1000, 2.0, 1.0);
    	testResources(1, 1000, 1.0, 2.0);
    	testResources(1, 1000, 1.5, 1.5);
    	testResources(1, 1000, 1.3, 1.0);

        // test cpu rounding
    	testResources(1.0 / 1.5, 1000, 1.3, 1.0);
    }

    public void testResources(double userCpuRequest, long userMemoryRequest, double cpuOverCommitRatio, double memoryOverCommitRatio) throws Exception {

        Function.Resources resources = Function.Resources.newBuilder()
                .setRam(userMemoryRequest).setCpu(userCpuRequest).setDisk(10000L).build();

        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        factory = createKubernetesRuntimeFactory(null, 10, cpuOverCommitRatio, memoryOverCommitRatio);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        List<String> args = container.getProcessArgs();

        // check padding and xmx
        long heap = Long.parseLong(args.stream().filter(s -> s.startsWith("-Xmx")).collect(Collectors.toList()).get(0).replace("-Xmx", ""));
        V1Container containerSpec = container.getFunctionContainer(Collections.emptyList(), resources);
        assertEquals(containerSpec.getResources().getLimits().get("memory").getNumber().longValue(), Math.round(heap + (heap * 0.1)));
        assertEquals(containerSpec.getResources().getRequests().get("memory").getNumber().longValue(), Math.round((heap + (heap * 0.1)) / memoryOverCommitRatio));

        // check cpu
        assertEquals(containerSpec.getResources().getRequests().get("cpu").getNumber().doubleValue(), roundDecimal(resources.getCpu() / cpuOverCommitRatio, 3));
        assertEquals(containerSpec.getResources().getLimits().get("cpu").getNumber().doubleValue(), roundDecimal(resources.getCpu(), 3));
    }

    private void verifyJavaInstance(InstanceConfig config, String depsDir, boolean secretsAttached) throws Exception {
        verifyJavaInstance(config, depsDir, secretsAttached, null);
    }

    private void verifyJavaInstance(InstanceConfig config, String depsDir, boolean secretsAttached, String downloadDirectory) throws Exception {
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        List<String> args = container.getProcessArgs();

        String classpath = javaInstanceJarFile;
        String extraDepsEnv;
        String jarLocation;
        int portArg;
        int metricsPortArg;
        int totalArgs;
        if (null != depsDir) {
            extraDepsEnv = " -Dpulsar.functions.extra.dependencies.dir=" + depsDir;
            classpath = classpath + ":" + depsDir + "/*";
            totalArgs = 40;
            portArg = 26;
            metricsPortArg = 28;
        } else {
            extraDepsEnv = "";
            portArg = 25;
            metricsPortArg = 27;
            totalArgs = 39;
        }
        if (secretsAttached) {
            totalArgs += 4;
        }
        if (config.isExposePulsarAdminClientEnabled()) {
            totalArgs += 3;
            portArg += 3;
            metricsPortArg += 3;
        }

        if (StringUtils.isNotEmpty(downloadDirectory)){
            jarLocation = downloadDirectory + "/" + userJarFile;
        } else {
            jarLocation = pulsarRootDir + "/" + userJarFile;
        }


        assertEquals(args.size(), totalArgs,
                "Actual args : " + StringUtils.join(args, " "));

        String pulsarAdminArg = config.isExposePulsarAdminClientEnabled() ?
                " --web_serviceurl " + pulsarAdminUrl + " --expose_pulsaradmin": "";

        String expectedArgs = "exec java -cp " + classpath
                + extraDepsEnv
                + " -Dpulsar.functions.instance.classpath=/pulsar/lib/*"
                + " -Dlog4j.configurationFile=kubernetes_instance_log4j2.xml "
                + "-Dpulsar.function.log.dir=" + logDirectory + "/" + FunctionCommon.getFullyQualifiedName(config.getFunctionDetails())
                + " -Dpulsar.function.log.file=" + config.getFunctionDetails().getName() + "-$SHARD_ID"
                + " -Dio.netty.tryReflectionSetAccessible=true -Xmx" + String.valueOf(RESOURCES.getRam())
                + " org.apache.pulsar.functions.instance.JavaInstanceMain"
                + " --jar " + jarLocation + " --instance_id "
                + "$SHARD_ID" + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '" + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + pulsarAdminArg
                + " --max_buffered_tuples 1024 --port " + args.get(portArg) + " --metrics_port " + args.get(metricsPortArg)
                + " --pending_async_requests 200"
                + " --state_storage_serviceurl " + stateStorageServiceUrl
                + " --expected_healthcheck_interval -1";
        if (secretsAttached) {
            expectedArgs += " --secrets_provider org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider"
                    + " --secrets_provider_config '{\"Somevalue\":\"myvalue\"}'";
        }
        expectedArgs += " --cluster_name standalone --nar_extraction_directory " + narExtractionDirectory;


        // check padding and xmx
        long heap = Long.parseLong(args.stream().filter(s -> s.startsWith("-Xmx")).collect(Collectors.toList()).get(0).replace("-Xmx", ""));
        V1Container containerSpec = container.getFunctionContainer(Collections.emptyList(), RESOURCES);
        assertEquals(heap, RESOURCES.getRam());
        assertEquals(containerSpec.getResources().getLimits().get("memory").getNumber().longValue(), Math.round(heap + (heap * 0.1)));

        // check cpu
        assertEquals(containerSpec.getResources().getRequests().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
        assertEquals(containerSpec.getResources().getLimits().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
    }

    @Test
    public void testPythonConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyPythonInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    @Test
    public void testPythonConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON, false);

        String extraDepsDir = "/path/to/deps/dir";

        factory = createKubernetesRuntimeFactory(extraDepsDir, 10, 1.0, 1.0);

        verifyPythonInstance(config, extraDepsDir, false);
    }

    private void verifyPythonInstance(InstanceConfig config, String extraDepsDir, boolean secretsAttached) throws Exception {
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        List<String> args = container.getProcessArgs();

        int totalArgs;
        int portArg;
        String pythonPath;
        int configArg;
        int metricsPortArg;
        if (null == extraDepsDir) {
            totalArgs = 37;
            portArg = 30;
            configArg = 10;
            pythonPath = "";
            metricsPortArg = 32;
        } else {
            totalArgs = 40;
            portArg = 31;
            configArg = 11;
            metricsPortArg = 33;
            pythonPath = "PYTHONPATH=${PYTHONPATH}:" + extraDepsDir + " ";
        }
        if (secretsAttached) {
            totalArgs += 4;
        }

        assertEquals(args.size(), totalArgs,
                "Actual args : " + StringUtils.join(args, " "));
        String expectedArgs = pythonPath + "exec python " + pythonInstanceFile
                + " --py " + pulsarRootDir + "/" + userJarFile
                + " --logging_directory " + logDirectory
                + " --logging_file " + config.getFunctionDetails().getName()
                + " --logging_config_file " + args.get(configArg)
                + " --install_usercode_dependencies True"
                + " --dependency_repository myrepo"
                + " --extra_dependency_repository anotherrepo"
                + " --instance_id " + "$SHARD_ID"
                + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '" + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(portArg) + " --metrics_port " + args.get(metricsPortArg)
                + " --state_storage_serviceurl bk://localhost:4181"
                + " --expected_healthcheck_interval -1";
        if (secretsAttached) {
            expectedArgs += " --secrets_provider secretsprovider.ClearTextSecretsProvider"
                    + " --secrets_provider_config '{\"Somevalue\":\"myvalue\"}'";
        }
        expectedArgs += " --cluster_name standalone";
        assertEquals(String.join(" ", args), expectedArgs);

        // check padding and xmx
        V1Container containerSpec = container.getFunctionContainer(Collections.emptyList(), RESOURCES);
        assertEquals(containerSpec.getResources().getLimits().get("memory").getNumber().longValue(),
                Math.round(RESOURCES.getRam() + (RESOURCES.getRam() * 0.1)));

        // check cpu
        assertEquals(containerSpec.getResources().getRequests().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
        assertEquals(containerSpec.getResources().getLimits().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
    }
    
    @Test
    public void testCreateJobName() throws Exception {    
        verifyCreateJobNameWithBackwardCompatibility();
        verifyCreateJobNameWithUpperCaseFunctionName();
        verifyCreateJobNameWithDotFunctionName();
        verifyCreateJobNameWithDotAndUpperCaseFunctionName();
        verifyCreateJobNameWithInvalidMarksFunctionName();
        verifyCreateJobNameWithCollisionalFunctionName();
        verifyCreateJobNameWithCollisionalAndInvalidMarksFunctionName();
        verifyCreateJobNameWithOverriddenK8sPodNameNoCollisionWithSameName();
        verifyCreateJobNameWithOverriddenK8sPodName();
        verifyCreateJobNameWithOverriddenK8sPodNameWithInvalidMarks();
        verifyCreateJobNameWithNameOverMaxCharLimit();
    }

    @Test
    public void testCreateFunctionLabels() throws Exception {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setTenant("tenant");
        // use long namespace to make sure label is truncated to 63 max characters for k8s requirements
        functionDetailsBuilder.setNamespace(String.format("%-100s", "namespace:$second.part:third@test_0").replace(" ", "0"));
        functionDetailsBuilder.setName("$function_name!");
        JsonObject configObj = new JsonObject();
        configObj.addProperty("jobNamespace", "custom-ns");
        configObj.addProperty("jobName", "custom-name");
        functionDetailsBuilder.setCustomRuntimeOptions(configObj.toString());
        final FunctionDetails functionDetails = functionDetailsBuilder.build();

        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(functionDetails);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();

        assertEquals(spec.getMetadata().getLabels().get("tenant"), "tenant");
        assertEquals(spec.getMetadata().getLabels().get("namespace"), String.format("%-63s", "namespace--second.part-third-test_0").replace(" ", "0"));
        assertEquals(spec.getMetadata().getLabels().get("name"), "0function_name0");
    }

    FunctionDetails createFunctionDetails(final String functionName) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setTenant(TEST_TENANT);
        functionDetailsBuilder.setNamespace(TEST_NAMESPACE);
        functionDetailsBuilder.setName(functionName);
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
        functionDetailsBuilder.setSecretsMap("SomeMap");
        functionDetailsBuilder.setResources(RESOURCES);
        return functionDetailsBuilder.build();
    }

    // used for backward compatibility test
    private String bcCreateJobName(String tenant, String namespace, String functionName) {
        return "pf-" + tenant + "-" + namespace + "-" + functionName;
    }

    private void verifyCreateJobNameWithBackwardCompatibility() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails(TEST_NAME);
        final String bcJobName = bcCreateJobName(functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
        final String jobName = KubernetesRuntime.createJobName(functionDetails, null);
        assertEquals(bcJobName, jobName);
        KubernetesRuntime.doChecks(functionDetails, null);
    }

    private void verifyCreateJobNameWithUpperCaseFunctionName() throws Exception {
        FunctionDetails functionDetails = createFunctionDetails("UpperCaseFunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, null);
        assertEquals(jobName, "pf-tenant-namespace-uppercasefunction-f0c5ca9a");
        KubernetesRuntime.doChecks(functionDetails, null);
    }

    private void verifyCreateJobNameWithDotFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("clazz.testfunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, null);
        assertEquals(jobName, "pf-tenant-namespace-clazz.testfunction");
        KubernetesRuntime.doChecks(functionDetails, null);
    }

    private void verifyCreateJobNameWithDotAndUpperCaseFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("Clazz.TestFunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, null);
        assertEquals(jobName, "pf-tenant-namespace-clazz.testfunction-92ec5bf6");
        KubernetesRuntime.doChecks(functionDetails, null);
    }

    private void verifyCreateJobNameWithInvalidMarksFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("test_function*name");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, null);
        assertEquals(jobName, "pf-tenant-namespace-test-function-name-b5a215ad");
        KubernetesRuntime.doChecks(functionDetails, null);
    }
    
    private void verifyCreateJobNameWithOverriddenK8sPodName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("clazz.testfunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, "custom-k8s-pod-name");
        assertEquals(jobName, "custom-k8s-pod-name-dedfc7cf");
        KubernetesRuntime.doChecks(functionDetails, "custom-k8s-pod-name");
    }
    
    private void verifyCreateJobNameWithOverriddenK8sPodNameWithInvalidMarks() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("clazz.testfunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails, "invalid_pod*name");
        assertEquals(jobName, "invalid-pod-name-af8c3a6c");
        KubernetesRuntime.doChecks(functionDetails, "invalid_pod*name");
    }
    
    private void verifyCreateJobNameWithOverriddenK8sPodNameNoCollisionWithSameName() throws Exception {
        final String CUSTOM_JOB_NAME = "custom-name";
        final String FUNCTION_NAME = "clazz.testfunction";
        
    	final FunctionDetails functionDetails1 = createFunctionDetails(FUNCTION_NAME);
        final String jobName1 = KubernetesRuntime.createJobName(functionDetails1, CUSTOM_JOB_NAME);
        
        // create a second function with the same name, but in different tenant/namespace to make sure collision does not
        // happen. If tenant, namespace, and function name are the same kubernetes handles collision issues
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setTenant("tenantA");
        functionDetailsBuilder.setNamespace("nsA");
        functionDetailsBuilder.setName(FUNCTION_NAME);
        final FunctionDetails functionDetails2 = functionDetailsBuilder.build();
        final String jobName2 = KubernetesRuntime.createJobName(functionDetails2, CUSTOM_JOB_NAME);
        
        // create a third function with different name but in same tenant/namespace to make sure
        // collision does not happen. If tenant, namespace, and function name are the same kubernetes handles collision issues
        final FunctionDetails functionDetails3 = createFunctionDetails(FUNCTION_NAME + "-extra");
        final String jobName3 = KubernetesRuntime.createJobName(functionDetails3, CUSTOM_JOB_NAME);

        assertEquals(jobName1, CUSTOM_JOB_NAME + "-85ac54b0");
        KubernetesRuntime.doChecks(functionDetails1, CUSTOM_JOB_NAME);

        assertEquals(jobName2, CUSTOM_JOB_NAME + "-c66edfe1");
        KubernetesRuntime.doChecks(functionDetails2, CUSTOM_JOB_NAME);
        
        assertEquals(jobName3, CUSTOM_JOB_NAME + "-0fc9c728");
        KubernetesRuntime.doChecks(functionDetails3, CUSTOM_JOB_NAME);
    }
    
    private void verifyCreateJobNameWithNameOverMaxCharLimit() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("clazz.testfunction");
        assertThrows(RuntimeException.class, () -> KubernetesRuntime.doChecks(functionDetails, 
        		"custom-k8s-pod-name-over-kuberenetes-max-character-limit-123456789"));
    }

    private void verifyCreateJobNameWithCollisionalFunctionName() throws Exception {
        final FunctionDetails functionDetail1 = createFunctionDetails("testfunction");
        final FunctionDetails functionDetail2 = createFunctionDetails("testFunction");
        final String jobName1 = KubernetesRuntime.createJobName(functionDetail1, null);
        final String jobName2 = KubernetesRuntime.createJobName(functionDetail2, null);
        assertNotEquals(jobName1, jobName2);
        KubernetesRuntime.doChecks(functionDetail1, null);
        KubernetesRuntime.doChecks(functionDetail2, null);
    }

    private void verifyCreateJobNameWithCollisionalAndInvalidMarksFunctionName() throws Exception {
        final FunctionDetails functionDetail1 = createFunctionDetails("test_function*name");
        final FunctionDetails functionDetail2 = createFunctionDetails("test+function*name");
        final String jobName1 = KubernetesRuntime.createJobName(functionDetail1, null);
        final String jobName2 = KubernetesRuntime.createJobName(functionDetail2, null);
        assertNotEquals(jobName1, jobName2);
        KubernetesRuntime.doChecks(functionDetail1, null);
        KubernetesRuntime.doChecks(functionDetail2, null);
    }

    @Test
    public void testNoOpKubernetesManifestCustomizer() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            JsonObject configObj = new JsonObject();
            configObj.addProperty("jobNamespace", "custom-ns");
            configObj.addProperty("jobName", "custom-name");

            return fb.setCustomRuntimeOptions(configObj.toString());
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);

        V1Service serviceSpec = container.createService();
        assertEquals(serviceSpec.getMetadata().getNamespace(), "default");
        assertEquals(serviceSpec.getMetadata().getName(), "pf-" + TEST_TENANT + "-" + 
        		TEST_NAMESPACE + "-" + TEST_NAME);
    }

    @Test
    public void testBasicKubernetesManifestCustomizer() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            JsonObject configObj = createRuntimeCustomizerConfig();
            return fb.setCustomRuntimeOptions(configObj.toString());
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.of(new BasicKubernetesManifestCustomizer()));

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        assertEquals(spec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(spec.getMetadata().getLabels().get("label"), "test");
        assertEquals(spec.getSpec().getTemplate().getSpec().getNodeSelector().get("selector"), "test");
        List<V1Toleration> tols = spec.getSpec().getTemplate().getSpec().getTolerations();
        // we add three by default, plus our custom
        assertEquals(tols.size(), 4);
        assertEquals(tols.get(3).getKey(), "test");
        assertEquals(tols.get(3).getValue(), "test");
        assertEquals(tols.get(3).getEffect(), "test");

        V1Service serviceSpec = container.createService();
        assertEquals(serviceSpec.getMetadata().getNamespace(), "custom-ns");
        assertEquals(serviceSpec.getMetadata().getName(), "custom-name-2deb2c2b");
        assertEquals(serviceSpec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(serviceSpec.getMetadata().getLabels().get("label"), "test");

        List<V1Container> containers = spec.getSpec().getTemplate().getSpec().getContainers();
        containers.forEach(c -> {
            V1ResourceRequirements resources = c.getResources();
            Map<String, Quantity> limits = resources.getLimits();
            Map<String, Quantity> requests = resources.getRequests();
            assertEquals(requests.get("cpu").getNumber(), new BigDecimal(1) );
            assertEquals(limits.get("cpu").getNumber(), new BigDecimal(2) );
            assertEquals(requests.get("memory").getNumber(), new BigDecimal(4000000000L) );
            assertEquals(limits.get("memory").getNumber(), new BigDecimal(8000000000L) );
        });

    }

    @Test
    public void testCustomKubernetesManifestCustomizer() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            return fb.setCustomRuntimeOptions("custom-service-account");
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.of(new TestKubernetesCustomManifestCustomizer()));

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        assertEquals(spec.getSpec().getTemplate().getSpec().getServiceAccountName(), "my-service-account");
    }

    @Test
    public void testCustomKubernetesDownloadCommands() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            return fb.setPackageUrl("function://public/default/test@v1");
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        String expectedDownloadCommand = "pulsar-admin --admin-url http://localhost:8080 packages download "
            + "function://public/default/test@v1 --path " + pulsarRootDir + "/" + userJarFile;
        String containerCommand = spec.getSpec().getTemplate().getSpec().getContainers().get(0).getCommand().get(2);
        assertTrue(containerCommand.contains(expectedDownloadCommand));
    }

    InstanceConfig createGolangInstanceConfig() {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.GO, false));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(0);
        config.setMaxBufferedTuples(1024);
        config.setClusterName("standalone");
        config.setMetricsPort(4331);

        return config;
    }

    @Test
    public void testGolangConstructor() throws Exception {
        InstanceConfig config = createGolangInstanceConfig();

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyGolangInstance(config);
    }

    private void verifyGolangInstance(InstanceConfig config) throws Exception {
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        List<String> args = container.getProcessArgs();

        int totalArgs = 8;

        assertEquals(args.size(), totalArgs,
                "Actual args : " + StringUtils.join(args, " "));

        HashMap goInstanceConfig = new ObjectMapper().readValue(args.get(7).replaceAll("^\'|\'$", ""), HashMap.class);

        assertEquals(args.get(0), "chmod");
        assertEquals(args.get(1), "777");
        assertEquals(args.get(2), pulsarRootDir + "/" + userJarFile);
        assertEquals(args.get(3), "&&");
        assertEquals(args.get(4), "exec");
        assertEquals(args.get(5), pulsarRootDir + "/" + userJarFile);
        assertEquals(args.get(6), "-instance-conf");
        assertEquals(goInstanceConfig.get("maxBufTuples"), 1024);
        assertEquals(goInstanceConfig.get("maxMessageRetries"), 0);
        assertEquals(goInstanceConfig.get("killAfterIdleMs"), 0);
        assertEquals(goInstanceConfig.get("parallelism"), 0);
        assertEquals(goInstanceConfig.get("className"), "");
        assertEquals(goInstanceConfig.get("sourceSpecsTopic"), "persistent://sample/standalone/ns1/test_src");
        assertEquals(goInstanceConfig.get("sourceSchemaType"), "");
        assertEquals(goInstanceConfig.get("sinkSpecsTopic"), TEST_NAME + "-output");
        assertEquals(goInstanceConfig.get("clusterName"), "standalone");
        assertEquals(goInstanceConfig.get("nameSpace"), TEST_NAMESPACE);
        assertEquals(goInstanceConfig.get("receiverQueueSize"), 0);
        assertEquals(goInstanceConfig.get("tenant"), TEST_TENANT);
        assertEquals(goInstanceConfig.get("logTopic"), TEST_NAME + "-log");
        assertEquals(goInstanceConfig.get("processingGuarantees"), 0);
        assertEquals(goInstanceConfig.get("autoAck"), false);
        assertEquals(goInstanceConfig.get("regexPatternSubscription"), false);
        assertEquals(goInstanceConfig.get("pulsarServiceURL"), pulsarServiceUrl);
        assertEquals(goInstanceConfig.get("runtime"), 0);
        assertEquals(goInstanceConfig.get("cpu"), 1.0);
        assertEquals(goInstanceConfig.get("funcVersion"), "1.0");
        assertEquals(goInstanceConfig.get("disk"), 10000);
        assertEquals(goInstanceConfig.get("instanceID"), 0);
        assertEquals(goInstanceConfig.get("cleanupSubscription"), false);
        assertEquals(goInstanceConfig.get("port"), 0);
        assertEquals(goInstanceConfig.get("subscriptionType"), 0);
        assertEquals(goInstanceConfig.get("timeoutMs"), 0);
        assertEquals(goInstanceConfig.get("subscriptionName"), "");
        assertEquals(goInstanceConfig.get("name"), TEST_NAME);
        assertEquals(goInstanceConfig.get("expectedHealthCheckInterval"), 0);
        assertEquals(goInstanceConfig.get("deadLetterTopic"), "");
        assertEquals(goInstanceConfig.get("metricsPort"), 4331);

        // check padding and xmx
        V1Container containerSpec = container.getFunctionContainer(Collections.emptyList(), RESOURCES);
        assertEquals(containerSpec.getResources().getLimits().get("memory").getNumber().longValue(),
                Math.round(RESOURCES.getRam() + (RESOURCES.getRam() * 0.1)));

        // check cpu
        assertEquals(containerSpec.getResources().getRequests().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
        assertEquals(containerSpec.getResources().getLimits().get("cpu").getNumber().doubleValue(), RESOURCES.getCpu());
    }

    @Test
    public void testKubernetesRuntimeWithExposeAdminClientEnabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false, true);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    @Test
    public void testKubernetesRuntimeWithExposeAdminClientDisabled() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding,
                                                            double cpuOverCommitRatio, double memoryOverCommitRatio,
                                                            String manifestCustomizerClassName,
                                                            Map<String, Object> runtimeCustomizerConfig) throws Exception {
        KubernetesRuntimeFactory factory = spy(new KubernetesRuntimeFactory());
        doNothing().when(factory).setupClient();

        WorkerConfig workerConfig = new WorkerConfig();
        KubernetesRuntimeFactoryConfig kubernetesRuntimeFactoryConfig = new KubernetesRuntimeFactoryConfig();
        kubernetesRuntimeFactoryConfig.setK8Uri(null);
        kubernetesRuntimeFactoryConfig.setJobNamespace(null);
        kubernetesRuntimeFactoryConfig.setJobName(null);
        kubernetesRuntimeFactoryConfig.setPulsarDockerImageName(null);
        kubernetesRuntimeFactoryConfig.setFunctionDockerImages(null);
        kubernetesRuntimeFactoryConfig.setImagePullPolicy(null);
        kubernetesRuntimeFactoryConfig.setPulsarRootDir(pulsarRootDir);
        kubernetesRuntimeFactoryConfig.setSubmittingInsidePod(false);
        kubernetesRuntimeFactoryConfig.setInstallUserCodeDependencies(true);
        kubernetesRuntimeFactoryConfig.setPythonDependencyRepository("myrepo");
        kubernetesRuntimeFactoryConfig.setPythonExtraDependencyRepository("anotherrepo");
        kubernetesRuntimeFactoryConfig.setExtraFunctionDependenciesDir(extraDepsDir);
        kubernetesRuntimeFactoryConfig.setCustomLabels(null);
        kubernetesRuntimeFactoryConfig.setPercentMemoryPadding(percentMemoryPadding);
        kubernetesRuntimeFactoryConfig.setCpuOverCommitRatio(cpuOverCommitRatio);
        kubernetesRuntimeFactoryConfig.setMemoryOverCommitRatio(memoryOverCommitRatio);
        kubernetesRuntimeFactoryConfig.setPulsarServiceUrl(pulsarServiceUrl);
        kubernetesRuntimeFactoryConfig.setPulsarAdminUrl(pulsarAdminUrl);
        kubernetesRuntimeFactoryConfig.setChangeConfigMapNamespace(null);
        kubernetesRuntimeFactoryConfig.setChangeConfigMap(null);
        kubernetesRuntimeFactoryConfig.setGrpcPort(4332);
        kubernetesRuntimeFactoryConfig.setMetricsPort(4331);
        kubernetesRuntimeFactoryConfig.setNarExtractionDirectory(narExtractionDirectory);
        workerConfig.setFunctionRuntimeFactoryClassName(KubernetesRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(kubernetesRuntimeFactoryConfig, Map.class));
        workerConfig.setFunctionInstanceMinResources(null);
        workerConfig.setStateStorageServiceUrl(stateStorageServiceUrl);
        workerConfig.setAuthenticationEnabled(false);
        workerConfig.setRuntimeCustomizerConfig(runtimeCustomizerConfig);
        workerConfig.setRuntimeCustomizerClassName(manifestCustomizerClassName);

        Optional<RuntimeCustomizer> manifestCustomizer = Optional.empty();
        if (!org.apache.commons.lang3.StringUtils.isEmpty(workerConfig.getRuntimeCustomizerClassName())) {
            manifestCustomizer = Optional.of(RuntimeCustomizer.getRuntimeCustomizer(workerConfig.getRuntimeCustomizerClassName()));
            manifestCustomizer.get().initialize(Optional.ofNullable(workerConfig.getRuntimeCustomizerConfig()).orElse(Collections.emptyMap()));
        }

        factory.initialize(workerConfig, null, new TestSecretProviderConfigurator(),
                Mockito.mock(ConnectorsManager.class), Optional.empty(), manifestCustomizer);
        return factory;
    }

    public static JsonObject createRuntimeCustomizerConfig() {
        JsonObject configObj = new JsonObject();
        configObj.addProperty("jobNamespace", "custom-ns");
        configObj.addProperty("jobName", "custom-name");

        JsonObject extraAnn = new JsonObject();
        extraAnn.addProperty("annotation", "test");
        configObj.add("extraAnnotations", extraAnn);

        JsonObject extraLabel = new JsonObject();
        extraLabel.addProperty("label", "test");
        configObj.add("extraLabels", extraLabel);

        JsonObject nodeLabels = new JsonObject();
        nodeLabels.addProperty("selector", "test");
        configObj.add("nodeSelectorLabels", nodeLabels);

        JsonArray tolerations = new JsonArray();
        JsonObject toleration = new JsonObject();
        toleration.addProperty("key", "test");
        toleration.addProperty("value", "test");
        toleration.addProperty("effect", "test");
        tolerations.add(toleration);
        configObj.add("tolerations", tolerations);

        JsonObject resourceRequirements = new JsonObject();
        JsonObject requests = new JsonObject();
        JsonObject limits = new JsonObject();
        requests.addProperty("cpu", "1");
        requests.addProperty("memory", "4G");
        limits.addProperty("cpu", "2");
        limits.addProperty("memory", "8G");
        resourceRequirements.add("requests", requests);
        resourceRequirements.add("limits", limits);
        configObj.add("resourceRequirements", resourceRequirements);
        return configObj;
    }

    @Test
    public void testBasicKubernetesManifestCustomizerWithRuntimeCustomizerConfig() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        Map<String, Object> configs = new Gson().fromJson(createRuntimeCustomizerConfig(), HashMap.class);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0,
                "org.apache.pulsar.functions.runtime.kubernetes.BasicKubernetesManifestCustomizer", configs);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        assertEquals(spec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(spec.getMetadata().getLabels().get("label"), "test");
        assertEquals(spec.getSpec().getTemplate().getSpec().getNodeSelector().get("selector"), "test");
        List<V1Toleration> tols = spec.getSpec().getTemplate().getSpec().getTolerations();
        // we add three by default, plus our custom
        assertEquals(tols.size(), 4);
        assertEquals(tols.get(3).getKey(), "test");
        assertEquals(tols.get(3).getValue(), "test");
        assertEquals(tols.get(3).getEffect(), "test");

        V1Service serviceSpec = container.createService();
        assertEquals(serviceSpec.getMetadata().getNamespace(), "custom-ns");
        assertEquals(serviceSpec.getMetadata().getName(), "custom-name-2deb2c2b");
        assertEquals(serviceSpec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(serviceSpec.getMetadata().getLabels().get("label"), "test");

        List<V1Container> containers = spec.getSpec().getTemplate().getSpec().getContainers();
        containers.forEach(c -> {
            V1ResourceRequirements resources = c.getResources();
            Map<String, Quantity> limits = resources.getLimits();
            Map<String, Quantity> requests = resources.getRequests();
            assertEquals(requests.get("cpu").getNumber(), new BigDecimal(1) );
            assertEquals(limits.get("cpu").getNumber(), new BigDecimal(2) );
            assertEquals(requests.get("memory").getNumber(), new BigDecimal(4000000000L) );
            assertEquals(limits.get("memory").getNumber(), new BigDecimal(8000000000L) );
        });

    }


    @Test
    public void testBasicKubernetesManifestCustomizerWithRuntimeCustomizerConfigOverwrite() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            JsonObject configObj = new JsonObject();
            configObj.addProperty("jobNamespace", "custom-ns-overwrite");
            configObj.addProperty("jobName", "custom-name-overwrite");
            return fb.setCustomRuntimeOptions(configObj.toString());
        }));

        Map<String, Object> configs = new Gson().fromJson(createRuntimeCustomizerConfig(), HashMap.class);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0,
                "org.apache.pulsar.functions.runtime.kubernetes.BasicKubernetesManifestCustomizer", configs);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        assertEquals(spec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(spec.getMetadata().getLabels().get("label"), "test");
        assertEquals(spec.getSpec().getTemplate().getSpec().getNodeSelector().get("selector"), "test");
        List<V1Toleration> tols = spec.getSpec().getTemplate().getSpec().getTolerations();
        // we add three by default, plus our custom
        assertEquals(tols.size(), 4);
        assertEquals(tols.get(3).getKey(), "test");
        assertEquals(tols.get(3).getValue(), "test");
        assertEquals(tols.get(3).getEffect(), "test");

        V1Service serviceSpec = container.createService();
        assertEquals(serviceSpec.getMetadata().getNamespace(), "custom-ns-overwrite");
        assertEquals(serviceSpec.getMetadata().getName(), "custom-name-overwrite-7757f1ff");
        assertEquals(serviceSpec.getMetadata().getAnnotations().get("annotation"), "test");
        assertEquals(serviceSpec.getMetadata().getLabels().get("label"), "test");

        List<V1Container> containers = spec.getSpec().getTemplate().getSpec().getContainers();
        containers.forEach(c -> {
            V1ResourceRequirements resources = c.getResources();
            Map<String, Quantity> limits = resources.getLimits();
            Map<String, Quantity> requests = resources.getRequests();
            assertEquals(requests.get("cpu").getNumber(), new BigDecimal(1) );
            assertEquals(limits.get("cpu").getNumber(), new BigDecimal(2) );
            assertEquals(requests.get("memory").getNumber(), new BigDecimal(4000000000L) );
            assertEquals(limits.get("memory").getNumber(), new BigDecimal(8000000000L) );
        });

    }

    @Test
    public void testJavaConstructorWithoutDownloadDirectoryDefined() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.empty(), null);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false, factory.getDownloadDirectory());
    }

    @Test
    public void testJavaConstructorWithDownloadDirectoryDefined() throws Exception {
        String downloadDirectory = "download/pulsar_functions";
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.empty(), downloadDirectory);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false, factory.getDownloadDirectory());
    }

    @Test
    public void testJavaConstructorWithAbsolutDownloadDirectoryDefined() throws Exception {
        String downloadDirectory = "/functions/download/pulsar_functions";
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.empty(), downloadDirectory);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false, factory.getDownloadDirectory());
    }

    @Test
    public void testCustomKubernetesDownloadCommandsWithDownloadDirectoryDefined() throws Exception {
        String downloadDirectory = "download/pulsar_functions";
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            return fb.setPackageUrl("function://public/default/test@v1");
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0, Optional.empty(), downloadDirectory);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false, factory.getDownloadDirectory());
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        V1StatefulSet spec = container.createStatefulSet();
        String expectedDownloadCommand = "pulsar-admin --admin-url http://localhost:8080 packages download "
                + "function://public/default/test@v1 --path " + factory.getDownloadDirectory() + "/" + userJarFile;
        String containerCommand = spec.getSpec().getTemplate().getSpec().getContainers().get(0).getCommand().get(2);
        assertTrue(containerCommand.contains(expectedDownloadCommand));
    }
}
