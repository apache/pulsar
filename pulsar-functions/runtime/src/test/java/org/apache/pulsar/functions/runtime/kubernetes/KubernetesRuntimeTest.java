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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.JsonFormat;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.*;
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
import org.apache.pulsar.functions.worker.WorkerConfig;
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
        public void doAdmissionChecks(AppsV1Api appsV1Api, CoreV1Api coreV1Api, String jobNamespace, FunctionDetails functionDetails) {

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

    @AfterMethod
    public void tearDown() {
        if (null != this.factory) {
            this.factory.close();
        }
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding,
                                                            double cpuOverCommitRatio, double memoryOverCommitRatio,
                                                            Optional<RuntimeCustomizer> manifestCustomizer) throws Exception {
        KubernetesRuntimeFactory factory = spy(new KubernetesRuntimeFactory());

        WorkerConfig workerConfig = new WorkerConfig();
        KubernetesRuntimeFactoryConfig kubernetesRuntimeFactoryConfig = new KubernetesRuntimeFactoryConfig();
        kubernetesRuntimeFactoryConfig.setK8Uri(null);
        kubernetesRuntimeFactoryConfig.setJobNamespace(null);
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

        factory.initialize(workerConfig,null, new TestSecretProviderConfigurator(), Optional.empty(), manifestCustomizer);
        doNothing().when(factory).setupClient();
        return factory;

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

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime, boolean addSecrets) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(runtime, addSecrets));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(0);
        config.setMaxBufferedTuples(1024);
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
        testResouces(1, 1000, 1.0, 1.0);
        testResouces(1, 1000, 2.0, 1.0);
        testResouces(1, 1000, 1.0, 2.0);
        testResouces(1, 1000, 1.5, 1.5);
        testResouces(1, 1000, 1.3, 1.0);

        // test cpu rounding
        testResouces(1.0 / 1.5, 1000, 1.3, 1.0);
    }

    public void testResouces(double userCpuRequest, long userMemoryRequest, double cpuOverCommitRatio, double memoryOverCommitRatio) throws Exception {

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
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);
        List<String> args = container.getProcessArgs();

        String classpath = javaInstanceJarFile;
        String extraDepsEnv;
        int portArg;
        int metricsPortArg;
        int totalArgs;
        if (null != depsDir) {
            extraDepsEnv = " -Dpulsar.functions.extra.dependencies.dir=" + depsDir;
            classpath = classpath + ":" + depsDir + "/*";
            totalArgs = 37;
            portArg = 26;
            metricsPortArg = 28;
        } else {
            extraDepsEnv = "";
            portArg = 25;
            metricsPortArg = 27;
            totalArgs = 36;
        }
        if (secretsAttached) {
            totalArgs += 4;
        }

        assertEquals(args.size(), totalArgs,
                "Actual args : " + StringUtils.join(args, " "));

        String expectedArgs = "exec java -cp " + classpath
                + extraDepsEnv
                + " -Dpulsar.functions.instance.classpath=/pulsar/lib/*"
                + " -Dlog4j.configurationFile=kubernetes_instance_log4j2.xml "
                + "-Dpulsar.function.log.dir=" + logDirectory + "/" + FunctionCommon.getFullyQualifiedName(config.getFunctionDetails())
                + " -Dpulsar.function.log.file=" + config.getFunctionDetails().getName() + "-$SHARD_ID"
                + " -Xmx" + String.valueOf(RESOURCES.getRam())
                + " org.apache.pulsar.functions.instance.JavaInstanceMain"
                + " --jar " + pulsarRootDir + "/" + userJarFile + " --instance_id "
                + "$SHARD_ID" + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '" + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(portArg) + " --metrics_port " + args.get(metricsPortArg)
                + " --state_storage_serviceurl " + stateStorageServiceUrl
                + " --expected_healthcheck_interval -1";
        if (secretsAttached) {
            expectedArgs += " --secrets_provider org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider"
                    + " --secrets_provider_config '{\"Somevalue\":\"myvalue\"}'";
        }
        expectedArgs += " --cluster_name standalone --nar_extraction_directory " + narExtractionDirectory;

        assertEquals(String.join(" ", args), expectedArgs);

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
        final String jobName = KubernetesRuntime.createJobName(functionDetails);
        assertEquals(bcJobName, jobName);
        KubernetesRuntime.doChecks(functionDetails);
    }

    private void verifyCreateJobNameWithUpperCaseFunctionName() throws Exception {
        FunctionDetails functionDetails = createFunctionDetails("UpperCaseFunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails);
        assertEquals(jobName, "pf-tenant-namespace-uppercasefunction-f0c5ca9a");
        KubernetesRuntime.doChecks(functionDetails);
    }

    private void verifyCreateJobNameWithDotFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("clazz.testfunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails);
        assertEquals(jobName, "pf-tenant-namespace-clazz.testfunction");
        KubernetesRuntime.doChecks(functionDetails);
    }

    private void verifyCreateJobNameWithDotAndUpperCaseFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("Clazz.TestFunction");
        final String jobName = KubernetesRuntime.createJobName(functionDetails);
        assertEquals(jobName, "pf-tenant-namespace-clazz.testfunction-92ec5bf6");
        KubernetesRuntime.doChecks(functionDetails);
    }

    private void verifyCreateJobNameWithInvalidMarksFunctionName() throws Exception {
        final FunctionDetails functionDetails = createFunctionDetails("test_function*name");
        final String jobName = KubernetesRuntime.createJobName(functionDetails);
        assertEquals(jobName, "pf-tenant-namespace-test-function-name-b5a215ad");
        KubernetesRuntime.doChecks(functionDetails);
    }

    private void verifyCreateJobNameWithCollisionalFunctionName() throws Exception {
        final FunctionDetails functionDetail1 = createFunctionDetails("testfunction");
        final FunctionDetails functionDetail2 = createFunctionDetails("testFunction");
        final String jobName1 = KubernetesRuntime.createJobName(functionDetail1);
        final String jobName2 = KubernetesRuntime.createJobName(functionDetail2);
        assertNotEquals(jobName1, jobName2);
        KubernetesRuntime.doChecks(functionDetail1);
        KubernetesRuntime.doChecks(functionDetail2);
    }

    private void verifyCreateJobNameWithCollisionalAndInvalidMarksFunctionName() throws Exception {
        final FunctionDetails functionDetail1 = createFunctionDetails("test_function*name");
        final FunctionDetails functionDetail2 = createFunctionDetails("test+function*name");
        final String jobName1 = KubernetesRuntime.createJobName(functionDetail1);
        final String jobName2 = KubernetesRuntime.createJobName(functionDetail2);
        assertNotEquals(jobName1, jobName2);
        KubernetesRuntime.doChecks(functionDetail1);
        KubernetesRuntime.doChecks(functionDetail2);
    }

    @Test
    public void testNoOpKubernetesManifestCustomizer() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            JsonObject configObj = new JsonObject();
            configObj.addProperty("jobNamespace", "custom-ns");

            return fb.setCustomRuntimeOptions(configObj.toString());
        }));

        factory = createKubernetesRuntimeFactory(null, 10, 1.0, 1.0);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
        KubernetesRuntime container = factory.createContainer(config, userJarFile, userJarFile, 30l);

        V1Service serviceSpec = container.createService();
        assertEquals(serviceSpec.getMetadata().getNamespace(), "default");
    }

    @Test
    public void testBasicKubernetesManifestCustomizer() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);
        config.setFunctionDetails(createFunctionDetails(FunctionDetails.Runtime.JAVA, false, (fb) -> {
            JsonObject configObj = new JsonObject();
            configObj.addProperty("jobNamespace", "custom-ns");

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
            requests.addProperty("cpu", 1);
            requests.addProperty("memory", "4G");
            limits.addProperty("cpu", 2);
            limits.addProperty("memory", "8G");
            resourceRequirements.add("requests", requests);
            resourceRequirements.add("limits", limits);
            configObj.add("resourceRequirements", resourceRequirements);

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
}
