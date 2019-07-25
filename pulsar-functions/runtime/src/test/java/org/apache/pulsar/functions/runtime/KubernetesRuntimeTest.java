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

package org.apache.pulsar.functions.runtime;

import com.google.protobuf.util.JsonFormat;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1PodSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.ConsumerSpec;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.pulsar.functions.runtime.RuntimeUtils.FUNCTIONS_INSTANCE_CLASSPATH;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.testng.Assert.assertEquals;

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

    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", "");
        topicsToSchema.put("persistent://sample/standalone/ns1/test_src",
                ConsumerSpec.newBuilder().setSerdeClassName("").setIsRegexPattern(false).build());
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

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, int percentMemoryPadding) throws Exception {
        KubernetesRuntimeFactory factory = spy(new KubernetesRuntimeFactory(
            null,
            null,
            null,
            null,
            pulsarRootDir,
            false,
            true,
            "myrepo",
            "anotherrepo",
            extraDepsDir,
            null,
                percentMemoryPadding,
                pulsarServiceUrl,
            pulsarAdminUrl,
            stateStorageServiceUrl,
            null,
            null,
            null,
            null,
                null, new TestSecretProviderConfigurator(), false));
        doNothing().when(factory).setupClient();
        return factory;
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime, boolean addSecrets) {
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
        return functionDetailsBuilder.build();
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
        factory = createKubernetesRuntimeFactory(null, percentMemoryPadding);
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

        factory = createKubernetesRuntimeFactory(null, 10);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    @Test
    public void testJavaConstructorWithSecrets() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, true);

        factory = createKubernetesRuntimeFactory(null, 10);

        verifyJavaInstance(config, pulsarRootDir + "/instances/deps", true);
    }

    @Test
    public void testJavaConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA, false);

        String extraDepsDir = "/path/to/deps/dir";

        factory = createKubernetesRuntimeFactory(extraDepsDir, 10);

        verifyJavaInstance(config, extraDepsDir, false);
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
            totalArgs = 35;
            portArg = 26;
            metricsPortArg = 28;
        } else {
            extraDepsEnv = "";
            portArg = 25;
            metricsPortArg = 27;
            totalArgs = 34;
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
        expectedArgs += " --cluster_name standalone";

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

        factory = createKubernetesRuntimeFactory(null, 10);

        verifyPythonInstance(config, pulsarRootDir + "/instances/deps", false);
    }

    @Test
    public void testPythonConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON, false);

        String extraDepsDir = "/path/to/deps/dir";

        factory = createKubernetesRuntimeFactory(extraDepsDir, 10);

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

}
