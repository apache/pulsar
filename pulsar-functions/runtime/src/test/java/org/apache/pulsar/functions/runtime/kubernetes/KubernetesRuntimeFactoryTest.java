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

import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider;
import org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Unit test of {@link KubernetesRuntimeFactoryTest}.
 */
public class KubernetesRuntimeFactoryTest {

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

    public KubernetesRuntimeFactoryTest() throws Exception {
        this.userJarFile = "UserJar.jar";
        this.pulsarRootDir = "/pulsar";
        this.javaInstanceJarFile = "/pulsar/instances/java-instance.jar";
        this.pythonInstanceFile = "/pulsar/instances/python-instance/python_instance_main.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.pulsarAdminUrl = "http://localhost:8080";
        this.stateStorageServiceUrl = "bk://localhost:4181";
        this.logDirectory = "logs/functions";
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (null != this.factory) {
            this.factory.close();
        }
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, Resources minResources,
                                                            Resources maxResources,
                                                            Resources resourceGranularities,
                                                            boolean resourceChangeInLockStep) throws Exception {
        return createKubernetesRuntimeFactory(extraDepsDir, minResources, maxResources, resourceGranularities,
                resourceChangeInLockStep, Optional.empty(), Optional.empty());
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir,
                                                            Resources minResources,
                                                            Resources maxResources,
                                                            Resources resourceGranularities,
                                                            boolean resourceChangeInLockStep,
                                                            Optional<FunctionAuthProvider> functionAuthProvider,
                                                            Optional<RuntimeCustomizer> manifestCustomizer) throws Exception {
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
        kubernetesRuntimeFactoryConfig.setPercentMemoryPadding(0);
        kubernetesRuntimeFactoryConfig.setCpuOverCommitRatio(1.0);
        kubernetesRuntimeFactoryConfig.setMemoryOverCommitRatio(1.0);
        kubernetesRuntimeFactoryConfig.setPulsarServiceUrl(pulsarServiceUrl);
        kubernetesRuntimeFactoryConfig.setPulsarAdminUrl(pulsarAdminUrl);
        kubernetesRuntimeFactoryConfig.setChangeConfigMapNamespace(null);
        kubernetesRuntimeFactoryConfig.setChangeConfigMap(null);
        kubernetesRuntimeFactoryConfig.setGrpcPort(4345);
        kubernetesRuntimeFactoryConfig.setMetricsPort(4344);

        workerConfig.setFunctionRuntimeFactoryClassName(KubernetesRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(kubernetesRuntimeFactoryConfig, Map.class));

        workerConfig.setFunctionInstanceMinResources(minResources);
        workerConfig.setFunctionInstanceMaxResources(maxResources);
        workerConfig.setFunctionInstanceResourceGranularities(resourceGranularities);
        workerConfig.setFunctionInstanceResourceChangeInLockStep(resourceChangeInLockStep);
        workerConfig.setStateStorageServiceUrl(null);
        workerConfig.setAuthenticationEnabled(false);

        factory.initialize(workerConfig,null, new TestSecretProviderConfigurator(), Mockito.mock(ConnectorsManager.class), functionAuthProvider, manifestCustomizer);
        return factory;
    }

    FunctionDetails createFunctionDetails() {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("function");
        functionDetailsBuilder.setSecretsMap("SomeMap");
        return functionDetailsBuilder.build();
    }

    @Test
    public void testAdmissionChecks() throws Exception {
        factory = createKubernetesRuntimeFactory(null, null, null, null, false);
        FunctionDetails functionDetails = createFunctionDetails();
        factory.doAdmissionChecks(functionDetails);
    }

    @Test
    public void testValidateMinResourcesRequired() throws Exception {
        factory = createKubernetesRuntimeFactory(null, null, null, null, false);

        FunctionDetails functionDetailsBase = createFunctionDetails();

        // min resources are not set
        try {
            factory.validateMinResourcesRequired(functionDetailsBase);
        } catch (Exception e) {
            fail();
        }

        testMinResource(0.2, 2048L, false, null);
        testMinResource(0.05, 2048L, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(0.2, 512L, true, "Per instance RAM requested, 512, for function is less than the minimum required, 1024");
        testMinResource(0.05, 512L, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(null, null, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
        testMinResource(0.2, null, true, "Per instance RAM requested, 0, for function is less than the minimum required, 1024");

        testMinResource(0.05, null, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(null, 2048L, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
        testMinResource(null, 512L, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
    }

    @Test
    public void testValidateMaxResourcesRequired() throws Exception {
        factory = createKubernetesRuntimeFactory(null, null, null, null, false);

        FunctionDetails functionDetailsBase = createFunctionDetails();

        // max resources are not set
        try {
            factory.validateMaxResourcesRequired(functionDetailsBase);
        } catch (Exception e) {
            fail();
        }

        testMaxResource(0.2, 2048L, false, null);
        testMaxResource(1.00, 2048L, false, null);
        testMaxResource(1.01, 512L, true, "Per instance CPU requested, 1.01, for function is greater than the maximum required, 1.0");
        testMaxResource(1.00, 2049L, true, "Per instance RAM requested, 2049, for function is greater than the maximum required, 2048");

        testMaxResource(null, null, false, null);
        testMaxResource(0.2, null, false, null);
        testMaxResource(null, 2048L, false, null);
        testMaxResource(1.05, null, true, "Per instance CPU requested, 1.05, for function is greater than the maximum required, 1.0");
        testMaxResource(null, 3072L, true, "Per instance RAM requested, 3072, for function is greater than the maximum required, 2048");
    }

    @Test
    public void testValidateMinMaxResourcesRequired() throws Exception {
        testMinMaxResource(0.1, 1024L, false, null);
        testMinMaxResource(0.2, 1536L, false, null);
        testMinMaxResource(1.00, 2048L, false, null);

        testMinMaxResource(1.01, 1024L, true, "Per instance CPU requested, 1.01, for function is greater than the maximum required, 1.0");
        testMinMaxResource(1.00, 2049L, true, "Per instance RAM requested, 2049, for function is greater than the maximum required, 2048");
        testMinMaxResource(0.05, 2048L, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinMaxResource(0.2, 512L, true, "Per instance RAM requested, 512, for function is less than the minimum required, 1024");

        testMinMaxResource(null, null, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
        testMinMaxResource(0.2, null, true, "Per instance RAM requested, 0, for function is less than the minimum required, 1024");
    }

    @Test
    public void testValidateResourcesGranularityAndProportion() throws Exception {
        factory = createKubernetesRuntimeFactory(null, null, null, null, false);

        Resources granularities = Resources.builder()
                .cpu(0.1)
                .ram(1000L)
                .build();

        // when resources granularities are not set
        testResourceGranularities(null, null, null, false, false, null);
        testResourceGranularities(0.05, 100L, null, false, false, null);

        // only accept positive resource values when granularities are set
        testResourceGranularities(null, null, granularities, false, true,
                "Per instance cpu requested, 0.0, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(0.1, null, granularities, false, true,
                "Per instance ram requested, 0, for function should be positive and a multiple of the granularity, 1000");
        testResourceGranularities(0.1, 0L, granularities, false, true,
                "Per instance ram requested, 0, for function should be positive and a multiple of the granularity, 1000");
        testResourceGranularities(null, 1000L, granularities, false, true,
                "Per instance cpu requested, 0.0, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(0.0, 1000L, granularities, false, true,
                "Per instance cpu requested, 0.0, for function should be positive and a multiple of the granularity, 0.1");

        // requested resources must be multiples of granularities
        testResourceGranularities(0.05, 100L, granularities, false, true,
                "Per instance cpu requested, 0.05, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(0.1, 100L, granularities, false, true,
                "Per instance ram requested, 100, for function should be positive and a multiple of the granularity, 1000");
        testResourceGranularities(1.01, 100L, granularities, false, true,
                "Per instance cpu requested, 1.01, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(0.999, 100L, granularities, false, true,
                "Per instance cpu requested, 0.999, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(1.001, 100L, granularities, false, true,
                "Per instance cpu requested, 1.001, for function should be positive and a multiple of the granularity, 0.1");
        testResourceGranularities(0.1, 1000L, granularities, false, false, null);
        testResourceGranularities(1.0, 1000L, granularities, false, false, null);
        testResourceGranularities(5.0, 1000L, granularities, false, false, null);

        // resource values of different dimensions should respect lock step configs
        testResourceGranularities(0.2, 1000L, granularities, false, false, null);
        testResourceGranularities(0.1, 2000L, granularities, false, false, null);
        testResourceGranularities(0.1, 2000L, granularities, true, true,
                "Per instance cpu requested, 0.1, ram requested, 2000, for function should be positive and the same multiple of the granularity, cpu, 0.1, ram, 1000");
        testResourceGranularities(0.2, 1000L, granularities, true, true,
                "Per instance cpu requested, 0.2, ram requested, 1000, for function should be positive and the same multiple of the granularity, cpu, 0.1, ram, 1000");
        testResourceGranularities(0.1, 1000L, granularities, true, false, null);
        testResourceGranularities(0.2, 2000L, granularities, true, false, null);
        testResourceGranularities(1.0, 10000L, granularities, true, false, null);
        testResourceGranularities(10.0, 100000L, granularities, true, false, null);
        testResourceGranularities(10.0, null, granularities, true, true,
                "Per instance ram requested, 0, for function should be positive and a multiple of the granularity, 1000");
    }

    private void testAuthProvider(Optional<FunctionAuthProvider> authProvider) throws Exception {
        factory = createKubernetesRuntimeFactory(null, null, null, null, false,
                authProvider, Optional.empty());
    }


    @Test
    public void testAuthProviderNotSet() throws Exception {
        testAuthProvider(Optional.empty());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Function authentication provider.*.must implement KubernetesFunctionAuthProvider")
    public void testAuthProviderWrongInterface() throws Exception {
        testAuthProvider(Optional.of(new FunctionAuthProvider() {
            @Override
            public void configureAuthenticationConfig(AuthenticationConfig authConfig,
                                                      Optional<FunctionAuthData> functionAuthData) {

            }

            @Override
            public Optional<FunctionAuthData> cacheAuthData(Function.FunctionDetails funcDetails,
                                                            AuthenticationDataSource authenticationDataSource) throws Exception {
                return Optional.empty();
            }

            @Override
            public Optional<FunctionAuthData> updateAuthData(Function.FunctionDetails funcDetails,
                                                             Optional<FunctionAuthData> existingFunctionAuthData,
                                                             AuthenticationDataSource authenticationDataSource) throws Exception {
                return Optional.empty();
            }

            @Override
            public void cleanUpAuthData(Function.FunctionDetails funcDetails, Optional<FunctionAuthData> functionAuthData) throws Exception {

            }
        }));
    }

    @Test
    public void testAuthProviderCorrectInterface() throws Exception {
        testAuthProvider(Optional.of(new KubernetesFunctionAuthProvider() {

            @Override
            public void initialize(CoreV1Api coreClient) {

            }

            @Override
            public void configureAuthenticationConfig(AuthenticationConfig authConfig,
                                                      Optional<FunctionAuthData> functionAuthData) {

            }

            @Override
            public Optional<FunctionAuthData> cacheAuthData(Function.FunctionDetails funcDetails,
                                                            AuthenticationDataSource authenticationDataSource) throws Exception {
                return Optional.empty();
            }

            @Override
            public Optional<FunctionAuthData> updateAuthData(Function.FunctionDetails funcDetails,
                                                             Optional<FunctionAuthData> existingFunctionAuthData,
                                                             AuthenticationDataSource authenticationDataSource) throws Exception {
                return Optional.empty();
            }

            @Override
            public void cleanUpAuthData(Function.FunctionDetails funcDetails, Optional<FunctionAuthData> functionAuthData) throws Exception {

            }

            @Override
            public void configureAuthDataStatefulSet(V1StatefulSet statefulSet, Optional<FunctionAuthData> functionAuthData) {

            }

        }));

        testAuthProvider(Optional.of(new KubernetesSecretsTokenAuthProvider()));
    }

    private void testMinResource(Double cpu, Long ram, boolean fail, String failError) throws Exception {
        testResourceRestrictions(cpu, ram, Resources.builder().cpu(0.1).ram(1024L).build(), null, null, false, fail, failError);
    }

    private void testMaxResource(Double cpu, Long ram, boolean fail, String failError) throws Exception {
        testResourceRestrictions(cpu, ram, null, Resources.builder().cpu(1.0).ram(2048L).build(), null, false, fail, failError);
    }

    private void testMinMaxResource(Double cpu, Long ram, boolean fail, String failError) throws Exception {
        testResourceRestrictions(cpu, ram, Resources.builder().cpu(0.1).ram(1024L).build(),
                Resources.builder().cpu(1.0).ram(2048L).build(), null, false, fail, failError);
    }

    private void testResourceGranularities(Double cpu, Long ram, Resources granularities, boolean changeInLockStep,
                                           boolean fail, String failError) throws Exception {
        testResourceRestrictions(cpu, ram, null, null, granularities, changeInLockStep,
                fail, failError);
    }

    private void testResourceRestrictions(Double cpu, Long ram, Resources minResources, Resources maxResources,
                                          Resources granularities, boolean changeInLockStep,
                                          boolean fail, String failError) throws Exception {

        factory = createKubernetesRuntimeFactory(null, minResources, maxResources, granularities, changeInLockStep);
        FunctionDetails functionDetailsBase = createFunctionDetails();

        Function.Resources.Builder resources = Function.Resources.newBuilder();
        if (cpu != null) {
            resources.setCpu(cpu);
        }
        if (ram != null) {
            resources.setRam(ram);
        }
        FunctionDetails functionDetails;
        if (ram != null || cpu != null) {
            functionDetails = FunctionDetails.newBuilder(functionDetailsBase).setResources(resources).build();
        } else {
            functionDetails = FunctionDetails.newBuilder(functionDetailsBase).build();
        }

        try {
            factory.doAdmissionChecks(functionDetails);
            if (fail) fail();
        } catch (IllegalArgumentException e) {
            if (!fail) fail();
            if (failError != null) {
                assertEquals(e.getMessage(), failError);
            }
        }
    }

    @Test
    public void testDynamicConfigMapLoading() throws Exception {

        String changeConfigMap = "changeMap";
        String changeConfigNamespace = "changeConfigNamespace";

        KubernetesRuntimeFactory kubernetesRuntimeFactory = getKuberentesRuntimeFactory();
        CoreV1Api coreV1Api = Mockito.mock(CoreV1Api.class);
        V1ConfigMap v1ConfigMap = new V1ConfigMap();
        Mockito.doReturn(v1ConfigMap).when(coreV1Api).readNamespacedConfigMap(any(), any(), any());
        KubernetesRuntimeFactory.fetchConfigMap(coreV1Api, changeConfigMap, changeConfigNamespace, kubernetesRuntimeFactory);
        Mockito.verify(coreV1Api, Mockito.times(1)).readNamespacedConfigMap(eq(changeConfigMap), eq(changeConfigNamespace), eq(null));
        KubernetesRuntimeFactory expected = getKuberentesRuntimeFactory();
        assertEquals(kubernetesRuntimeFactory, expected);

        HashMap<String, String> configs = new HashMap<>();
        configs.put("pulsarDockerImageName", "test_dockerImage2");
        configs.put("imagePullPolicy", "test_imagePullPolicy2");
        v1ConfigMap.setData(configs);
        KubernetesRuntimeFactory.fetchConfigMap(coreV1Api, changeConfigMap, changeConfigNamespace, kubernetesRuntimeFactory);
        Mockito.verify(coreV1Api, Mockito.times(2)).readNamespacedConfigMap(eq(changeConfigMap), eq(changeConfigNamespace), eq(null));

       assertEquals(kubernetesRuntimeFactory.getPulsarDockerImageName(), "test_dockerImage2");
       assertEquals(kubernetesRuntimeFactory.getImagePullPolicy(), "test_imagePullPolicy2");
    }

    private KubernetesRuntimeFactory getKuberentesRuntimeFactory() {
        KubernetesRuntimeFactory kubernetesRuntimeFactory = new KubernetesRuntimeFactory();
        WorkerConfig workerConfig = new WorkerConfig();
        Map<String, String> imageNames = new HashMap<>();
        imageNames.put("JAVA", "test-java-function-docker-image");
        imageNames.put("PYTHON", "test-python-function-docker-image");
        imageNames.put("GO", "test-go-function-docker-image");
        KubernetesRuntimeFactoryConfig kubernetesRuntimeFactoryConfig = new KubernetesRuntimeFactoryConfig();
        kubernetesRuntimeFactoryConfig.setK8Uri("test_k8uri");
        kubernetesRuntimeFactoryConfig.setJobNamespace("test_jobNamespace");
        kubernetesRuntimeFactoryConfig.setJobName("test_jobName");
        kubernetesRuntimeFactoryConfig.setPulsarDockerImageName("test_dockerImage");
        kubernetesRuntimeFactoryConfig.setFunctionDockerImages(imageNames);
        kubernetesRuntimeFactoryConfig.setImagePullPolicy("test_imagePullPolicy");
        workerConfig.setFunctionRuntimeFactoryClassName(KubernetesRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(kubernetesRuntimeFactoryConfig, Map.class));
        AuthenticationConfig authenticationConfig = AuthenticationConfig.builder().build();
        kubernetesRuntimeFactory.initialize(workerConfig, authenticationConfig, new DefaultSecretsProviderConfigurator(), Mockito.mock(ConnectorsManager.class), Optional.empty(), Optional.empty());
        return kubernetesRuntimeFactory;
    }
}
