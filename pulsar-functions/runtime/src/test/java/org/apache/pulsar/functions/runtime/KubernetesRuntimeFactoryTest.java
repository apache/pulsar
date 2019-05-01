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

import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1PodSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Unit test of {@link ThreadRuntime}.
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

    @AfterMethod
    public void tearDown() {
        if (null != this.factory) {
            this.factory.close();
        }
    }

    KubernetesRuntimeFactory createKubernetesRuntimeFactory(String extraDepsDir, Resources minResources) throws Exception {
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
                0,
                pulsarServiceUrl,
            pulsarAdminUrl,
            stateStorageServiceUrl,
            null,
            null,
            null,
            null,
                minResources,
                new TestSecretProviderConfigurator(), false));
        doNothing().when(factory).setupClient();
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
        factory = createKubernetesRuntimeFactory(null, null);
        FunctionDetails functionDetails = createFunctionDetails();
        factory.doAdmissionChecks(functionDetails);
    }

    @Test
    public void testValidateMinResourcesRequired() throws Exception {
        factory = createKubernetesRuntimeFactory(null, null);

        FunctionDetails functionDetailsBase = createFunctionDetails();

        // min resources are not set
        try {
            factory.validateMinResourcesRequired(functionDetailsBase);
        } catch (Exception e) {
            fail();
        }

        testMinResource(0.2, 2048L, false, null);
        testMinResource(0.05, 2048L, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(0.2,512L, true, "Per instance RAM requested, 512, for function is less than the minimum required, 1024");
        testMinResource(0.05,512L, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(null, null, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
        testMinResource(0.2, null, true, "Per instance RAM requested, 0, for function is less than the minimum required, 1024");

        testMinResource(0.05, null, true, "Per instance CPU requested, 0.05, for function is less than the minimum required, 0.1");
        testMinResource(null, 2048L, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
        testMinResource(null, 512L, true, "Per instance CPU requested, 0.0, for function is less than the minimum required, 0.1");
    }

    private void testMinResource(Double cpu, Long ram, boolean fail, String failError) throws Exception {

        factory = createKubernetesRuntimeFactory(null, Resources.builder().cpu(0.1).ram(1024L).build());
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
            factory.validateMinResourcesRequired(functionDetails);
            if (fail) fail();
        } catch (IllegalArgumentException e) {
            if (!fail) fail();
            if (failError != null) {
                assertEquals(e.getMessage(), failError);
            }
        }
    }
}
