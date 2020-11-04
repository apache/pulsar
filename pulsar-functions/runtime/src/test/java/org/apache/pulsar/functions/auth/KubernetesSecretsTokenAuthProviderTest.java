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
package org.apache.pulsar.functions.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;

import java.util.Collections;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KubernetesSecretsTokenAuthProviderTest {

    @Test
    public void testConfigureAuthDataStatefulSet() {
        byte[] testBytes = new byte[]{0, 1, 2, 3, 4};

        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api, testBytes, (fd) -> "default");


        V1StatefulSet statefulSet = new V1StatefulSet();
        statefulSet.setSpec(
                new V1StatefulSetSpec().template(
                        new V1PodTemplateSpec().spec(
                                new V1PodSpec().containers(
                                        Collections.singletonList(new V1Container())))));
        FunctionAuthData functionAuthData = FunctionAuthData.builder().data("foo".getBytes()).build();
        kubernetesSecretsTokenAuthProvider.configureAuthDataStatefulSet(statefulSet, Optional.of(functionAuthData));

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), "function-auth");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().get(0).getSecret().getSecretName(), "pf-secret-foo");

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), "function-auth");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), "/etc/auth");
    }

    @Test
    public void testConfigureAuthDataStatefulSetNoCa() {
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api, null, (fd) -> "default");


        V1StatefulSet statefulSet = new V1StatefulSet();
        statefulSet.setSpec(
                new V1StatefulSetSpec().template(
                        new V1PodTemplateSpec().spec(
                                new V1PodSpec().containers(
                                        Collections.singletonList(new V1Container())))));
        FunctionAuthData functionAuthData = FunctionAuthData.builder().data("foo".getBytes()).build();
        kubernetesSecretsTokenAuthProvider.configureAuthDataStatefulSet(statefulSet, Optional.of(functionAuthData));

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), "function-auth");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getVolumes().get(0).getSecret().getSecretName(), "pf-secret-foo");

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), "function-auth");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), "/etc/auth");
    }

    @Test
    public void testCacheAuthData() throws ApiException {
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        doReturn(new V1Secret()).when(coreV1Api).createNamespacedSecret(anyString(), any(), anyString(), anyString(), anyString());
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api,  null, (fd) -> "default");
        Function.FunctionDetails funcDetails = Function.FunctionDetails.newBuilder().setTenant("test-tenant").setNamespace("test-ns").setName("test-func").build();
        Optional<FunctionAuthData> functionAuthData = kubernetesSecretsTokenAuthProvider.cacheAuthData(funcDetails, new AuthenticationDataSource() {
                    @Override
                    public boolean hasDataFromCommand() {
                        return true;
                    }

                    @Override
                    public String getCommandData() {
                        return "test-token";
                    }
                });

        Assert.assertTrue(functionAuthData.isPresent());
        Assert.assertTrue(StringUtils.isNotBlank(new String(functionAuthData.get().getData())));
    }

    @Test
    public void configureAuthenticationConfig() {
        byte[] testBytes = new byte[]{0, 1, 2, 3, 4};
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api, testBytes, (fd) -> "default");
        AuthenticationConfig authenticationConfig = AuthenticationConfig.builder().build();
        FunctionAuthData functionAuthData = FunctionAuthData.builder().data("foo".getBytes()).build();
        kubernetesSecretsTokenAuthProvider.configureAuthenticationConfig(authenticationConfig, Optional.of(functionAuthData));

        Assert.assertEquals(authenticationConfig.getClientAuthenticationPlugin(), AuthenticationToken.class.getName());
        Assert.assertEquals(authenticationConfig.getClientAuthenticationParameters(), "file:///etc/auth/token");
        Assert.assertEquals(authenticationConfig.getTlsTrustCertsFilePath(), "/etc/auth/ca.pem");
    }

    @Test
    public void configureAuthenticationConfigNoCa() {
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api, null, (fd) -> "default");
        AuthenticationConfig authenticationConfig = AuthenticationConfig.builder().build();
        FunctionAuthData functionAuthData = FunctionAuthData.builder().data("foo".getBytes()).build();
        kubernetesSecretsTokenAuthProvider.configureAuthenticationConfig(authenticationConfig, Optional.of(functionAuthData));

        Assert.assertEquals(authenticationConfig.getClientAuthenticationPlugin(), AuthenticationToken.class.getName());
        Assert.assertEquals(authenticationConfig.getClientAuthenticationParameters(), "file:///etc/auth/token");
        Assert.assertEquals(authenticationConfig.getTlsTrustCertsFilePath(), null);
    }


    @Test
    public void testUpdateAuthData() throws Exception {
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        KubernetesSecretsTokenAuthProvider kubernetesSecretsTokenAuthProvider = new KubernetesSecretsTokenAuthProvider();
        kubernetesSecretsTokenAuthProvider.initialize(coreV1Api, null, (fd) -> "default");
        // test when existingFunctionAuthData is empty
        Optional<FunctionAuthData> existingFunctionAuthData = Optional.empty();
        Function.FunctionDetails funcDetails = Function.FunctionDetails.newBuilder().setTenant("test-tenant").setNamespace("test-ns").setName("test-func").build();
        Optional<FunctionAuthData> functionAuthData = kubernetesSecretsTokenAuthProvider.updateAuthData(funcDetails, existingFunctionAuthData, new AuthenticationDataSource() {
                    @Override
                    public boolean hasDataFromCommand() {
                        return true;
                    }

                    @Override
                    public String getCommandData() {
                        return "test-token";
                    }
                });


        Assert.assertTrue(functionAuthData.isPresent());
        Assert.assertTrue(StringUtils.isNotBlank(new String(functionAuthData.get().getData())));

        // test when existingFunctionAuthData is NOT empty
        existingFunctionAuthData = Optional.of(new FunctionAuthData("pf-secret-z7mxx".getBytes(), null));
        functionAuthData = kubernetesSecretsTokenAuthProvider.updateAuthData(funcDetails, existingFunctionAuthData, new AuthenticationDataSource() {
                    @Override
                    public boolean hasDataFromCommand() {
                        return true;
                    }

                    @Override
                    public String getCommandData() {
                        return "test-token";
                    }
                });


        Assert.assertTrue(functionAuthData.isPresent());
        Assert.assertEquals(new String(functionAuthData.get().getData()), "pf-secret-z7mxx");
    }
}
