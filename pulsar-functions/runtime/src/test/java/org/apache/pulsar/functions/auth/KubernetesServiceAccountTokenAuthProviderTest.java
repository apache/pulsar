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
package org.apache.pulsar.functions.auth;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ServiceAccountTokenProjection;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KubernetesServiceAccountTokenAuthProviderTest {
    @Test
    public void testConfigureAuthDataStatefulSet() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("brokerClientTrustCertsSecretName", "my-secret");
        config.put("serviceAccountTokenExpirationSeconds", "600");
        config.put("serviceAccountTokenAudience", "my-audience");
        KubernetesServiceAccountTokenAuthProvider provider = new KubernetesServiceAccountTokenAuthProvider();
        provider.initialize(null, null, (fd) -> "default", config);

        // Create a stateful set with a container
        V1StatefulSet statefulSet = new V1StatefulSet();
        statefulSet.setSpec(
                new V1StatefulSetSpec().template(
                        new V1PodTemplateSpec().spec(
                                new V1PodSpec().containers(
                                        Collections.singletonList(new V1Container())))));
        provider.configureAuthDataStatefulSet(statefulSet, Optional.empty());

        List<V1Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
        Assert.assertEquals(volumes.size(), 2);

        Assert.assertEquals(volumes.get(0).getName(), "ca-cert");
        Assert.assertEquals(volumes.get(0).getSecret().getSecretName(), "my-secret");
        Assert.assertEquals(volumes.get(0).getSecret().getItems().size(), 1);
        Assert.assertEquals(volumes.get(0).getSecret().getItems().get(0).getKey(), "ca.crt");
        Assert.assertEquals(volumes.get(0).getSecret().getItems().get(0).getPath(), "ca.crt");


        Assert.assertEquals(volumes.get(1).getName(), "service-account-token");
        Assert.assertEquals(volumes.get(1).getProjected().getSources().size(), 1);
        V1ServiceAccountTokenProjection tokenProjection =
                volumes.get(1).getProjected().getSources().get(0).getServiceAccountToken();
        Assert.assertEquals(tokenProjection.getExpirationSeconds(), 600);
        Assert.assertEquals(tokenProjection.getAudience(), "my-audience");

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().size(), 2);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), "service-account-token");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), "/etc/auth");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), "ca-cert");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), "/etc/auth");
    }

    @Test
    public void testConfigureAuthDataStatefulSetNoCa() {

        HashMap<String, Object> config = new HashMap<>();
        config.put("serviceAccountTokenExpirationSeconds", "600");
        config.put("serviceAccountTokenAudience", "pulsar-cluster");
        KubernetesServiceAccountTokenAuthProvider provider = new KubernetesServiceAccountTokenAuthProvider();
        provider.initialize(null, null, (fd) -> "default", config);

        // Create a stateful set with a container
        V1StatefulSet statefulSet = new V1StatefulSet();
        statefulSet.setSpec(
                new V1StatefulSetSpec().template(
                        new V1PodTemplateSpec().spec(
                                new V1PodSpec().containers(
                                        Collections.singletonList(new V1Container())))));
        provider.configureAuthDataStatefulSet(statefulSet, Optional.empty());

        List<V1Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
        Assert.assertEquals(volumes.size(), 1);

        Assert.assertEquals(volumes.get(0).getName(), "service-account-token");
        Assert.assertEquals(volumes.get(0).getProjected().getSources().size(), 1);
        V1ServiceAccountTokenProjection tokenProjection =
                volumes.get(0).getProjected().getSources().get(0).getServiceAccountToken();
        Assert.assertEquals(tokenProjection.getExpirationSeconds(), 600);
        Assert.assertEquals(tokenProjection.getAudience(), "pulsar-cluster");

        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().size(), 1);
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), "service-account-token");
        Assert.assertEquals(statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), "/etc/auth");
    }

    @Test
    public void configureAuthenticationConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("brokerClientTrustCertsSecretName", "my-secret");
        KubernetesServiceAccountTokenAuthProvider provider = new KubernetesServiceAccountTokenAuthProvider();
        provider.initialize(null, null, (fd) -> "default", config);
        AuthenticationConfig authenticationConfig = AuthenticationConfig.builder().build();
        provider.configureAuthenticationConfig(authenticationConfig, Optional.empty());

        Assert.assertEquals(authenticationConfig.getClientAuthenticationPlugin(), AuthenticationToken.class.getName());
        Assert.assertEquals(authenticationConfig.getClientAuthenticationParameters(), "file:///etc/auth/token");
        Assert.assertEquals(authenticationConfig.getTlsTrustCertsFilePath(), "/etc/auth/ca.crt");
    }
}
