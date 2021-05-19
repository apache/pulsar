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
package org.apache.pulsar.functions.secretsproviderconfigurator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1SecretKeySelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * This file defines the SecretsProviderConfigurator that will be used by default for running in Kubernetes.
 * As such this implementation is strictly when workers are configured to use kubernetes runtime.
 * We use kubernetes in built secrets and bind them as environment variables within the function container
 * to ensure that the secrets are available to the function at runtime. Then we plug in the
 * EnvironmentBasedSecretsConfig as the secrets provider who knows how to read these environment variables.
 */
public class KubernetesSecretsProviderConfigurator implements SecretsProviderConfigurator {
    private static String ID_KEY = "path";
    private static String KEY_KEY = "key";
    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        switch (functionDetails.getRuntime()) {
            case JAVA:
                return EnvironmentBasedSecretsProvider.class.getName();
            case PYTHON:
                return "secretsprovider.EnvironmentBasedSecretsProvider";
            case GO:
                // [TODO] See GH issue #8425, we should finish this part once the issue is resolved.
                return "";
            default:
                throw new RuntimeException("Unknown function runtime " + functionDetails.getRuntime());
        }
    }

    @Override
    public Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails) {
        return null;
    }

    // Kubernetes secrets can be exposed as volume mounts or as environment variables in the pods. We are currently using the
    // environment variables way. Essentially the secretName/secretPath is attached as secretRef to the environment variables
    // of a pod and kubernetes magically makes the secret pointed to by this combination available as a env variable.
    @Override
    public void configureKubernetesRuntimeSecretsProvider(V1PodSpec podSpec, String functionsContainerName, Function.FunctionDetails functionDetails) {
        V1Container container = null;
        for (V1Container v1Container : podSpec.getContainers()) {
            if (v1Container.getName().equals(functionsContainerName)) {
                container = v1Container;
                break;
            }
        }
        if (container == null) {
            throw new RuntimeException("No FunctionContainer found");
        }
        if (!StringUtils.isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            for (Map.Entry<String, Object> entry : secretsMap.entrySet()) {
                final V1EnvVar secretEnv = new V1EnvVar();
                Map<String, String> kv = (Map<String, String>) entry.getValue();
                secretEnv.name(entry.getKey())
                        .valueFrom(new V1EnvVarSource()
                                .secretKeyRef(new V1SecretKeySelector()
                                        .name(kv.get(ID_KEY))
                                        .key(kv.get(KEY_KEY))));
                container.addEnvItem(secretEnv);
            }
        }
    }

    @Override
    public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, Function.FunctionDetails functionDetails) {
        throw new RuntimeException("KubernetesSecretsProviderConfigurator should only be setup for Kubernetes Runtime");
    }

    @Override
    public Type getSecretObjectType() {
        return new TypeToken<Map<String, String>>() {}.getType();
    }

    // The secret object should be of type Map<String, String> and it should contain "id" and "key"
    @Override
    public void doAdmissionChecks(AppsV1Api appsV1Api, CoreV1Api coreV1Api, String jobNamespace, String jobName, Function.FunctionDetails functionDetails) {
        if (!StringUtils.isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);

            for (Object object : secretsMap.values()) {
                if (object instanceof Map) {
                    Map<String, String> kubernetesSecret = (Map<String, String>) object;
                    if (kubernetesSecret.size() < 2) {
                        throw new IllegalArgumentException("Kubernetes Secret should contain id and key");
                    }
                    if (!kubernetesSecret.containsKey(ID_KEY)) {
                        throw new IllegalArgumentException("Kubernetes Secret should contain id information");
                    }
                    if (!kubernetesSecret.containsKey(KEY_KEY)) {
                        throw new IllegalArgumentException("Kubernetes Secret should contain key information");
                    }
                } else {
                    throw new IllegalArgumentException("Kubernetes Secret should be a Map containing id/key pairs");
                }
            }
        }
    }
}
