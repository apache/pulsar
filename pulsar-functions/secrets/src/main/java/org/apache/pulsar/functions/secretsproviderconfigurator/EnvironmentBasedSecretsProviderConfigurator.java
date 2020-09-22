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

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.models.V1PodSpec;
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
public class EnvironmentBasedSecretsProviderConfigurator implements SecretsProviderConfigurator {
    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        switch (functionDetails.getRuntime()) {
            case JAVA:
                return EnvironmentBasedSecretsProvider.class.getName();
            case PYTHON:
                return "secretsprovider.EnvironmentBasedSecretsProvider";
            case GO:
                throw new UnsupportedOperationException();
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
        // noop
    }

    @Override
    public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, Function.FunctionDetails functionDetails) {
        // noop
    }

    @Override
    public Type getSecretObjectType() {
        return new TypeToken<Map<String, String>>() {}.getType();
    }
}
