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

import java.lang.reflect.Type;
import java.util.Map;

/**
 * This is a very simple secrets provider which wires in a given secrets provider classname/config
 * to the function instances/containers. This does not do any special kubernetes specific wiring.
 */
public class NameAndConfigBasedSecretsProviderConfigurator implements SecretsProviderConfigurator {
    private String className;
    private Map<String, String> config;
    public NameAndConfigBasedSecretsProviderConfigurator(String className, Map<String, String> config) {
        this.className = className;
        this.config = config;
    }
    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        return className;
    }

    @Override
    public Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails) {
        return config;
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
