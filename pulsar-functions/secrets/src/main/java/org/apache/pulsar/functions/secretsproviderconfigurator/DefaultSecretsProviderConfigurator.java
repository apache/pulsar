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
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * This is a barebones version of a secrets provider which wires in ClearTextSecretsProvider
 * to the function instances/containers.
 * While this is the default configurator, it is highly recommended that for real-security
 * you use some alternate provider.
 */
public class DefaultSecretsProviderConfigurator implements SecretsProviderConfigurator {
    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        switch (functionDetails.getRuntime()) {
            case JAVA:
                return ClearTextSecretsProvider.class.getName();
            case PYTHON:
                return "secretsprovider.ClearTextSecretsProvider";
            case GO:
                return "";
            default:
                throw new RuntimeException("Unknown runtime " + functionDetails.getRuntime());
        }
    }

    @Override
    public Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails) {
        return null;
    }

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
        return new TypeToken<String>() {}.getType();
    }
}