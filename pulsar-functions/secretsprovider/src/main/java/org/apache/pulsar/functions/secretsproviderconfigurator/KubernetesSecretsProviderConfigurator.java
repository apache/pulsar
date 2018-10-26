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
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1SecretKeySelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Context provides contextual information to the executing function.
 * Features like which message id we are handling, whats the topic name of the
 * message, what are our operating constraints, etc can be accessed by the
 * executing function
 */
public class KubernetesSecretsProviderConfigurator implements SecretsProviderConfigurator {
    @Override
    public void init(Map<String, String> config) {

    }

    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        switch (functionDetails.getRuntime()) {
            case JAVA:
                return EnvironmentBasedSecretsProvider.class.getName();
            case PYTHON:
                return "secretsprovider.EnvironmentBasedSecretsProvider";
            default:
                throw new RuntimeException("Unknown function runtime " + functionDetails.getRuntime());
        }
    }

    @Override
    public Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails) {
        return null;
    }

    @Override
    public void configureKubernetesRuntimeSecretsProvider(V1Container container, Function.FunctionDetails functionDetails) {
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
                                        .name(kv.entrySet().iterator().next().getKey())
                                        .key(kv.entrySet().iterator().next().getValue())));
                container.addEnvItem(secretEnv);
            }
        }
    }

    @Override
    public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, Function.FunctionDetails functionDetails) {
        // noop
    }
}