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

import io.kubernetes.client.models.V1Container;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;

import java.util.Map;

/**
 * Context provides contextual information to the executing function.
 * Features like which message id we are handling, whats the topic name of the
 * message, what are our operating constraints, etc can be accessed by the
 * executing function
 */
public class DefaultSecretsProviderConfigurator implements SecretsProviderConfigurator {
    @Override
    public void init(Map<String, String> config) {

    }

    @Override
    public String getSecretsProviderClassName(Function.FunctionDetails functionDetails) {
        switch (functionDetails.getRuntime()) {
            case JAVA:
                return ClearTextSecretsProvider.class.getName();
            case PYTHON:
                return "secretsprovider.ClearTextSecretsProvider";
            default:
                throw new RuntimeException("Unknwon runtime " + functionDetails.getRuntime());
        }
    }

    @Override
    public Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails) {
        return null;
    }

    @Override
    public void configureKubernetesRuntimeSecretsProvider(V1Container container, Function.FunctionDetails functionDetails) {
        // noop
    }

    @Override
    public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, Function.FunctionDetails functionDetails) {
        // noop
    }
}