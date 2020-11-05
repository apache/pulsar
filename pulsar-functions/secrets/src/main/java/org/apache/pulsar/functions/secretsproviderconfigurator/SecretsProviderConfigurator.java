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

import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PodSpec;
import org.apache.pulsar.functions.proto.Function;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * This file defines the SecretsProviderConfigurator interface. This interface is used by the function_workers
 * to choose the SecretProvider class name(if any) and its associated config at the time of starting
 * the function instances.
 */
public interface SecretsProviderConfigurator {
    /**
     * Initialize the SecretsProviderConfigurator.
     */
    default void init(Map<String, String> config) {}

    /**
     * Return the Secrets Provider Classname. This will be passed to the cmdline
     * of the instance and should contain the logic of connecting with the secrets
     * provider and obtaining secrets.
     */
    String getSecretsProviderClassName(Function.FunctionDetails functionDetails);

    /**
     * Return the secrets provider config.
     */
    Map<String, String> getSecretsProviderConfig(Function.FunctionDetails functionDetails);

    /**
     * Attaches any secrets specific stuff to the k8 container for kubernetes runtime.
     */
    void configureKubernetesRuntimeSecretsProvider(V1PodSpec podSpec, String functionsContainerName, Function.FunctionDetails functionDetails);

    /**
     * Attaches any secrets specific stuff to the ProcessBuilder for process runtime.
     */
    void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, Function.FunctionDetails functionDetails);

    /**
     * What is the type of the object that should be in the user secret config.
     *
     * @return
     */
    Type getSecretObjectType();

    /**
     * Do config checks to see whether the secrets provided are conforming.
     */
    default void doAdmissionChecks(AppsV1Api appsV1Api, CoreV1Api coreV1Api, String jobNamespace, String jobName, Function.FunctionDetails functionDetails) {}

}