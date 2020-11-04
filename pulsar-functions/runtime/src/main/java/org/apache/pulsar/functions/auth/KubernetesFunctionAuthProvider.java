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

import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.common.util.Reflections;

import java.util.Optional;

/**
 * Kubernetes runtime specific functions authentication provider
 */
public interface KubernetesFunctionAuthProvider extends FunctionAuthProvider {

    void initialize(CoreV1Api coreClient);

    default void initialize(CoreV1Api coreClient, byte[] caBytes, java.util.function.Function<Function.FunctionDetails, String> namespaceCustomizerFunc) {
        setCaBytes(caBytes);
        setNamespaceProviderFunc(namespaceCustomizerFunc);
        initialize(coreClient);
    }

    default void setCaBytes(byte[] caBytes) {

    }

    default void setNamespaceProviderFunc(java.util.function.Function<Function.FunctionDetails, String> funcDetails) {

    }

    /**
     * Configure function statefulset spec based on function auth data
     * @param statefulSet statefulset spec for function
     * @param functionAuthData function auth data
     */
    void configureAuthDataStatefulSet(V1StatefulSet statefulSet, Optional<FunctionAuthData> functionAuthData);

    static KubernetesFunctionAuthProvider getAuthProvider(String className) {
        return Reflections.createInstance(className, KubernetesFunctionAuthProvider.class, Thread.currentThread().getContextClassLoader());
    }
}
