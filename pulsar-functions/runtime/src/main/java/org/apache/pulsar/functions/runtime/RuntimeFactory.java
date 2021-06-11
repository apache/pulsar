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

package org.apache.pulsar.functions.runtime;

import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;

import java.util.Optional;

/**
 * A factory to create {@link Runtime}s to invoke functions.
 */
public interface RuntimeFactory extends AutoCloseable {

    void initialize(WorkerConfig workerConfig,
                    AuthenticationConfig authenticationConfig,
                    SecretsProviderConfigurator secretsProviderConfigurator,
                    ConnectorsManager connectorsManager,
                    Optional<FunctionAuthProvider> authProvider,
                    Optional<RuntimeCustomizer> runtimeCustomizer) throws Exception;

    /**
     * Create a function container to execute a java instance.
     *
     * @param instanceConfig java instance config
     * @param codeFile code file
     * @param expectedHealthCheckInterval expected health check interval in seconds
     * @return function container to start/stop instance
     */
    Runtime createContainer(
            InstanceConfig instanceConfig, String codeFile, String originalCodeFileName,
            Long expectedHealthCheckInterval) throws Exception;

    default boolean externallyManaged() { return false; }

    default void doAdmissionChecks(Function.FunctionDetails functionDetails) { }

    default Optional<? extends FunctionAuthProvider> getAuthProvider() {
        return Optional.empty();
    }

    default Optional<? extends RuntimeCustomizer> getRuntimeCustomizer() {
        return Optional.empty();
    }

    @Override
    void close();

    static RuntimeFactory getFuntionRuntimeFactory(String className) {
        return Reflections.createInstance(className, RuntimeFactory.class, Thread.currentThread().getContextClassLoader());
    }

}

