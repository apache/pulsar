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
package org.apache.pulsar.functions.runtime.thread;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.functions.MemoryLimit.calculateClientMemoryLimit;
import java.util.Optional;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.MemoryLimit;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManagerImpl;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.FunctionsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
@NoArgsConstructor
public class ThreadRuntimeFactory implements RuntimeFactory {

    @Getter
    private ThreadGroup threadGroup;
    private FunctionCacheManager fnCache;
    private ClientBuilder clientBuilder;
    private PulsarClient pulsarClient;
    private PulsarAdmin pulsarAdmin;
    private String stateStorageImplClass;
    private String storageServiceUrl;
    private SecretsProvider defaultSecretsProvider;
    private FunctionCollectorRegistry collectorRegistry;
    private String narExtractionDirectory;
    private volatile boolean closed;
    private SecretsProviderConfigurator secretsProviderConfigurator;
    private ClassLoader rootClassLoader;
    private Optional<ConnectorsManager> connectorsManager;
    private Optional<FunctionsManager> functionsManager;

    /**
     * This constructor is used by other runtimes (e.g. ProcessRuntime and KubernetesRuntime)
     * that rely on ThreadRuntime to actually run an instance of the function.
     * When used by other runtimes, the arguments such as secretsProvider and rootClassLoader will be provided.
     */
    public ThreadRuntimeFactory(String threadGroupName, String pulsarServiceUrl,
                                String stateStorageImplClass,
                                String storageServiceUrl,
                                AuthenticationConfig authConfig, SecretsProvider secretsProvider,
                                FunctionCollectorRegistry collectorRegistry, String narExtractionDirectory,
                                ClassLoader rootClassLoader, boolean exposePulsarAdminClientEnabled,
                                String pulsarWebServiceUrl, MemoryLimit memoryLimit) throws Exception {
        initialize(threadGroupName, Optional.of(memoryLimit), pulsarServiceUrl, authConfig,
                stateStorageImplClass, storageServiceUrl, null, secretsProvider, collectorRegistry,
                narExtractionDirectory,
                rootClassLoader, exposePulsarAdminClientEnabled, pulsarWebServiceUrl, Optional.empty(),
                Optional.empty(), null);
    }

    public ThreadRuntimeFactory(String threadGroupName, String pulsarServiceUrl,
                                String stateStorageImplClass,
                                String storageServiceUrl,
                                AuthenticationConfig authConfig, SecretsProvider secretsProvider,
                                FunctionCollectorRegistry collectorRegistry, String narExtractionDirectory,
                                ClassLoader rootClassLoader, boolean exposePulsarAdminClientEnabled,
                                String pulsarWebServiceUrl, FunctionCacheManager fnCache, MemoryLimit memoryLimit) throws Exception {
        initialize(threadGroupName, Optional.of(memoryLimit), pulsarServiceUrl, authConfig,
                stateStorageImplClass, storageServiceUrl, null, secretsProvider, collectorRegistry,
                narExtractionDirectory,
                rootClassLoader, exposePulsarAdminClientEnabled, pulsarWebServiceUrl, Optional.empty(),
                Optional.empty(), fnCache);
    }

    private void initialize(String threadGroupName, Optional<MemoryLimit> memoryLimit,
                            String pulsarServiceUrl, AuthenticationConfig authConfig, String stateStorageImplClass,
                            String storageServiceUrl,
                            SecretsProviderConfigurator secretsProviderConfigurator, SecretsProvider secretsProvider,
                            FunctionCollectorRegistry collectorRegistry, String narExtractionDirectory,
                            ClassLoader rootClassLoader, boolean exposePulsarAdminClientEnabled,
                            String pulsarWebServiceUrl, Optional<ConnectorsManager> connectorsManager,
                            Optional<FunctionsManager> functionsManager, FunctionCacheManager fnCache)
            throws PulsarClientException {

        if (rootClassLoader == null) {
            rootClassLoader = Thread.currentThread().getContextClassLoader();
        }

        this.rootClassLoader = rootClassLoader;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.defaultSecretsProvider = secretsProvider;
        this.fnCache = fnCache;
        if (fnCache == null) {
            this.fnCache = new FunctionCacheManagerImpl(rootClassLoader);
        }
        this.threadGroup = new ThreadGroup(threadGroupName);
        this.pulsarAdmin =
                exposePulsarAdminClientEnabled ? InstanceUtils.createPulsarAdminClient(pulsarWebServiceUrl, authConfig)
                        : null;
        this.clientBuilder = InstanceUtils
                .createPulsarClientBuilder(pulsarServiceUrl, authConfig, calculateClientMemoryLimit(memoryLimit));
        this.pulsarClient = this.clientBuilder.build();
        this.stateStorageImplClass = stateStorageImplClass;
        this.storageServiceUrl = storageServiceUrl;
        this.collectorRegistry = collectorRegistry;
        this.narExtractionDirectory = narExtractionDirectory;
        this.connectorsManager = connectorsManager;
        this.functionsManager = functionsManager;
    }



    @Override
    public void initialize(WorkerConfig workerConfig, AuthenticationConfig authenticationConfig,
                           SecretsProviderConfigurator secretsProviderConfigurator,
                           ConnectorsManager connectorsManager,
                           FunctionsManager functionsManager,
                           Optional<FunctionAuthProvider> functionAuthProvider,
                           Optional<RuntimeCustomizer> runtimeCustomizer) throws Exception {
        ThreadRuntimeFactoryConfig factoryConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionRuntimeFactoryConfigs(), ThreadRuntimeFactoryConfig.class);

        initialize(factoryConfig.getThreadGroupName(), Optional.ofNullable(factoryConfig.getPulsarClientMemoryLimit()),
                workerConfig.getPulsarServiceUrl(), authenticationConfig,
                workerConfig.getStateStorageProviderImplementation(),
                workerConfig.getStateStorageServiceUrl(), secretsProviderConfigurator, null,
                null, workerConfig.getNarExtractionDirectory(), null,
                workerConfig.isExposeAdminClientEnabled(),
                workerConfig.getPulsarWebServiceUrl(), Optional.of(connectorsManager), Optional.of(functionsManager),
                null);
    }

    @Override
    public ThreadRuntime createContainer(InstanceConfig instanceConfig, String jarFile,
                                         String originalCodeFileName,
                                         String transformFunctionFile,
                                         String originalTransformFunctionFileName,
                                         Long expectedHealthCheckInterval) {
        SecretsProvider secretsProvider = defaultSecretsProvider;
        if (secretsProvider == null) {
            String secretsProviderClassName =
                    secretsProviderConfigurator.getSecretsProviderClassName(instanceConfig.getFunctionDetails());
            secretsProvider =
                    (SecretsProvider) Reflections.createInstance(secretsProviderClassName, this.rootClassLoader);
            log.info("Initializing secrets provider {} with configs: {}",
                    secretsProvider.getClass().getName(),
                    secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
            secretsProvider
                    .init(secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
        }

        return new ThreadRuntime(
            instanceConfig,
            fnCache,
            threadGroup,
            jarFile,
            transformFunctionFile,
            pulsarClient,
            clientBuilder,
            pulsarAdmin,
            stateStorageImplClass,
            storageServiceUrl,
            secretsProvider,
            collectorRegistry,
            narExtractionDirectory,
            connectorsManager,
            functionsManager);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        threadGroup.interrupt();
        fnCache.close();
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close pulsar client when closing function container factory", e);
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }

        // Shutdown instance cache
        InstanceCache.shutdown();
    }
}
