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

package org.apache.pulsar.functions.runtime.thread;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.CollectorRegistry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManagerImpl;
import org.apache.pulsar.functions.worker.WorkerConfig;

import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
@NoArgsConstructor
public class ThreadRuntimeFactory implements RuntimeFactory {

    @Getter
    private ThreadGroup threadGroup;
    private FunctionCacheManager fnCache;
    private PulsarClient pulsarClient;
    private String storageServiceUrl;
    private SecretsProvider defaultSecretsProvider;
    private CollectorRegistry collectorRegistry;
    private String narExtractionDirectory;
    private volatile boolean closed;
    private SecretsProviderConfigurator secretsProviderConfigurator;
    private ClassLoader rootClassLoader;

    /**
     * This constructor is used by other runtimes (e.g. ProcessRuntime and KubernetesRuntime) that rely on ThreadRuntime to actually run an instance of the function.
     * When used by other runtimes, the arguments such as secretsProvider and rootClassLoader will be provided.
     */
    public ThreadRuntimeFactory(String threadGroupName, String pulsarServiceUrl, String storageServiceUrl,
                                AuthenticationConfig authConfig, SecretsProvider secretsProvider,
                                CollectorRegistry collectorRegistry, String narExtractionDirectory,
                                ClassLoader rootClassLoader) throws Exception {
        initialize(threadGroupName, createPulsarClient(pulsarServiceUrl, authConfig),
                storageServiceUrl, null, secretsProvider, collectorRegistry, narExtractionDirectory, rootClassLoader);
    }

    @VisibleForTesting
    public ThreadRuntimeFactory(String threadGroupName, PulsarClient pulsarClient, String storageServiceUrl,
                                SecretsProvider secretsProvider, CollectorRegistry collectorRegistry,
                                String narExtractionDirectory, ClassLoader rootClassLoader) {

        initialize(threadGroupName, pulsarClient, storageServiceUrl,
                null, secretsProvider, collectorRegistry, narExtractionDirectory, rootClassLoader);
    }

    private static PulsarClient createPulsarClient(String pulsarServiceUrl, AuthenticationConfig authConfig)
            throws PulsarClientException {
        ClientBuilder clientBuilder = null;
        if (isNotBlank(pulsarServiceUrl)) {
            clientBuilder = PulsarClient.builder().serviceUrl(pulsarServiceUrl);
            if (authConfig != null) {
                if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                        && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                    clientBuilder.authentication(authConfig.getClientAuthenticationPlugin(),
                            authConfig.getClientAuthenticationParameters());
                }
                clientBuilder.enableTls(authConfig.isUseTls());
                clientBuilder.allowTlsInsecureConnection(authConfig.isTlsAllowInsecureConnection());
                clientBuilder.enableTlsHostnameVerification(authConfig.isTlsHostnameVerificationEnable());
                clientBuilder.tlsTrustCertsFilePath(authConfig.getTlsTrustCertsFilePath());
            }
            clientBuilder.ioThreads(Runtime.getRuntime().availableProcessors());
            return clientBuilder.build();
        }
        return null;
    }

    private void initialize(String threadGroupName, PulsarClient pulsarClient, String storageServiceUrl,
                            SecretsProviderConfigurator secretsProviderConfigurator, SecretsProvider secretsProvider,
                            CollectorRegistry collectorRegistry,  String narExtractionDirectory, ClassLoader rootClassLoader) {
        if (rootClassLoader == null) {
            rootClassLoader = Thread.currentThread().getContextClassLoader();
        }

        this.rootClassLoader = rootClassLoader;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.defaultSecretsProvider = secretsProvider;
        this.fnCache = new FunctionCacheManagerImpl(rootClassLoader);
        this.threadGroup = new ThreadGroup(threadGroupName);
        this.pulsarClient = pulsarClient;
        this.storageServiceUrl = storageServiceUrl;
        this.collectorRegistry = collectorRegistry;
        this.narExtractionDirectory = narExtractionDirectory;
    }

    @Override
    public void initialize(WorkerConfig workerConfig, AuthenticationConfig authenticationConfig,
                           SecretsProviderConfigurator secretsProviderConfigurator,
                           Optional<FunctionAuthProvider> functionAuthProvider,
                           Optional<RuntimeCustomizer> runtimeCustomizer) throws Exception {
        ThreadRuntimeFactoryConfig factoryConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionRuntimeFactoryConfigs(), ThreadRuntimeFactoryConfig.class);

        initialize(factoryConfig.getThreadGroupName(),
                createPulsarClient(workerConfig.getPulsarServiceUrl(), authenticationConfig),
                workerConfig.getStateStorageServiceUrl(), secretsProviderConfigurator, null,
                null, workerConfig.getNarExtractionDirectory(), null);
    }

    @Override
    public ThreadRuntime createContainer(InstanceConfig instanceConfig, String jarFile,
                                         String originalCodeFileName,
                                         Long expectedHealthCheckInterval) {
        SecretsProvider secretsProvider = defaultSecretsProvider;
        if (secretsProvider == null) {
            String secretsProviderClassName = secretsProviderConfigurator.getSecretsProviderClassName(instanceConfig.getFunctionDetails());
            secretsProvider = (SecretsProvider) Reflections.createInstance(secretsProviderClassName, this.rootClassLoader);
            log.info("Initializing secrets provider {} with configs: {}",
              secretsProvider.getClass().getName(), secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
            secretsProvider.init(secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
        }

        return new ThreadRuntime(
            instanceConfig,
            fnCache,
            threadGroup,
            jarFile,
            pulsarClient,
            storageServiceUrl,
            secretsProvider,
            collectorRegistry,
            narExtractionDirectory);
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

        // Shutdown instance cache
        InstanceCache.shutdown();
    }
}
