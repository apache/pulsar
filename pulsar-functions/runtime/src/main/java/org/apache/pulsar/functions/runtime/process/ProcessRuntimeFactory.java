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

package org.apache.pulsar.functions.runtime.process;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;

import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
@NoArgsConstructor
@Data
public class ProcessRuntimeFactory implements RuntimeFactory {

    private String pulsarServiceUrl;
    private String pulsarWebServiceUrl;
    private String stateStorageServiceUrl;
    private boolean authenticationEnabled;
    private AuthenticationConfig authConfig;
    private String javaInstanceJarFile;
    private String pythonInstanceFile;
    private String logDirectory;
    private String extraDependenciesDir;
    private String narExtractionDirectory;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private SecretsProviderConfigurator secretsProviderConfigurator;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Optional<FunctionAuthProvider> authProvider;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Optional<RuntimeCustomizer> runtimeCustomizer;

    @VisibleForTesting
    public ProcessRuntimeFactory(String pulsarServiceUrl,
                                 String pulsarWebServiceUrl,
                                 String stateStorageServiceUrl,
                                 AuthenticationConfig authConfig,
                                 String javaInstanceJarFile,
                                 String pythonInstanceFile,
                                 String logDirectory,
                                 String extraDependenciesDir,
                                 String narExtractionDirectory,
                                 SecretsProviderConfigurator secretsProviderConfigurator,
                                 boolean authenticationEnabled,
                                 Optional<FunctionAuthProvider> functionAuthProvider,
                                 Optional<RuntimeCustomizer> runtimeCustomizer) {

        initialize(pulsarServiceUrl, pulsarWebServiceUrl, stateStorageServiceUrl, authConfig, javaInstanceJarFile,
                pythonInstanceFile, logDirectory, extraDependenciesDir, narExtractionDirectory,
                secretsProviderConfigurator, authenticationEnabled, functionAuthProvider, runtimeCustomizer);
    }

    @Override
    public void initialize(WorkerConfig workerConfig, AuthenticationConfig authenticationConfig,
                           SecretsProviderConfigurator secretsProviderConfigurator,
                           ConnectorsManager connectorsManager,
                           Optional<FunctionAuthProvider> authProvider,
                           Optional<RuntimeCustomizer> runtimeCustomizer) {
        ProcessRuntimeFactoryConfig factoryConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionRuntimeFactoryConfigs(), ProcessRuntimeFactoryConfig.class);

        initialize(workerConfig.getPulsarServiceUrl(),
                workerConfig.getPulsarWebServiceUrl(),
                workerConfig.getStateStorageServiceUrl(),
                authenticationConfig,
                factoryConfig.getJavaInstanceJarLocation(),
                factoryConfig.getPythonInstanceLocation(),
                factoryConfig.getLogDirectory(),
                factoryConfig.getExtraFunctionDependenciesDir(),
                workerConfig.getNarExtractionDirectory(),
                secretsProviderConfigurator,
                workerConfig.isAuthenticationEnabled(),
                authProvider,
                runtimeCustomizer);
    }

    private void initialize(String pulsarServiceUrl,
                            String pulsarWebServiceUrl,
                            String stateStorageServiceUrl,
                            AuthenticationConfig authConfig,
                            String javaInstanceJarFile,
                            String pythonInstanceFile,
                            String logDirectory,
                            String extraDependenciesDir,
                            String narExtractionDirectory,
                            SecretsProviderConfigurator secretsProviderConfigurator,
                            boolean authenticationEnabled,
                            Optional<FunctionAuthProvider> functionAuthProvider,
                            Optional<RuntimeCustomizer> runtimeCustomizer) {
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarWebServiceUrl = pulsarWebServiceUrl;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.authConfig = authConfig;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.javaInstanceJarFile = javaInstanceJarFile;
        this.pythonInstanceFile = pythonInstanceFile;
        this.extraDependenciesDir = extraDependenciesDir;
        this.narExtractionDirectory = narExtractionDirectory;
        this.logDirectory = logDirectory;
        this.authenticationEnabled = authenticationEnabled;

        // if things are not specified, try to figure out by env properties
        if (this.javaInstanceJarFile == null) {
            String envJavaInstanceJarLocation = System.getProperty(FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY);
            if (null != envJavaInstanceJarLocation) {
                log.info("Java instance jar location is not defined,"
                        + " using the location defined in system environment : {}", envJavaInstanceJarLocation);
                this.javaInstanceJarFile = envJavaInstanceJarLocation;
            } else {
                throw new RuntimeException("No JavaInstanceJar specified");
            }
        }

        if (this.pythonInstanceFile == null) {
            String envPythonInstanceLocation = System.getProperty("pulsar.functions.python.instance.file");
            if (null != envPythonInstanceLocation) {
                log.info("Python instance file location is not defined"
                        + " using the location defined in system environment : {}", envPythonInstanceLocation);
                this.pythonInstanceFile = envPythonInstanceLocation;
            } else {
                throw new RuntimeException("No PythonInstanceFile specified");
            }
        }

        if (this.logDirectory == null) {
            String envProcessContainerLogDirectory = System.getProperty("pulsar.functions.process.container.log.dir");
            if (null != envProcessContainerLogDirectory) {
                this.logDirectory = envProcessContainerLogDirectory;
            } else {
                // use a default location
                this.logDirectory = Paths.get("logs").toFile().getAbsolutePath();
            }
        }
        this.logDirectory = this.logDirectory + "/functions";

        if (this.extraDependenciesDir == null) {
            String envProcessContainerExtraDependenciesDir =
                    System.getProperty("pulsar.functions.extra.dependencies.dir");
            if (null != envProcessContainerExtraDependenciesDir) {
                log.info("Extra dependencies location is not defined using"
                        + " the location defined in system environment : {}", envProcessContainerExtraDependenciesDir);
                this.extraDependenciesDir = envProcessContainerExtraDependenciesDir;
            } else {
                log.info("No extra dependencies location is defined in either"
                        + " function worker config or system environment");
            }
        }

        authProvider = functionAuthProvider;
        this.runtimeCustomizer = runtimeCustomizer;
    }

    @Override
    public ProcessRuntime createContainer(InstanceConfig instanceConfig, String codeFile,
                                          String originalCodeFileName,
                                          Long expectedHealthCheckInterval) throws Exception {
        String instanceFile = null;
        switch (instanceConfig.getFunctionDetails().getRuntime()) {
            case JAVA:
                instanceFile = javaInstanceJarFile;
                break;
            case PYTHON:
                instanceFile = pythonInstanceFile;
                break;
            case GO:
                break;
            default:
                throw new RuntimeException("Unsupported Runtime " + instanceConfig.getFunctionDetails().getRuntime());
        }

        // configure auth if necessary
        if (authenticationEnabled) {
            authProvider.ifPresent(functionAuthProvider -> functionAuthProvider.configureAuthenticationConfig(authConfig,
                    Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(instanceConfig.getFunctionAuthenticationSpec())))));
        }

        return new ProcessRuntime(
            instanceConfig,
            instanceFile,
            extraDependenciesDir,
            narExtractionDirectory,
            logDirectory,
            codeFile,
            pulsarServiceUrl,
            stateStorageServiceUrl,
            authConfig,
            secretsProviderConfigurator,
            expectedHealthCheckInterval,
            pulsarWebServiceUrl);
    }

    @Override
    public Optional<FunctionAuthProvider> getAuthProvider() {
        return authProvider;
    }

    @Override
    public Optional<RuntimeCustomizer> getRuntimeCustomizer() {
        return runtimeCustomizer;
    }

    @Override
    public void close() {
    }
}
