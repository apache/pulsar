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

import com.google.common.annotations.VisibleForTesting;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.util.Config;
import java.nio.file.Paths;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider;
import org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;

/**
 * Kubernetes based function container factory implementation.
 */
@Slf4j
public class KubernetesRuntimeFactory implements RuntimeFactory {

    static int NUM_RETRIES = 5;
    static long SLEEP_BETWEEN_RETRIES_MS = 500;

    @Getter
    @Setter
    @NoArgsConstructor
    class KubernetesInfo {
        private String k8Uri;
        private String jobNamespace;
        private String pulsarDockerImageName;
        private String imagePullPolicy;
        private String pulsarRootDir;
        private String pulsarAdminUrl;
        private String pulsarServiceUrl;
        private String pythonDependencyRepository;
        private String pythonExtraDependencyRepository;
        private String extraDependenciesDir;
        private String changeConfigMap;
        private String changeConfigMapNamespace;
        private int percentMemoryPadding;
    }
    private final KubernetesInfo kubernetesInfo;
    private final Boolean submittingInsidePod;
    private final Boolean installUserCodeDependencies;
    private final Map<String, String> customLabels;
    private final Integer expectedMetricsCollectionInterval;
    private final String stateStorageServiceUri;
    private final AuthenticationConfig authConfig;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String extraDependenciesDir;
    private final SecretsProviderConfigurator secretsProviderConfigurator;
    private final String logDirectory = "logs/functions";
    private Timer changeConfigMapTimer;
    private AppsV1Api appsClient;
    private CoreV1Api coreClient;
    private Resources functionInstanceMinResources;
    private final boolean authenticationEnabled;

    @VisibleForTesting
    public KubernetesRuntimeFactory(String k8Uri,
                                    String jobNamespace,
                                    String pulsarDockerImageName,
                                    String imagePullPolicy,
                                    String pulsarRootDir,
                                    Boolean submittingInsidePod,
                                    Boolean installUserCodeDependencies,
                                    String pythonDependencyRepository,
                                    String pythonExtraDependencyRepository,
                                    String extraDependenciesDir,
                                    Map<String, String> customLabels,
                                    int percentMemoryPadding,
                                    String pulsarServiceUri,
                                    String pulsarAdminUri,
                                    String stateStorageServiceUri,
                                    AuthenticationConfig authConfig,
                                    Integer expectedMetricsCollectionInterval,
                                    String changeConfigMap,
                                    String changeConfigMapNamespace,
                                    Resources functionInstanceMinResources,
                                    SecretsProviderConfigurator secretsProviderConfigurator,
                                    boolean authenticationEnabled) {
        this.kubernetesInfo = new KubernetesInfo();
        this.kubernetesInfo.setK8Uri(k8Uri);
        if (!isEmpty(jobNamespace)) {
            this.kubernetesInfo.setJobNamespace(jobNamespace);
        } else {
            this.kubernetesInfo.setJobNamespace("default");
        }
        if (!isEmpty(pulsarDockerImageName)) {
            this.kubernetesInfo.setPulsarDockerImageName(pulsarDockerImageName);
        } else {
            this.kubernetesInfo.setPulsarDockerImageName("apachepulsar/pulsar");
        }
        if (!isEmpty(imagePullPolicy)) {
            this.kubernetesInfo.setImagePullPolicy(imagePullPolicy);
        } else {
            this.kubernetesInfo.setImagePullPolicy("IfNotPresent");
        }
        if (!isEmpty(pulsarRootDir)) {
            this.kubernetesInfo.setPulsarRootDir(pulsarRootDir);
        } else {
            this.kubernetesInfo.setPulsarRootDir("/pulsar");
        }
        if (StringUtils.isNotEmpty(extraDependenciesDir)) {
            if (Paths.get(extraDependenciesDir).isAbsolute()) {
                this.extraDependenciesDir = extraDependenciesDir;
            } else {
                this.extraDependenciesDir = this.kubernetesInfo.getPulsarRootDir()
                    + "/" + extraDependenciesDir;
            }
        } else {
            this.extraDependenciesDir = this.kubernetesInfo.getPulsarRootDir() + "/instances/deps";
        }
        this.kubernetesInfo.setExtraDependenciesDir(extraDependenciesDir);
        this.kubernetesInfo.setPythonDependencyRepository(pythonDependencyRepository);
        this.kubernetesInfo.setPythonExtraDependencyRepository(pythonExtraDependencyRepository);
        this.kubernetesInfo.setPulsarServiceUrl(pulsarServiceUri);
        this.kubernetesInfo.setPulsarAdminUrl(pulsarAdminUri);
        this.kubernetesInfo.setChangeConfigMap(changeConfigMap);
        this.kubernetesInfo.setChangeConfigMapNamespace(changeConfigMapNamespace);
        this.kubernetesInfo.setPercentMemoryPadding(percentMemoryPadding);
        this.submittingInsidePod = submittingInsidePod;
        this.installUserCodeDependencies = installUserCodeDependencies;
        this.customLabels = customLabels;
        this.stateStorageServiceUri = stateStorageServiceUri;
        this.authConfig = authConfig;
        this.javaInstanceJarFile = this.kubernetesInfo.getPulsarRootDir() + "/instances/java-instance.jar";
        this.pythonInstanceFile = this.kubernetesInfo.getPulsarRootDir() + "/instances/python-instance/python_instance_main.py";
        this.expectedMetricsCollectionInterval = expectedMetricsCollectionInterval == null ? -1 : expectedMetricsCollectionInterval;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.functionInstanceMinResources = functionInstanceMinResources;
        this.authenticationEnabled = authenticationEnabled;
        try {
            setupClient();
        } catch (Exception e) {
            log.error("Failed to setup client", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean externallyManaged() {
        return true;
    }

    @Override
    public KubernetesRuntime createContainer(InstanceConfig instanceConfig, String codePkgUrl,
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
                throw new UnsupportedOperationException();
            default:
                throw new RuntimeException("Unsupported Runtime " + instanceConfig.getFunctionDetails().getRuntime());
        }

        // adjust the auth config to support auth
        if (authenticationEnabled) {
            getAuthProvider().configureAuthenticationConfig(authConfig,
                    Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(instanceConfig.getFunctionAuthenticationSpec()))));
        }

        return new KubernetesRuntime(
            appsClient,
            coreClient,
            this.kubernetesInfo.getJobNamespace(),
            customLabels,
            installUserCodeDependencies,
            this.kubernetesInfo.getPythonDependencyRepository(),
            this.kubernetesInfo.getPythonExtraDependencyRepository(),
            this.kubernetesInfo.getPulsarDockerImageName(),
            this.kubernetesInfo.imagePullPolicy,
            this.kubernetesInfo.getPulsarRootDir(),
            instanceConfig,
            instanceFile,
            extraDependenciesDir,
            logDirectory,
            codePkgUrl,
            originalCodeFileName,
            this.kubernetesInfo.getPulsarServiceUrl(),
            this.kubernetesInfo.getPulsarAdminUrl(),
            stateStorageServiceUri,
            authConfig,
            secretsProviderConfigurator,
            expectedMetricsCollectionInterval,
            this.kubernetesInfo.getPercentMemoryPadding(),
            getAuthProvider(),
            authenticationEnabled);
    }

    @Override
    public void close() {
    }

    @Override
    public void doAdmissionChecks(Function.FunctionDetails functionDetails) {
        KubernetesRuntime.doChecks(functionDetails);
        validateMinResourcesRequired(functionDetails);
        secretsProviderConfigurator.doAdmissionChecks(appsClient, coreClient, kubernetesInfo.getJobNamespace(), functionDetails);
    }

    @VisibleForTesting
    public void setupClient() throws Exception {
        if (appsClient == null) {
            if (this.kubernetesInfo.getK8Uri() == null) {
                log.info("k8Uri is null thus going by defaults");
                ApiClient cli;
                if (submittingInsidePod) {
                    log.info("Looks like we are inside a k8 pod ourselves. Initializing as cluster");
                    cli = Config.fromCluster();
                } else {
                    log.info("Using default cluster since we are not running inside k8");
                    cli = Config.defaultClient();
                }
                Configuration.setDefaultApiClient(cli);
                appsClient = new AppsV1Api();
                coreClient = new CoreV1Api();
            } else {
                log.info("Setting up k8Client using uri " + this.kubernetesInfo.getK8Uri());
                final ApiClient apiClient = new ApiClient().setBasePath(this.kubernetesInfo.getK8Uri());
                appsClient = new AppsV1Api(apiClient);
                coreClient = new CoreV1Api(apiClient);
            }

            // Setup a timer to change stuff.
            if (!isEmpty(this.kubernetesInfo.getChangeConfigMap())) {
                changeConfigMapTimer = new Timer();
                changeConfigMapTimer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        fetchConfigMap();
                    }
                }, 300000, 300000);
            }
        }
    }

    void fetchConfigMap() {
        try {
            V1ConfigMap v1ConfigMap = coreClient.readNamespacedConfigMap(kubernetesInfo.getChangeConfigMap(), kubernetesInfo.getChangeConfigMapNamespace(), null, true, false);
            Map<String, String> data = v1ConfigMap.getData();
            if (data != null) {
                overRideKubernetesConfig(data);
            }
        } catch (Exception e) {
            log.error("Error while trying to fetch configmap {} at namespace {}", kubernetesInfo.getChangeConfigMap(), kubernetesInfo.getChangeConfigMapNamespace(), e);
        }
    }

    void overRideKubernetesConfig(Map<String, String> data) throws Exception {
        for (Field field : KubernetesInfo.class.getDeclaredFields()) {
            field.setAccessible(true);
            if (data.containsKey(field.getName()) && !data.get(field.getName()).equals(field.get(kubernetesInfo))) {
                log.info("Kubernetes Config {} changed from {} to {}", field.getName(), field.get(kubernetesInfo), data.get(field.getName()));
                field.set(kubernetesInfo, data.get(field.getName()));
            }
        }
    }

    void validateMinResourcesRequired(Function.FunctionDetails functionDetails) {
        if (functionInstanceMinResources != null) {
            Double minCpu = functionInstanceMinResources.getCpu();
            Long minRam = functionInstanceMinResources.getRam();

            if (minCpu != null) {
                if (functionDetails.getResources() == null) {
                    throw new IllegalArgumentException(
                            String.format("Per instance CPU requested is not specified. Must specify CPU requested for function to be at least %s", minCpu));
                } else if (functionDetails.getResources().getCpu() < minCpu) {
                    throw new IllegalArgumentException(
                            String.format("Per instance CPU requested, %s, for function is less than the minimum required, %s",
                                    functionDetails.getResources().getCpu(), minCpu));
                }
            }

            if (minRam != null) {
                if (functionDetails.getResources() == null) {
                    throw new IllegalArgumentException(
                            String.format("Per instance RAM requested is not specified. Must specify RAM requested for function to be at least %s", minRam));
                } else if (functionDetails.getResources().getRam() < minRam) {
                    throw new IllegalArgumentException(
                            String.format("Per instance RAM requested, %s, for function is less than the minimum required, %s",
                                    functionDetails.getResources().getRam(), minRam));
                }
            }
        }
    }

    @Override
    public KubernetesFunctionAuthProvider getAuthProvider() {
        return new KubernetesSecretsTokenAuthProvider(coreClient, kubernetesInfo.jobNamespace);
    }
}
