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

package org.apache.pulsar.functions.runtime.kubernetes;

import com.google.common.annotations.VisibleForTesting;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.Config;
import java.nio.file.Paths;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;

/**
 * Kubernetes based function container factory implementation.
 */
@Slf4j
@Data
public class KubernetesRuntimeFactory implements RuntimeFactory {

    static int NUM_RETRIES = 5;
    static long SLEEP_BETWEEN_RETRIES_MS = 500;

    private String k8Uri;
    private String jobNamespace;
    private String jobName;
    private String pulsarDockerImageName;
    private Map<String, String> functionDockerImages;
    private String imagePullPolicy;
    private String pulsarRootDir;
    private String configAdminCLI;
    private String pulsarAdminUrl;
    private String pulsarServiceUrl;
    private String pythonDependencyRepository;
    private String pythonExtraDependencyRepository;
    private String extraDependenciesDir;
    private String changeConfigMap;
    private String changeConfigMapNamespace;
    private int percentMemoryPadding;
    private double cpuOverCommitRatio;
    private double memoryOverCommitRatio;
    private Boolean submittingInsidePod;
    private Boolean installUserCodeDependencies;
    private Map<String, String> customLabels;
    private Integer expectedMetricsCollectionInterval;
    private String stateStorageServiceUri;
    private AuthenticationConfig authConfig;
    private String javaInstanceJarFile;
    private String pythonInstanceFile;
    private final String logDirectory = "logs/functions";
    private Resources functionInstanceMinResources;
    private Resources functionInstanceMaxResources;
    private Resources functionInstanceResourceGranularities;
    private boolean functionInstanceResourceChangeInLockStep;
    private boolean authenticationEnabled;
    private Integer grpcPort;
    private Integer metricsPort;
    private String narExtractionDirectory;
    private String functionInstanceClassPath;
    private String downloadDirectory;
    private int gracePeriodSeconds;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Timer changeConfigMapTimer;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private AppsV1Api appsClient;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private CoreV1Api coreClient;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private SecretsProviderConfigurator secretsProviderConfigurator;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Optional<KubernetesFunctionAuthProvider> authProvider;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Optional<KubernetesManifestCustomizer> manifestCustomizer;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private byte[] serverCaBytes;

    @Override
    public boolean externallyManaged() {
        return true;
    }

    @Override
    public void initialize(WorkerConfig workerConfig, AuthenticationConfig authenticationConfig,
                           SecretsProviderConfigurator secretsProviderConfigurator,
                           ConnectorsManager connectorsManager,
                           Optional<FunctionAuthProvider> functionAuthProvider,
                           Optional<RuntimeCustomizer> runtimeCustomizer) {

        KubernetesRuntimeFactoryConfig factoryConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionRuntimeFactoryConfigs(), KubernetesRuntimeFactoryConfig.class);

        this.k8Uri = factoryConfig.getK8Uri();
        if (!isEmpty(factoryConfig.getJobNamespace())) {
            this.jobNamespace = factoryConfig.getJobNamespace();
        } else {
            this.jobNamespace = "default";
        }
        if (!isEmpty(factoryConfig.getJobName())) {
            this.jobName = factoryConfig.getJobName();
        } else {
            this.jobName = null;
        }
        if (!isEmpty(factoryConfig.getPulsarDockerImageName())) {
            this.pulsarDockerImageName = factoryConfig.getPulsarDockerImageName();
        } else {
            this.pulsarDockerImageName = "apachepulsar/pulsar";
        }
        this.functionDockerImages = factoryConfig.getFunctionDockerImages();
        if (!isEmpty(factoryConfig.getImagePullPolicy())) {
            this.imagePullPolicy = factoryConfig.getImagePullPolicy();
        } else {
            this.imagePullPolicy = "IfNotPresent";
        }
        if (!isEmpty(factoryConfig.getPulsarRootDir())) {
            this.pulsarRootDir = factoryConfig.getPulsarRootDir();
        } else {
            this.pulsarRootDir = "/pulsar";
        }
        if (!isEmpty(factoryConfig.getConfigAdminCLI())) {
            this.configAdminCLI = factoryConfig.getConfigAdminCLI();
        } else {
            this.configAdminCLI = "/bin/pulsar-admin";
        }
        this.downloadDirectory = isNotEmpty(workerConfig.getDownloadDirectory()) ?
                workerConfig.getDownloadDirectory() : this.pulsarRootDir; // for backward comp
        if (!Paths.get(this.downloadDirectory).isAbsolute()) {
            this.downloadDirectory = this.pulsarRootDir + "/" + this.downloadDirectory;
        }

        this.submittingInsidePod = factoryConfig.getSubmittingInsidePod();
        this.installUserCodeDependencies = factoryConfig.getInstallUserCodeDependencies();
        this.pythonDependencyRepository = factoryConfig.getPythonDependencyRepository();
        this.pythonExtraDependencyRepository = factoryConfig.getPythonExtraDependencyRepository();

        if (StringUtils.isNotEmpty(factoryConfig.getExtraFunctionDependenciesDir())) {
            if (Paths.get(factoryConfig.getExtraFunctionDependenciesDir()).isAbsolute()) {
                this.extraDependenciesDir = factoryConfig.getExtraFunctionDependenciesDir();
            } else {
                this.extraDependenciesDir = this.pulsarRootDir
                        + "/" + factoryConfig.getExtraFunctionDependenciesDir();
            }
        } else {
            this.extraDependenciesDir = this.pulsarRootDir + "/instances/deps";
        }

        this.customLabels = factoryConfig.getCustomLabels();
        this.percentMemoryPadding = factoryConfig.getPercentMemoryPadding();
        this.cpuOverCommitRatio = factoryConfig.getCpuOverCommitRatio();
        this.memoryOverCommitRatio = factoryConfig.getMemoryOverCommitRatio();
        this.gracePeriodSeconds = factoryConfig.getGracePeriodSeconds();
        this.pulsarServiceUrl = StringUtils.isEmpty(factoryConfig.getPulsarServiceUrl())
                ? workerConfig.getPulsarServiceUrl() : factoryConfig.getPulsarServiceUrl();
        this.pulsarAdminUrl = StringUtils.isEmpty(factoryConfig.getPulsarAdminUrl())
                ? workerConfig.getPulsarWebServiceUrl() : factoryConfig.getPulsarAdminUrl();
        this.stateStorageServiceUri = workerConfig.getStateStorageServiceUrl();
        this.authConfig = authenticationConfig;
        this.expectedMetricsCollectionInterval = factoryConfig.getExpectedMetricsCollectionInterval() == null
                ? -1 : factoryConfig.getExpectedMetricsCollectionInterval();
        this.changeConfigMap = factoryConfig.getChangeConfigMap();
        this.changeConfigMapNamespace = factoryConfig.getChangeConfigMapNamespace();
        this.functionInstanceMinResources = workerConfig.getFunctionInstanceMinResources();
        this.functionInstanceMaxResources = workerConfig.getFunctionInstanceMaxResources();
        this.functionInstanceResourceGranularities = workerConfig.getFunctionInstanceResourceGranularities();
        this.functionInstanceResourceChangeInLockStep = workerConfig.isFunctionInstanceResourceChangeInLockStep();
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.authenticationEnabled = workerConfig.isAuthenticationEnabled();
        this.javaInstanceJarFile = this.pulsarRootDir + "/instances/java-instance.jar";
        this.pythonInstanceFile = this.pulsarRootDir + "/instances/python-instance/python_instance_main.py";
        this.serverCaBytes = workerConfig.getTlsTrustChainBytes();
        try {
            setupClient();
        } catch (Exception e) {
            log.error("Failed to setup client", e);
            throw new RuntimeException(e);
        }
        // make sure the provided class is a kubernetes auth provider, this needs to run before the authProvider!
        if (runtimeCustomizer.isPresent()) {
            if (!(runtimeCustomizer.get() instanceof KubernetesManifestCustomizer)) {
                throw new IllegalArgumentException("Function runtime customizer "
                        + runtimeCustomizer.get().getClass().getName() + " must implement KubernetesManifestCustomizer");
            } else {
                KubernetesManifestCustomizer manifestCustomizer = (KubernetesManifestCustomizer) runtimeCustomizer.get();
                this.manifestCustomizer = Optional.of(manifestCustomizer);
            }
        } else {
            this.manifestCustomizer = Optional.empty();
        }

        // make sure the provided class is a kubernetes auth provider
        if (functionAuthProvider.isPresent()) {
            if (!(functionAuthProvider.get() instanceof KubernetesFunctionAuthProvider)) {
                throw new IllegalArgumentException("Function authentication provider "
                        + functionAuthProvider.get().getClass().getName() + " must implement KubernetesFunctionAuthProvider");
            } else {
                KubernetesFunctionAuthProvider kubernetesFunctionAuthProvider = (KubernetesFunctionAuthProvider) functionAuthProvider.get();
                kubernetesFunctionAuthProvider.initialize(coreClient, serverCaBytes, (funcDetails) -> getRuntimeCustomizer().map((customizer) -> customizer.customizeNamespace(funcDetails, jobNamespace)).orElse(jobNamespace));
                this.authProvider = Optional.of(kubernetesFunctionAuthProvider);
            }
        } else {
            this.authProvider = Optional.empty();
        }

        this.grpcPort = factoryConfig.getGrpcPort();
        this.metricsPort = factoryConfig.getMetricsPort();
        this.narExtractionDirectory = factoryConfig.getNarExtractionDirectory();
        this.functionInstanceClassPath = factoryConfig.getFunctionInstanceClassPath();
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
                break;
            default:
                throw new RuntimeException("Unsupported Runtime " + instanceConfig.getFunctionDetails().getRuntime());
        }

        // adjust the auth config to support auth
        if (authenticationEnabled) {
            authProvider.ifPresent(kubernetesFunctionAuthProvider ->
                    kubernetesFunctionAuthProvider.configureAuthenticationConfig(authConfig,
                            Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(instanceConfig.getFunctionAuthenticationSpec())))));

        }

        Optional<KubernetesManifestCustomizer> manifestCustomizer = getRuntimeCustomizer();
        String overriddenNamespace = manifestCustomizer.map((customizer) -> customizer.customizeNamespace(instanceConfig.getFunctionDetails(), jobNamespace)).orElse(jobNamespace);
        String overriddenName = manifestCustomizer.map((customizer) -> customizer.customizeName(instanceConfig.getFunctionDetails(), jobName)).orElse(jobName);

        return new KubernetesRuntime(
            appsClient,
            coreClient,
            // get the namespace for this function
            overriddenNamespace,
            overriddenName,
            customLabels,
            installUserCodeDependencies,
            pythonDependencyRepository,
            pythonExtraDependencyRepository,
            pulsarDockerImageName,
            functionDockerImages,
            imagePullPolicy,
            pulsarRootDir,
            instanceConfig,
            instanceFile,
            extraDependenciesDir,
            logDirectory,
            configAdminCLI,
            codePkgUrl,
            originalCodeFileName,
            pulsarServiceUrl,
            pulsarAdminUrl,
            stateStorageServiceUri,
            authConfig,
            secretsProviderConfigurator,
            expectedMetricsCollectionInterval,
            percentMemoryPadding,
            cpuOverCommitRatio,
            memoryOverCommitRatio,
            gracePeriodSeconds,
            authProvider,
            authenticationEnabled,
            grpcPort,
            narExtractionDirectory,
            manifestCustomizer,
            functionInstanceClassPath,
            downloadDirectory);
    }

    @Override
    public void close() {
    }

    @Override
    public void doAdmissionChecks(Function.FunctionDetails functionDetails) {
    	final String overriddenJobName = getOverriddenName(functionDetails);
        KubernetesRuntime.doChecks(functionDetails, overriddenJobName);
        validateMinResourcesRequired(functionDetails);
        validateMaxResourcesRequired(functionDetails);
        validateResourcesGranularityAndProportion(functionDetails);
        secretsProviderConfigurator.doAdmissionChecks(appsClient, coreClient,
        		getOverriddenNamespace(functionDetails), overriddenJobName, functionDetails);
    }

    @VisibleForTesting
    public void setupClient() throws Exception {
        if (appsClient == null) {
            if (k8Uri == null) {
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
                log.info("Setting up k8Client using uri " + k8Uri);
                final ApiClient apiClient = new ApiClient().setBasePath(k8Uri);
                appsClient = new AppsV1Api(apiClient);
                coreClient = new CoreV1Api(apiClient);
            }

            // Setup a timer to change stuff.
            if (!isEmpty(changeConfigMap)) {
                changeConfigMapTimer = new Timer();
                final KubernetesRuntimeFactory THIS = this;
                changeConfigMapTimer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        fetchConfigMap(coreClient, changeConfigMap, changeConfigMapNamespace, THIS);
                    }
                }, 300000, 300000);
            }
        }
    }

    static void fetchConfigMap(CoreV1Api coreClient, String changeConfigMap,
                               String changeConfigMapNamespace,
                               KubernetesRuntimeFactory kubernetesRuntimeFactory) {
        try {
            V1ConfigMap v1ConfigMap = coreClient.readNamespacedConfigMap(changeConfigMap, changeConfigMapNamespace, null, true, false);
            Map<String, String> data = v1ConfigMap.getData();
            if (data != null) {
                overRideKubernetesConfig(data, kubernetesRuntimeFactory);
            }
        } catch (Exception e) {
            log.error("Error while trying to fetch configmap {} at namespace {}", changeConfigMap, changeConfigMapNamespace, e);
        }
    }

    static void overRideKubernetesConfig(Map<String, String> data,
                                         KubernetesRuntimeFactory kubernetesRuntimeFactory) throws Exception {
        for (Field field : KubernetesRuntimeFactory.class.getDeclaredFields()) {
            field.setAccessible(true);
            if (data.containsKey(field.getName()) && !data.get(field.getName()).equals(field.get(kubernetesRuntimeFactory))) {
                log.info("Kubernetes Config {} changed from {} to {}", field.getName(), field.get(kubernetesRuntimeFactory), data.get(field.getName()));
                field.set(kubernetesRuntimeFactory, data.get(field.getName()));
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

    void validateMaxResourcesRequired(Function.FunctionDetails functionDetails) {
        if (functionInstanceMaxResources != null) {
            Double maxCpu = functionInstanceMaxResources.getCpu();
            Long maxRam = functionInstanceMaxResources.getRam();

            if (maxCpu != null && functionDetails.getResources().getCpu() > maxCpu) {
                throw new IllegalArgumentException(
                        String.format("Per instance CPU requested, %s, for function is greater than the maximum required, %s",
                                functionDetails.getResources().getCpu(), maxCpu));
            }

            if (maxRam != null && functionDetails.getResources().getRam() > maxRam) {
                throw new IllegalArgumentException(
                        String.format("Per instance RAM requested, %s, for function is greater than the maximum required, %s",
                                functionDetails.getResources().getRam(), maxRam));
            }
        }
    }

    void validateResourcesGranularityAndProportion(Function.FunctionDetails functionDetails) {
        final long baseMillis = 1000;
        long multiples = 0L;
        if (functionInstanceResourceGranularities != null) {
            Double grnCpu = functionInstanceResourceGranularities.getCpu();
            Long grnRam = functionInstanceResourceGranularities.getRam();
            if (grnCpu != null) {
                // convert cpus to milli-cores to avoid loss of precision
                long grnCpuMillis = Math.round(baseMillis * grnCpu);
                if (grnCpuMillis > 0) {
                    long cpuMillis = Math.round(baseMillis * functionDetails.getResources().getCpu());
                    if (cpuMillis == 0 || cpuMillis % grnCpuMillis != 0) {
                        throw new IllegalArgumentException(
                                String.format("Per instance cpu requested, %s, for function should be positive and a multiple of the granularity, %s",
                                        functionDetails.getResources().getCpu(), grnCpu));
                    }
                    if (functionInstanceResourceChangeInLockStep) {
                        multiples = cpuMillis / grnCpuMillis;
                    }
                }
            }
            if (grnRam != null && grnRam > 0) {
                if (functionDetails.getResources().getRam() == 0 || functionDetails.getResources().getRam() % grnRam != 0) {
                    throw new IllegalArgumentException(
                            String.format("Per instance ram requested, %s, for function should be positive and a multiple of the granularity, %s",
                                    functionDetails.getResources().getRam(), grnRam));
                }
                if (functionInstanceResourceChangeInLockStep && multiples > 0) {
                    long ramMultiples = functionDetails.getResources().getRam() / grnRam;
                    if (multiples != ramMultiples) {
                        throw new IllegalArgumentException(
                                String.format("Per instance cpu requested, %s, ram requested, %s," +
                                                " for function should be positive and the same multiple of the granularity, cpu, %s, ram, %s",
                                        functionDetails.getResources().getCpu(), functionDetails.getResources().getRam(),
                                        grnCpu, grnRam));
                    }
                }
            }
        }
    }

    @Override
    public Optional<KubernetesFunctionAuthProvider> getAuthProvider() {
        return authProvider;
    }

    @Override
    public Optional<KubernetesManifestCustomizer> getRuntimeCustomizer() {
        return manifestCustomizer;
    }

    private String getOverriddenNamespace(Function.FunctionDetails funcDetails) {
        Optional<KubernetesManifestCustomizer> manifestCustomizer = getRuntimeCustomizer();
        return manifestCustomizer.map((customizer) -> customizer.customizeNamespace(funcDetails, jobNamespace)).orElse(jobNamespace);
    }

    private String getOverriddenName(Function.FunctionDetails funcDetails) {
        Optional<KubernetesManifestCustomizer> manifestCustomizer = getRuntimeCustomizer();
        return manifestCustomizer.map((customizer) -> customizer.customizeName(funcDetails, jobName)).orElse(jobName);
    }
}
