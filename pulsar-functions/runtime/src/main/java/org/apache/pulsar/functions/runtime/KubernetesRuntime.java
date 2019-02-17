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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.protobuf.Empty;
import com.squareup.okhttp.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetSpec;
import io.kubernetes.client.models.V1Toleration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

/**
 * Kubernetes based runtime for running functions.
 * This runtime provides the usual methods to start/stop/getfunctionstatus
 * interfaces to control the kubernetes job running function.
 * We first create a headless service and then a statefulset for starting function pods
 * Each function instance runs as a pod itself. The reason using statefulset as opposed
 * to a regular deployment is that functions require a unique instance_id for each instance.
 * The service abstraction is used for getting functionstatus.
 */
@Slf4j
@VisibleForTesting
public class KubernetesRuntime implements Runtime {

    private static int NUM_RETRIES = 5;
    private static long SLEEP_BETWEEN_RETRIES_MS = 500;

    private static final String ENV_SHARD_ID = "SHARD_ID";
    private static final int maxJobNameSize = 55;
    private static final Integer GRPC_PORT = 9093;
    private static final Integer METRICS_PORT = 9094;
    public static final Pattern VALID_POD_NAME_REGEX =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
                    Pattern.CASE_INSENSITIVE);
    private static final String PULSARFUNCTIONS_CONTAINER_NAME = "pulsarfunction";

    private final AppsV1Api appsClient;
    private final CoreV1Api coreClient;
    static final List<String> TOLERATIONS = Collections.unmodifiableList(
            Arrays.asList(
                    "node.kubernetes.io/not-ready",
                    "node.alpha.kubernetes.io/notReady",
                    "node.alpha.kubernetes.io/unreachable"
            )
    );
    private static final long GRPC_TIMEOUT_SECS = 5;

    // The thread that invokes the function
    @Getter
    private List<String> processArgs;
    @Getter
    private ManagedChannel[] channel;
    private InstanceControlGrpc.InstanceControlFutureStub[] stub;
    private InstanceConfig instanceConfig;
    private final String jobNamespace;
    private final Map<String, String> customLabels;
    private final String pulsarDockerImageName;
    private final String imagePullPolicy;
    private final String pulsarRootDir;
    private final String userCodePkgUrl;
    private final String originalCodeFileName;
    private final String pulsarAdminUrl;
    private final SecretsProviderConfigurator secretsProviderConfigurator;
    private int percentMemoryPadding;


    KubernetesRuntime(AppsV1Api appsClient,
                      CoreV1Api coreClient,
                      String jobNamespace,
                      Map<String, String> customLabels,
                      Boolean installUserCodeDependencies,
                      String pythonDependencyRepository,
                      String pythonExtraDependencyRepository,
                      String pulsarDockerImageName,
                      String imagePullPolicy,
                      String pulsarRootDir,
                      InstanceConfig instanceConfig,
                      String instanceFile,
                      String extraDependenciesDir,
                      String logDirectory,
                      String userCodePkgUrl,
                      String originalCodeFileName,
                      String pulsarServiceUrl,
                      String pulsarAdminUrl,
                      String stateStorageServiceUrl,
                      AuthenticationConfig authConfig,
                      SecretsProviderConfigurator secretsProviderConfigurator,
                      Integer expectedMetricsCollectionInterval,
                      int percentMemoryPadding) throws Exception {
        this.appsClient = appsClient;
        this.coreClient = coreClient;
        this.instanceConfig = instanceConfig;
        this.jobNamespace = jobNamespace;
        this.customLabels = customLabels;
        this.pulsarDockerImageName = pulsarDockerImageName;
        this.imagePullPolicy = imagePullPolicy;
        this.pulsarRootDir = pulsarRootDir;
        this.userCodePkgUrl = userCodePkgUrl;
        this.originalCodeFileName = pulsarRootDir + "/" + originalCodeFileName;
        this.pulsarAdminUrl = pulsarAdminUrl;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.percentMemoryPadding = percentMemoryPadding;
        String logConfigFile = null;
        String secretsProviderClassName = secretsProviderConfigurator.getSecretsProviderClassName(instanceConfig.getFunctionDetails());
        String secretsProviderConfig = null;
        if (secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()) != null) {
            secretsProviderConfig = new Gson().toJson(secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
        }
        switch (instanceConfig.getFunctionDetails().getRuntime()) {
            case JAVA:
                logConfigFile = "kubernetes_instance_log4j2.yml";
                break;
            case PYTHON:
                logConfigFile = pulsarRootDir + "/conf/functions-logging/console_logging_config.ini";
                break;
        }
        this.processArgs = RuntimeUtils.composeArgs(
            instanceConfig,
            instanceFile,
            extraDependenciesDir,
            logDirectory,
            this.originalCodeFileName,
            pulsarServiceUrl,
            stateStorageServiceUrl,
            authConfig,
            "$" + ENV_SHARD_ID,
            GRPC_PORT,
            -1l,
            logConfigFile,
            secretsProviderClassName,
            secretsProviderConfig,
            installUserCodeDependencies,
            pythonDependencyRepository,
            pythonExtraDependencyRepository,
                METRICS_PORT);
        doChecks(instanceConfig.getFunctionDetails());
    }

    /**
     * The core logic that creates a service first followed by statefulset
     */
    @Override
    public void start() throws Exception {
        submitService();
        try {
            submitStatefulSet();
        } catch (Exception e) {
            log.error("Could not submit statefulset for {}/{}/{}, deleting service as well",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(), e);
            deleteService();
        }
        if (channel == null && stub == null) {
            channel = new ManagedChannel[instanceConfig.getFunctionDetails().getParallelism()];
            stub = new InstanceControlGrpc.InstanceControlFutureStub[instanceConfig.getFunctionDetails().getParallelism()];
            for (int i = 0; i < instanceConfig.getFunctionDetails().getParallelism(); ++i) {
                String address = createJobName(instanceConfig.getFunctionDetails()) + "-" +
                        i + "." + createJobName(instanceConfig.getFunctionDetails());
                channel[i] = ManagedChannelBuilder.forAddress(address, GRPC_PORT)
                        .usePlaintext(true)
                        .build();
                stub[i] = InstanceControlGrpc.newFutureStub(channel[i]);
            }
        }
    }

    @Override
    public void join() throws Exception {
        // K8 functions never return
        this.wait();
    }

    @Override
    public void stop() throws Exception {
        deleteStatefulSet();
        deleteService();

        if (channel != null) {
            for (ManagedChannel cn : channel) {
                cn.shutdown();
            }
        }
        channel = null;
        stub = null;
    }

    @Override
    public Throwable getDeathException() {
        return null;
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus(int instanceId) {
        CompletableFuture<FunctionStatus> retval = new CompletableFuture<>();
        if (instanceId < 0 || instanceId >= stub.length) {
            if (stub == null) {
                retval.completeExceptionally(new RuntimeException("Invalid InstanceId"));
                return retval;
            }
        }
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<FunctionStatus> response = stub[instanceId].withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).getFunctionStatus(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<FunctionStatus>() {
            @Override
            public void onFailure(Throwable throwable) {
                FunctionStatus.Builder builder = FunctionStatus.newBuilder();
                builder.setRunning(false);
                builder.setFailureException(throwable.getMessage());
                retval.complete(builder.build());
            }

            @Override
            public void onSuccess(FunctionStatus t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        retval.completeExceptionally(new RuntimeException("Kubernetes Runtime doesnt support getAndReset metrics via rest"));
        return retval;
    }

    @Override
    public CompletableFuture<Void> resetMetrics() {
        CompletableFuture<Void> retval = new CompletableFuture<>();
        retval.completeExceptionally(new RuntimeException("Kubernetes Runtime doesnt support resetting metrics via rest"));
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics(int instanceId) {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        if (instanceId < 0 || instanceId >= stub.length) {
            if (stub == null) {
                retval.completeExceptionally(new RuntimeException("Invalid InstanceId"));
                return retval;
            }
        }
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub[instanceId].withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).getMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                InstanceCommunication.MetricsData.Builder builder = InstanceCommunication.MetricsData.newBuilder();
                retval.complete(builder.build());
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    @Override
    public String getPrometheusMetrics() throws IOException {
        return RuntimeUtils.getPrometheusMetrics(METRICS_PORT);
    }

    @Override
    public boolean isAlive() {
        // No point for kubernetes just return dummy value
        return true;
    }

    private void submitService() throws Exception {
        final V1Service service = createService();
        log.info("Submitting the following service to k8 {}", coreClient.getApiClient().getJSON().serialize(service));

        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());

        RuntimeUtils.Actions.Action createService = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Submitting service for function %s", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    final V1Service response;
                    try {
                        response = coreClient.createNamespacedService(jobNamespace, service, null);
                    } catch (ApiException e) {
                        // already exists
                        if (e.getCode() == HTTP_CONFLICT) {
                            log.warn("Service already present for function {}", fqfn);
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }

                    return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                })
                .build();


        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(createService.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to create service for function %s", fqfn));
        }
    }

    private V1Service createService() {
        final String jobName = createJobName(instanceConfig.getFunctionDetails());

        final V1Service service = new V1Service();

        // setup stateful set metadata
        final V1ObjectMeta objectMeta = new V1ObjectMeta();
        objectMeta.name(jobName);
        service.metadata(objectMeta);

        // create the stateful set spec
        final V1ServiceSpec serviceSpec = new V1ServiceSpec();

        serviceSpec.clusterIP("None");

        final V1ServicePort servicePort = new V1ServicePort();
        servicePort.name("grpc").port(GRPC_PORT).protocol("TCP");
        serviceSpec.addPortsItem(servicePort);

        serviceSpec.selector(getLabels(instanceConfig.getFunctionDetails()));

        service.spec(serviceSpec);

        return service;
    }

    private void submitStatefulSet() throws Exception {
        final V1StatefulSet statefulSet = createStatefulSet();

        log.info("Submitting the following spec to k8 {}", appsClient.getApiClient().getJSON().serialize(statefulSet));

        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());

        RuntimeUtils.Actions.Action createStatefulSet = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Submitting statefulset for function %s", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    final V1StatefulSet response;
                    try {
                        response = appsClient.createNamespacedStatefulSet(jobNamespace, statefulSet, null);
                    } catch (ApiException e) {
                        // already exists
                        if (e.getCode() == HTTP_CONFLICT) {
                            log.warn("Statefulset already present for function {}", fqfn);
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }

                    return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                })
                .build();


        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(createStatefulSet.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to create statefulset for function %s", fqfn));
        }
    }


    public void deleteStatefulSet() throws InterruptedException {
        String statefulSetName = createJobName(instanceConfig.getFunctionDetails());
        final V1DeleteOptions options = new V1DeleteOptions();
        options.setGracePeriodSeconds(0L);
        options.setPropagationPolicy("Foreground");

        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());
        RuntimeUtils.Actions.Action deleteStatefulSet = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Deleting statefulset for function %s", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    Response response;
                    try {
                        // cannot use deleteNamespacedStatefulSet because of bug in kuberenetes
                        // https://github.com/kubernetes-client/java/issues/86
                        response = appsClient.deleteNamespacedStatefulSetCall(
                                statefulSetName,
                                jobNamespace, options, null,
                                null, null, null,
                                null, null)
                                .execute();
                    } catch (ApiException e) {
                        // if already deleted
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            log.warn("Statefulset for function {} does not exist", fqfn);
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    } catch (IOException e) {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(e.getMessage())
                                .build();
                    }

                    // if already deleted
                    if (response.code() == HTTP_NOT_FOUND) {
                        log.warn("Statefulset for function {} does not exist", fqfn);
                        return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                    } else {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(response.isSuccessful())
                                .errorMsg(response.message())
                                .build();
                    }
                })
                .build();


        RuntimeUtils.Actions.Action waitForStatefulSetDeletion = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Waiting for statefulset for function %s to complete deletion", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    V1StatefulSet response;
                    try {
                        response = appsClient.readNamespacedStatefulSet(jobNamespace, statefulSetName,
                                null, null, null);
                    } catch (ApiException e) {
                        // statefulset is gone
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }
                    return RuntimeUtils.Actions.ActionResult.builder()
                            .success(false)
                            .errorMsg(response.getStatus().toString())
                            .build();
                })
                .build();

        // Need to wait for all pods to die so we can cleanup subscriptions.
        RuntimeUtils.Actions.Action waitForStatefulPodsToTerminate = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Waiting for pods for function %s to terminate", fqfn))
                .numRetries(NUM_RETRIES * 2)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS * 2)
                .supplier(() -> {
                    String labels = String.format("tenant=%s,namespace=%s,name=%s",
                            instanceConfig.getFunctionDetails().getTenant(),
                            instanceConfig.getFunctionDetails().getNamespace(),
                            instanceConfig.getFunctionDetails().getName());

                    V1PodList response;
                    try {
                        response = coreClient.listNamespacedPod(jobNamespace, null, null,
                                null, null, labels,
                                null, null, null, null);
                    } catch (ApiException e) {

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }

                    if (response.getItems().size() > 0) {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(response.getItems().size() + " pods still alive.")
                                .build();
                    } else {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(true)
                                .build();
                    }
                })
                .build();


        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(deleteStatefulSet.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForStatefulSetDeletion.toBuilder()
                        .continueOn(false)
                        .onSuccess(() -> success.set(true))
                        .build())
                .addAction(deleteStatefulSet.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForStatefulSetDeletion.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to delete statefulset for function %s", fqfn));
        } else {
            // wait for pods to terminate
            RuntimeUtils.Actions.newBuilder()
                    .addAction(waitForStatefulPodsToTerminate)
                    .run();
        }
    }

    public void deleteService() throws InterruptedException {

        final V1DeleteOptions options = new V1DeleteOptions();
        options.setGracePeriodSeconds(0L);
        options.setPropagationPolicy("Foreground");
        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());
        String serviceName = createJobName(instanceConfig.getFunctionDetails());

        RuntimeUtils.Actions.Action deleteService = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Deleting service for function %s", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    final Response response;
                    try {
                        // cannot use deleteNamespacedService because of bug in kuberenetes
                        // https://github.com/kubernetes-client/java/issues/86
                        response = coreClient.deleteNamespacedServiceCall(
                                serviceName,
                                jobNamespace, options, null,
                                null, null,
                                null, null, null).execute();
                    } catch (ApiException e) {
                        // if already deleted
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            log.warn("Service for function {} does not exist", fqfn);
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    } catch (IOException e) {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(e.getMessage())
                                .build();
                    }

                    // if already deleted
                    if (response.code() == HTTP_NOT_FOUND) {
                        log.warn("Service for function {} does not exist", fqfn);
                        return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                    } else {
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(response.isSuccessful())
                                .errorMsg(response.message())
                                .build();
                    }
                })
                .build();

        RuntimeUtils.Actions.Action waitForServiceDeletion = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Waiting for statefulset for function %s to complete deletion", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    V1Service response;
                    try {
                        response = coreClient.readNamespacedService(serviceName, jobNamespace,
                                null, null, null);

                    } catch (ApiException e) {
                        // statefulset is gone
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }
                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }
                    return RuntimeUtils.Actions.ActionResult.builder()
                            .success(false)
                            .errorMsg(response.getStatus().toString())
                            .build();
                })
                .build();

        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(deleteService.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForServiceDeletion.toBuilder()
                        .continueOn(false)
                        .onSuccess(() -> success.set(true))
                        .build())
                .addAction(deleteService.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForServiceDeletion.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to delete service for function %s", fqfn));
        }
    }

    protected List<String> getExecutorCommand() {
        return Arrays.asList(
                "sh",
                "-c",
                String.join(" ", getDownloadCommand(userCodePkgUrl, originalCodeFileName))
                        + " && " + setShardIdEnvironmentVariableCommand()
                        + " && " + String.join(" ", processArgs)
        );
    }

    private List<String> getDownloadCommand(String bkPath, String userCodeFilePath) {
        return Arrays.asList(
                pulsarRootDir + "/bin/pulsar-admin",
                "--admin-url",
                pulsarAdminUrl,
                "functions",
                "download",
                "--path",
                bkPath,
                "--destination-file",
                userCodeFilePath);
    }

    private static String setShardIdEnvironmentVariableCommand() {
        return String.format("%s=${POD_NAME##*-} && echo shardId=${%s}", ENV_SHARD_ID, ENV_SHARD_ID);
    }

    private V1StatefulSet createStatefulSet() {
        final String jobName = createJobName(instanceConfig.getFunctionDetails());

        final V1StatefulSet statefulSet = new V1StatefulSet();

        // setup stateful set metadata
        final V1ObjectMeta objectMeta = new V1ObjectMeta();
        objectMeta.name(jobName);
        statefulSet.metadata(objectMeta);

        // create the stateful set spec
        final V1StatefulSetSpec statefulSetSpec = new V1StatefulSetSpec();
        statefulSetSpec.serviceName(jobName);
        statefulSetSpec.setReplicas(instanceConfig.getFunctionDetails().getParallelism());

        // Parallel pod management tells the StatefulSet controller to launch or terminate
        // all Pods in parallel, and not to wait for Pods to become Running and Ready or completely
        // terminated prior to launching or terminating another Pod.
        statefulSetSpec.setPodManagementPolicy("Parallel");

        // add selector match labels
        // so the we know which pods to manage
        final V1LabelSelector selector = new V1LabelSelector();
        selector.matchLabels(getLabels(instanceConfig.getFunctionDetails()));
        statefulSetSpec.selector(selector);

        // create a pod template
        final V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();

        // set up pod meta
        final V1ObjectMeta templateMetaData = new V1ObjectMeta().labels(getLabels(instanceConfig.getFunctionDetails()));
        templateMetaData.annotations(getPrometheusAnnotations());
        podTemplateSpec.setMetadata(templateMetaData);

        final List<String> command = getExecutorCommand();
        podTemplateSpec.spec(getPodSpec(command, instanceConfig.getFunctionDetails().hasResources() ? instanceConfig.getFunctionDetails().getResources() : null));

        statefulSetSpec.setTemplate(podTemplateSpec);

        statefulSet.spec(statefulSetSpec);

        return statefulSet;
    }

    private Map<String, String> getPrometheusAnnotations() {
        final Map<String, String> annotations = new HashMap<>();
        annotations.put("prometheus.io/scrape", "true");
        annotations.put("prometheus.io/port", String.valueOf(METRICS_PORT));
        return annotations;
    }

    private Map<String, String> getLabels(Function.FunctionDetails functionDetails) {
        final Map<String, String> labels = new HashMap<>();
        Utils.ComponentType componentType = InstanceUtils.calculateSubjectType(functionDetails);
        String component;
        switch (componentType) {
            case FUNCTION:
                component = "function";
                break;
            case SOURCE:
                component = "source";
                break;
            case SINK:
                component = "sink";
                break;
            default:
                component = "function";
                break;
        }
        labels.put("component", component);
        labels.put("namespace", functionDetails.getNamespace());
        labels.put("tenant", functionDetails.getTenant());
        labels.put("name", functionDetails.getName());
        if (customLabels != null && !customLabels.isEmpty()) {
            labels.putAll(customLabels);
        }
        return labels;
    }

    private V1PodSpec getPodSpec(List<String> instanceCommand, Function.Resources resource) {
        final V1PodSpec podSpec = new V1PodSpec();

        // set the termination period to 0 so pods can be deleted quickly
        podSpec.setTerminationGracePeriodSeconds(0L);

        // set the pod tolerations so pods are rescheduled when nodes go down
        // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
        podSpec.setTolerations(getTolerations());

        List<V1Container> containers = new LinkedList<>();
        containers.add(getFunctionContainer(instanceCommand, resource));
        podSpec.containers(containers);

        // Configure secrets
        secretsProviderConfigurator.configureKubernetesRuntimeSecretsProvider(podSpec, PULSARFUNCTIONS_CONTAINER_NAME, instanceConfig.getFunctionDetails());

        return podSpec;
    }

    private List<V1Toleration> getTolerations() {
        final List<V1Toleration> tolerations = new ArrayList<>();
        TOLERATIONS.forEach(t -> {
            final V1Toleration toleration =
                    new V1Toleration()
                            .key(t)
                            .operator("Exists")
                            .effect("NoExecute")
                            .tolerationSeconds(10L);
            tolerations.add(toleration);
        });

        return tolerations;
    }

    @VisibleForTesting
    V1Container getFunctionContainer(List<String> instanceCommand, Function.Resources resource) {
        final V1Container container = new V1Container().name(PULSARFUNCTIONS_CONTAINER_NAME);

        // set up the container images
        container.setImage(pulsarDockerImageName);
        container.setImagePullPolicy(imagePullPolicy);

        // set up the container command
        container.setCommand(instanceCommand);

        // setup the environment variables for the container
        final V1EnvVar envVarPodName = new V1EnvVar();
        envVarPodName.name("POD_NAME")
                .valueFrom(new V1EnvVarSource()
                        .fieldRef(new V1ObjectFieldSelector()
                                .fieldPath("metadata.name")));
        container.addEnvItem(envVarPodName);

        // set container resources
        final V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
        final Map<String, Quantity> requests = new HashMap<>();

        long ram = resource != null && resource.getRam() != 0 ? resource.getRam() : 1073741824;

        // add memory padding
        long padding = Math.round(ram * (percentMemoryPadding / 100.0));
        long ramWithPadding = ram + padding;

        requests.put("memory", Quantity.fromString(Long.toString(ramWithPadding)));
        requests.put("cpu", Quantity.fromString(Double.toString(resource != null && resource.getCpu() != 0 ? resource.getCpu() : 1)));
        resourceRequirements.setRequests(requests);
        resourceRequirements.setLimits(requests);
        container.setResources(resourceRequirements);

        // set container ports
        container.setPorts(getFunctionContainerPorts());

        return container;
    }

    private List<V1ContainerPort> getFunctionContainerPorts() {
        List<V1ContainerPort> ports = new ArrayList<>();
        final V1ContainerPort port = new V1ContainerPort();
        port.setName("grpc");
        port.setContainerPort(GRPC_PORT);
        ports.add(port);
        return ports;
    }

    private List<V1ContainerPort> getPrometheusContainerPorts() {
        List<V1ContainerPort> ports = new ArrayList<>();
        final V1ContainerPort port = new V1ContainerPort();
        port.setName("prometheus");
        port.setContainerPort(METRICS_PORT);
        ports.add(port);
        return ports;
    }

    private static String createJobName(Function.FunctionDetails functionDetails) {
        return createJobName(functionDetails.getTenant(),
                functionDetails.getNamespace(),
                functionDetails.getName());
    }

    private static String createJobName(String tenant, String namespace, String functionName) {
        return "pf-" + tenant + "-" + namespace + "-" + functionName;
    }

    public static void doChecks(Function.FunctionDetails functionDetails) {
        final String jobName = createJobName(functionDetails);
        if (!jobName.equals(jobName.toLowerCase())) {
            throw new RuntimeException("Kubernetes does not allow upper case jobNames.");
        }
        final Matcher matcher = VALID_POD_NAME_REGEX.matcher(jobName);
        if (!matcher.matches()) {
            throw new RuntimeException("Kubernetes only admits lower case and numbers.");
        }
        if (jobName.length() > maxJobNameSize) {
            throw new RuntimeException("Kubernetes job name size should be less than " + maxJobNameSize);
        }
    }
}
