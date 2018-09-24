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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.squareup.okhttp.Response;
import io.grpc.ManagedChannel;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_CONFLICT;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class KubernetesRuntime implements Runtime {

    private static final String ENV_SHARD_ID = "SHARD_ID";
    private static final Integer GRPC_PORT = 9093;
    public static final Pattern VALID_POD_NAME_REGEX =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
                    Pattern.CASE_INSENSITIVE);

    private final AppsV1Api client;
    static final List<String> TOLERATIONS = Collections.unmodifiableList(
            Arrays.asList(
                    "node.kubernetes.io/not-ready",
                    "node.alpha.kubernetes.io/notReady",
                    "node.alpha.kubernetes.io/unreachable"
            )
    );

    // The thread that invokes the function
    @Getter
    private List<String> processArgs;
    @Getter
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;
    private InstanceConfig instanceConfig;
    private final String jobNamespace;
    private final String pulsarDockerImageName;
    private final String pulsarRootDir;
    private final String userCodePkgUrl;
    private final String originalCodeFileName;
    private final String pulsarAdminUrl;
    private boolean running;


    KubernetesRuntime(AppsV1Api client,
                      String jobNamespace,
                      String pulsarDockerImageName,
                      String pulsarRootDir,
                      InstanceConfig instanceConfig,
                      String instanceFile,
                      String logDirectory,
                      String userCodePkgUrl,
                      String originalCodeFileName,
                      String pulsarServiceUrl,
                      String pulsarAdminUrl,
                      String stateStorageServiceUrl,
                      AuthenticationConfig authConfig) throws Exception {
        this.client = client;
        this.instanceConfig = instanceConfig;
        this.jobNamespace = jobNamespace;
        this.pulsarDockerImageName = pulsarDockerImageName;
        this.pulsarRootDir = pulsarRootDir;
        this.userCodePkgUrl = userCodePkgUrl;
        this.originalCodeFileName = originalCodeFileName;
        this.pulsarAdminUrl = pulsarAdminUrl;
        this.processArgs = RuntimeUtils.composeArgs(instanceConfig, instanceFile, logDirectory, originalCodeFileName, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig, "$" + ENV_SHARD_ID, GRPC_PORT, -1l, "conf/log4j2.yaml");
        running = false;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() {
        createKubernetesJob();
        // TODO:- Make someting here
        /*
        if (channel == null && stub == null) {
            // channel = ManagedChannelBuilder.forAddress("127.0.0.1", instancePort)
                    .usePlaintext(true)
                    .build();
            stub = InstanceControlGrpc.newFutureStub(channel);
        }
        */
    }

    @Override
    public void join() throws Exception {
        // K8 functions never return
        this.wait();
    }

    @Override
    public void stop() {
        deleteKubernetesJob();
        if (channel != null) {
            channel.shutdown();
        }
        channel = null;
        stub = null;
    }

    @Override
    public Throwable getDeathException() {
        return null;
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        CompletableFuture<FunctionStatus> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<FunctionStatus> response = stub.getFunctionStatus(Empty.newBuilder().build());
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
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.getAndResetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    @Override
    public CompletableFuture<Void> resetMetrics() {
        CompletableFuture<Void> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<Empty> response = stub.resetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<Empty>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(Empty t) {
                retval.complete(null);
            }
        });
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.getMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    @Override
    public boolean isAlive() {
        return running;
    }

    private void createKubernetesJob() {
        final String jobName = createJobName(instanceConfig.getFunctionDetails());
        if (!jobName.equals(jobName.toLowerCase())) {
            throw new RuntimeException("K8S scheduler does not allow upper case jobNames.");
        }
        final Matcher matcher = VALID_POD_NAME_REGEX.matcher(jobName);
        if (!matcher.matches()) {
            throw new RuntimeException("K8S scheduler only admits lower case and numbers.");
        }

        final V1StatefulSet statefulSet = createStatefulSet();

        log.info("Submitting the following spec to k8 " + client.getApiClient().getJSON().serialize(statefulSet));

        try {
            final Response response =
                    client.createNamespacedStatefulSetCall(jobNamespace, statefulSet, null,
                            null, null).execute();
            if (!response.isSuccessful()) {
                if (response.code() == HTTP_CONFLICT) {
                    log.warn("Kubernetes job already running");
                    running = true;
                } else {
                    log.error("Error creating k8 job:- : " + response.message());
                    // construct a message based on the k8s api server response
                    throw new RuntimeException(response.message());
                }
            } else {
                log.info("Job Submitted Successfully");
                running = true;
            }
        } catch (IOException | ApiException e) {
            log.error("Error creating k8 job", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public void deleteKubernetesJob() {
        if (!running) return;
        try {
            final V1DeleteOptions options = new V1DeleteOptions();
            options.setGracePeriodSeconds(0L);
            options.setPropagationPolicy("Foreground");
            final Response response = client.deleteNamespacedStatefulSetCall(
                    createJobName(instanceConfig.getFunctionDetails()),
                    jobNamespace, options, null, null, null, null, null, null)
                    .execute();

            if (!response.isSuccessful()) {
                throw new RuntimeException("Error killing k8 job " + response.message());
            } else {
                log.info("Killed Successfully");
                running = false;
            }
        } catch (IOException | ApiException e) {
            throw new RuntimeException(e);
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
        /*
        TODO:- Figure out the metrics collection later.
        templateMetaData.annotations(getPrometheusAnnotations());
        */
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
        annotations.put("prometheus.io/port", "8080");
        return annotations;
    }

    private Map<String, String> getLabels(Function.FunctionDetails functionDetails) {
        final Map<String, String> labels = new HashMap<>();
        labels.put("app", "pulsarfunction");
        labels.put("name", functionDetails.getName());
        labels.put("namespace", functionDetails.getNamespace());
        labels.put("tenant", functionDetails.getTenant());
        return labels;
    }

    private V1PodSpec getPodSpec(List<String> instanceCommand, Function.Resources resource) {
        final V1PodSpec podSpec = new V1PodSpec();

        // set the termination period to 0 so pods can be deleted quickly
        podSpec.setTerminationGracePeriodSeconds(0L);

        // set the pod tolerations so pods are rescheduled when nodes go down
        // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
        podSpec.setTolerations(getTolerations());

        podSpec.containers(Collections.singletonList(
                getContainer(instanceCommand, resource)));

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

    private V1Container getContainer(List<String> instanceCommand, Function.Resources resource) {
        final V1Container container = new V1Container().name("pulsarfunction");

        // set up the container images
        container.setImage(pulsarDockerImageName);

        // set up the container command
        container.setCommand(instanceCommand);

        // setup the environment variables for the container
        final V1EnvVar envVarPodName = new V1EnvVar();
        envVarPodName.name("POD_NAME")
                .valueFrom(new V1EnvVarSource()
                        .fieldRef(new V1ObjectFieldSelector()
                                .fieldPath("metadata.name")));
        container.setEnv(Arrays.asList(envVarPodName));


        // set container resources
        final V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
        final Map<String, Quantity> requests = new HashMap<>();
        requests.put("memory", Quantity.fromString(Long.toString(resource != null && resource.getRam() != 0 ? resource.getRam() : 1073741824)));
        requests.put("cpu", Quantity.fromString(Double.toString(resource != null && resource.getCpu() != 0 ? resource.getCpu() : 1)));
        resourceRequirements.setRequests(requests);
        container.setResources(resourceRequirements);

        // set container ports
        container.setPorts(getContainerPorts());

        return container;
    }

    private List<V1ContainerPort> getContainerPorts() {
        List<V1ContainerPort> ports = new ArrayList<>();
        final V1ContainerPort port = new V1ContainerPort();
        port.setName("grpc");
        port.setContainerPort(GRPC_PORT);
        ports.add(port);
        return ports;
    }

    private String createJobName(Function.FunctionDetails functionDetails) {
        return createJobName(functionDetails.getTenant(),
                functionDetails.getNamespace(),
                functionDetails.getName());
    }

    private String createJobName(String tenant, String namespace, String functionName) {
        return "pfn-" + tenant + "-" + namespace + "-" + functionName;
    }
}
