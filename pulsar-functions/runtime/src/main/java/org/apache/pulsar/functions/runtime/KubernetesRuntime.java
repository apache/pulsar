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
import com.google.protobuf.util.JsonFormat;
import com.squareup.okhttp.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.metrics.PrometheusMetricsServer;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_CONFLICT;

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
class KubernetesRuntime implements Runtime {

    private static final String ENV_SHARD_ID = "SHARD_ID";
    private static final int maxJobNameSize = 55;
    private static final Integer GRPC_PORT = 9093;
    private static final Integer PROMETHEUS_PORT = 9094;
    private static final Double prometheusMetricsServerCpu = 0.1;
    private static final Long prometheusMetricsServerRam = 250000000l;
    public static final Pattern VALID_POD_NAME_REGEX =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
                    Pattern.CASE_INSENSITIVE);

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
    private List<String> prometheusMetricsServerArgs;
    @Getter
    private ManagedChannel[] channel;
    private InstanceControlGrpc.InstanceControlFutureStub[] stub;
    private InstanceConfig instanceConfig;
    private final String jobNamespace;
    private final Map<String, String> customLabels;
    private final String pulsarDockerImageName;
    private final String pulsarRootDir;
    private final String userCodePkgUrl;
    private final String originalCodeFileName;
    private final String pulsarAdminUrl;
    private boolean running;


    KubernetesRuntime(AppsV1Api appsClient,
                      CoreV1Api coreClient,
                      String jobNamespace,
                      Map<String, String> customLabels,
                      Boolean installUserCodeDependencies,
                      String pulsarDockerImageName,
                      String pulsarRootDir,
                      InstanceConfig instanceConfig,
                      String instanceFile,
                      String prometheusMetricsServerJarFile,
                      String logDirectory,
                      String userCodePkgUrl,
                      String originalCodeFileName,
                      String pulsarServiceUrl,
                      String pulsarAdminUrl,
                      String stateStorageServiceUrl,
                      AuthenticationConfig authConfig,
                      Integer expectedMetricsInterval) throws Exception {
        this.appsClient = appsClient;
        this.coreClient = coreClient;
        this.instanceConfig = instanceConfig;
        this.jobNamespace = jobNamespace;
        this.customLabels = customLabels;
        this.pulsarDockerImageName = pulsarDockerImageName;
        this.pulsarRootDir = pulsarRootDir;
        this.userCodePkgUrl = userCodePkgUrl;
        this.originalCodeFileName = pulsarRootDir + "/" + originalCodeFileName;
        this.pulsarAdminUrl = pulsarAdminUrl;
        this.processArgs = RuntimeUtils.composeArgs(instanceConfig, instanceFile, logDirectory, this.originalCodeFileName, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig, "$" + ENV_SHARD_ID, GRPC_PORT, -1l, pulsarRootDir + "/conf/log4j2.yaml", installUserCodeDependencies);
        this.prometheusMetricsServerArgs = composePrometheusMetricsServerArgs(prometheusMetricsServerJarFile, expectedMetricsInterval);
        running = false;
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
            deleteService();
        }
        running = true;
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
        if (running) {
            deleteStatefulSet();
            deleteService();
        }
        if (channel != null) {
            for (ManagedChannel cn : channel) {
                cn.shutdown();
            }
        }
        channel = null;
        stub = null;
        running = false;
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
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        retval.completeExceptionally(new RuntimeException("Kubernetes Runtime doesnt support getting metrics via rest"));
        return retval;
    }

    @Override
    public boolean isAlive() {
        return running;
    }

    private void submitService() throws Exception {
        final V1Service service = createService();
        log.info("Submitting the following service to k8 {}", coreClient.getApiClient().getJSON().serialize(service));

        final Response response =
                coreClient.createNamespacedServiceCall(jobNamespace, service, null,
                        null, null).execute();
        if (!response.isSuccessful()) {
            if (response.code() == HTTP_CONFLICT) {
                log.warn("Service already created for function {}/{}/{}",
                        instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace(),
                        instanceConfig.getFunctionDetails().getName());
            } else {
                log.error("Error creating Service for function {}/{}/{}:- {}",
                        instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace(),
                        instanceConfig.getFunctionDetails().getName(),
                        response.message());
                // construct a message based on the k8s api server response
                throw new IllegalStateException(response.message());
            }
        } else {
            log.info("Service Created Successfully for function {}/{}/{}",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName());
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

        final Response response =
                appsClient.createNamespacedStatefulSetCall(jobNamespace, statefulSet, null,
                        null, null).execute();
        if (!response.isSuccessful()) {
            if (response.code() == HTTP_CONFLICT) {
                log.warn("Statefulset already present for function {}/{}/{}",
                        instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace(),
                        instanceConfig.getFunctionDetails().getName());
            } else {
                log.error("Error creating statefulset for function {}/{}/{}:- {}",
                        instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace(),
                        instanceConfig.getFunctionDetails().getName(),
                        response.message());
                // construct a message based on the k8s api server response
                throw new IllegalStateException(response.message());
            }
        } else {
            log.info("Successfully created statefulset for function {}/{}/{}",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName());
        }
    }

    public void deleteStatefulSet() throws Exception {
        final V1DeleteOptions options = new V1DeleteOptions();
        options.setGracePeriodSeconds(0L);
        options.setPropagationPolicy("Foreground");
        final Response response = appsClient.deleteNamespacedStatefulSetCall(
                createJobName(instanceConfig.getFunctionDetails()),
                jobNamespace, options, null, null, null, null, null, null)
                .execute();

        if (!response.isSuccessful()) {
            throw new RuntimeException(String.format("Error deleting statefulset for function {}/{}/{} :- {} ",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(),
                    response.message()));
        } else {
            log.info("Successfully deleted statefulset for function {}/{}/{}",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName());
        }
    }

    public void deleteService() throws Exception {
        final V1DeleteOptions options = new V1DeleteOptions();
        options.setGracePeriodSeconds(0L);
        options.setPropagationPolicy("Foreground");
        final Response response = coreClient.deleteNamespacedServiceCall(
                createJobName(instanceConfig.getFunctionDetails()),
                jobNamespace, options, null, null, null, null, null, null)
                .execute();

        if (!response.isSuccessful()) {
            throw new RuntimeException(String.format("Error deleting service for function {}/{}/{} :- {}",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(),
                    response.message()));
        } else {
            log.info("Service deleted successfully for function {}/{}/{}",
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName());
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

    protected List<String> getPrometheusMetricsServerCommand() {
        return Arrays.asList(
                "sh",
                "-c",
                String.join(" ", prometheusMetricsServerArgs)
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
        annotations.put("prometheus.io/port", String.valueOf(PROMETHEUS_PORT));
        return annotations;
    }

    private Map<String, String> getLabels(Function.FunctionDetails functionDetails) {
        final Map<String, String> labels = new HashMap<>();
        labels.put("app", createJobName(functionDetails));
        labels.put("namespace", functionDetails.getNamespace());
        labels.put("tenant", functionDetails.getTenant());
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
        containers.add(getPrometheusContainer());
        podSpec.containers(containers);

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

    private V1Container getFunctionContainer(List<String> instanceCommand, Function.Resources resource) {
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
        container.setPorts(getFunctionContainerPorts());

        return container;
    }

    private V1Container getPrometheusContainer() {
        final V1Container container = new V1Container().name("prometheusmetricsserver");

        // set up the container images
        container.setImage(pulsarDockerImageName);

        // set up the container command
        container.setCommand(getPrometheusMetricsServerCommand());

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
        requests.put("memory", Quantity.fromString(Long.toString(prometheusMetricsServerRam)));
        requests.put("cpu", Quantity.fromString(Double.toString(prometheusMetricsServerCpu)));
        resourceRequirements.setRequests(requests);
        container.setResources(resourceRequirements);

        // set container ports
        container.setPorts(getPrometheusContainerPorts());

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
        port.setContainerPort(PROMETHEUS_PORT);
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

    private List<String> composePrometheusMetricsServerArgs(String prometheusMetricsServerFile,
                                                            Integer expectedMetricsInterval) throws Exception {
        List<String> args = new LinkedList<>();
        args.add("java");
        args.add("-cp");
        args.add(prometheusMetricsServerFile);
        args.add("-Xmx" + String.valueOf(prometheusMetricsServerRam));
        args.add(PrometheusMetricsServer.class.getName());
        args.add("--function_details");
        args.add("'" + JsonFormat.printer().omittingInsignificantWhitespace().print(instanceConfig.getFunctionDetails()) + "'");
        args.add("--prometheus_port");
        args.add(String.valueOf(PROMETHEUS_PORT));
        args.add("--grpc_port");
        args.add(String.valueOf(GRPC_PORT));
        args.add("--collection_interval");
        args.add(String.valueOf(expectedMetricsInterval));
        return args;
    }
}
