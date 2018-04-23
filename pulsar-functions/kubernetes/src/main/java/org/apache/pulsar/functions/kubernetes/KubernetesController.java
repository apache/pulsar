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
package org.apache.pulsar.functions.kubernetes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


import com.squareup.okhttp.Response;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1beta1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Toleration;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.models.V1beta1StatefulSet;
import io.kubernetes.client.models.V1beta1StatefulSetSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.ProcessRuntime;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;

@Slf4j
public class KubernetesController {

    private static final String ENV_SHARD_ID = "SHARD_ID";

    private final AppsV1beta1Api client;
    private KubernetesConfig kubernetesConfig;
    static final List<String> TOLERATIONS = Collections.unmodifiableList(
            Arrays.asList(
                    "node.kubernetes.io/not-ready",
                    "node.alpha.kubernetes.io/notReady",
                    "node.alpha.kubernetes.io/unreachable"
            )
    );
    static final int instancePort = 7000;
    static final String logDirectory = "/logs";
    static final String javaInstanceFile = "/javaInstance.jar";
    static final String pythonInstanceFile = "/python_instance_main.py";

    public KubernetesController(String yamlFile) throws IOException {
        kubernetesConfig = KubernetesConfig.load(yamlFile);
        if (!kubernetesConfig.areAllFieldsPresent()) {
            throw new RuntimeException("Missing arguments");
        }
        final ApiClient apiClient = new ApiClient().setBasePath(kubernetesConfig.getK8Uri());
        client = new AppsV1beta1Api(apiClient);
    }

    public void create(InstanceConfig instanceConfig, String userCodeFile, String pulsarServiceUrl, Resource resource) {
        instanceConfig.setInstanceId("$" + ENV_SHARD_ID);
        instanceConfig.setPort(instancePort);
        String instanceCodeFile;
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            instanceCodeFile = javaInstanceFile;
        } else {
            instanceCodeFile = pythonInstanceFile;
        }
        final String fqn = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());
        if (!fqn.equals(fqn.toLowerCase())) {
            throw new RuntimeException("K8S scheduler does not allow upper case fqns.");
        }

        final V1beta1StatefulSet statefulSet = createStatefulSet(instanceConfig, instanceCodeFile,
                userCodeFile, pulsarServiceUrl, resource);

        try {
            final Response response =
                    client.createNamespacedStatefulSetCall(kubernetesConfig.getJobNamespace(), statefulSet, null,
                            null, null).execute();
            if (!response.isSuccessful()) {
                log.error("Error creating topology message: " + response.message());
                // construct a message based on the k8s api server response
                throw new RuntimeException(response.message());
            }
        } catch (IOException | ApiException e) {
            log.error("Error creating topology", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public void delete(String tenant, String namespace, String name) {
        try {
            final V1DeleteOptions options = new V1DeleteOptions();
            options.setGracePeriodSeconds(0L);
            options.setPropagationPolicy("Foreground");
            final Response response = client.deleteNamespacedStatefulSetCall(
                    FunctionDetailsUtils.getFullyQualifiedName(tenant, namespace, name),
                    kubernetesConfig.getJobNamespace(), options, null, null, null, null, null, null)
                    .execute();

            if (!response.isSuccessful()) {
                throw new RuntimeException("Error killing topology " + response.message());
            }
        } catch (IOException | ApiException e) {
            throw new RuntimeException(e);
        }

    }

    protected List<String> getExecutorCommand(InstanceConfig instanceConfig,
                                              String instanceCodeFile,
                                              String userCodeFile,
                                              String pulsarServiceUrl) {
        final List<String> executorCommand =
                ProcessRuntime.composeArgs(instanceConfig, instanceCodeFile, logDirectory,
                        userCodeFile, pulsarServiceUrl);
        return Arrays.asList(
                "sh",
                "-c",
                setShardIdEnvironmentVariableCommand()
                        + " && " + String.join(" ", executorCommand)
        );
    }

    private static String setShardIdEnvironmentVariableCommand() {
        return String.format("%s=${POD_NAME##*-} && echo shardId=${%s}", ENV_SHARD_ID, ENV_SHARD_ID);
    }


    private V1beta1StatefulSet createStatefulSet(InstanceConfig instanceConfig,
                                                 String instanceCodeFile,
                                                 String userCodeFile,
                                                 String pulsarServiceUrl,
                                                 Resource resource) {
        final String jobName = FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails());

        final V1beta1StatefulSet statefulSet = new V1beta1StatefulSet();

        // setup stateful set metadata
        final V1ObjectMeta objectMeta = new V1ObjectMeta();
        objectMeta.name(jobName);
        statefulSet.metadata(objectMeta);

        // create the stateful set spec
        final V1beta1StatefulSetSpec statefulSetSpec = new V1beta1StatefulSetSpec();
        statefulSetSpec.serviceName(jobName);
        statefulSetSpec.setReplicas(instanceConfig.getFunctionDetails().getParallelism());

        // Parallel pod management tells the StatefulSet controller to launch or terminate
        // all Pods in parallel, and not to wait for Pods to become Running and Ready or completely
        // terminated prior to launching or terminating another Pod.
        statefulSetSpec.setPodManagementPolicy("Parallel");

        // add selector match labels "app=heron" and "topology=topology-name"
        // so the we know which pods to manage
        final V1LabelSelector selector = new V1LabelSelector();
        selector.matchLabels(getLabels(jobName));
        statefulSetSpec.selector(selector);

        // create a pod template
        final V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();

        // set up pod meta
        final V1ObjectMeta templateMetaData = new V1ObjectMeta().labels(getLabels(jobName));
        templateMetaData.annotations(getPrometheusAnnotations());
        podTemplateSpec.setMetadata(templateMetaData);

        final List<String> command = getExecutorCommand(instanceConfig, instanceCodeFile, userCodeFile, pulsarServiceUrl);
        podTemplateSpec.spec(getPodSpec(command, resource));

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

    private Map<String, String> getLabels(String jobName) {
        final Map<String, String> labels = new HashMap<>();
        labels.put("app", "pulsarfunction");
        labels.put("job", jobName);
        return labels;
    }

    private V1PodSpec getPodSpec(List<String> executorCommand, Resource resource) {
        final V1PodSpec podSpec = new V1PodSpec();

        // set the termination period to 0 so pods can be deleted quickly
        podSpec.setTerminationGracePeriodSeconds(0L);

        // set the pod tolerations so pods are rescheduled when nodes go down
        // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
        podSpec.setTolerations(getTolerations());

        podSpec.containers(Collections.singletonList(
                getContainer(executorCommand, resource)));

        // addVolumesIfPresent(podSpec);

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

    /*
    private void addVolumesIfPresent(V1PodSpec spec) {
        final Config config = getConfiguration();
        if (KubernetesContext.hasVolume(config)) {
            final V1Volume volume = Volumes.get().create(config);
            if (volume != null) {
                LOG.fine("Adding volume: " + volume.toString());
                spec.volumes(Collections.singletonList(volume));
            }
        }
    }
    */

    private V1Container getContainer(List<String> executorCommand, Resource resource) {
        final V1Container container = new V1Container().name("executor");

        // set up the container images
        container.setImage(kubernetesConfig.getPulsarDockerImageName());

        // set up the container command
        container.setCommand(executorCommand);

        /*
        // setup the environment variables for the container
        final V1EnvVar envVarHost = new V1EnvVar();
        envVarHost.name(KubernetesConstants.ENV_HOST)
                .valueFrom(new V1EnvVarSource()
                        .fieldRef(new V1ObjectFieldSelector()
                                .fieldPath(KubernetesConstants.POD_IP)));

        final V1EnvVar envVarPodName = new V1EnvVar();
        envVarPodName.name(KubernetesConstants.ENV_POD_NAME)
                .valueFrom(new V1EnvVarSource()
                        .fieldRef(new V1ObjectFieldSelector()
                                .fieldPath(KubernetesConstants.POD_NAME)));
        container.setEnv(Arrays.asList(envVarHost, envVarPodName));
        */


        // set container resources
        final V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
        final Map<String, Quantity> requests = new HashMap<>();
        requests.put("memory", Quantity.fromString(resource.getRam()));
        requests.put("cpu", Quantity.fromString(resource.getCpu()));
        resourceRequirements.setRequests(requests);
        container.setResources(resourceRequirements);

        // set container ports
        container.setPorts(getContainerPorts());

        // setup volume mounts
        // mountVolumeIfPresent(container);

        return container;
    }

    private List<V1ContainerPort> getContainerPorts() {
        List<V1ContainerPort> ports = new ArrayList<>();
        final V1ContainerPort port = new V1ContainerPort();
        port.setName("grpc");
        port.setContainerPort(instancePort);
        ports.add(port);

        return ports;
    }

    /*
    private void mountVolumeIfPresent(V1Container container) {
        final Config config = getConfiguration();
        if (KubernetesContext.hasContainerVolume(config)) {
            final V1VolumeMount mount =
                    new V1VolumeMount()
                            .name(KubernetesContext.getContainerVolumeName(config))
                            .mountPath(KubernetesContext.getContainerVolumeMountPath(config));
            container.volumeMounts(Collections.singletonList(mount));
        }
    }
    */
}