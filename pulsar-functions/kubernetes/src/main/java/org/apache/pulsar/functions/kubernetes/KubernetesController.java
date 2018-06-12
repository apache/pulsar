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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.google.gson.Gson;
import com.squareup.okhttp.Response;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1beta2Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.*;
import io.kubernetes.client.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Resources;

@Slf4j
public class KubernetesController {

    private static final String ENV_SHARD_ID = "SHARD_ID";
    public static final Pattern VALID_POD_NAME_REGEX =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
                    Pattern.CASE_INSENSITIVE);

    private final AppsV1beta2Api client;
    private KubernetesConfig kubernetesConfig;
    static final List<String> TOLERATIONS = Collections.unmodifiableList(
            Arrays.asList(
                    "node.kubernetes.io/not-ready",
                    "node.alpha.kubernetes.io/notReady",
                    "node.alpha.kubernetes.io/unreachable"
            )
    );

    public KubernetesController(String yamlFile) throws IOException {
        if (yamlFile != null) {
            kubernetesConfig = KubernetesConfig.load(yamlFile);
        } else {
            kubernetesConfig = new KubernetesConfig();
        }
        if (!kubernetesConfig.areAllFieldsPresent()) {
            throw new RuntimeException("Missing arguments");
        }
        if (kubernetesConfig.getK8Uri() == null) {
            ApiClient cli = Config.defaultClient();
            Configuration.setDefaultApiClient(cli);
            client = new AppsV1beta2Api();
        } else {
            final ApiClient apiClient = new ApiClient().setBasePath(kubernetesConfig.getK8Uri());
            client = new AppsV1beta2Api(apiClient);
        }
    }

    public void create(FunctionConfig functionConfig, int parallelism, String bkPath, String fileBaseName) {
        final String jobName = createJobName(functionConfig);
        if (!jobName.equals(jobName.toLowerCase())) {
            throw new RuntimeException("K8S scheduler does not allow upper case jobNames.");
        }
        final Matcher matcher = VALID_POD_NAME_REGEX.matcher(jobName);
        if (!matcher.matches()) {
            throw new RuntimeException("K8S scheduler only admits lower case and numbers.");
        }

        final V1beta2StatefulSet statefulSet = createStatefulSet(functionConfig, parallelism, bkPath, fileBaseName);

        log.info("Submitting the following spec to k8 " + client.getApiClient().getJSON().serialize(statefulSet));

        try {
            final Response response =
                    client.createNamespacedStatefulSetCall(kubernetesConfig.getJobNamespace(), statefulSet, null,
                            null, null).execute();
            if (!response.isSuccessful()) {
                log.error("Error creating k8 job:- : " + response.message());
                // construct a message based on the k8s api server response
                throw new RuntimeException(response.message());
            } else {
                System.out.println("Job Submitted Successfully");
            }
        } catch (IOException | ApiException e) {
            log.error("Error creating k8 job", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public void delete(String tenant, String namespace, String name) {
        try {
            final V1DeleteOptions options = new V1DeleteOptions();
            options.setGracePeriodSeconds(0L);
            options.setPropagationPolicy("Foreground");
            final Response response = client.deleteNamespacedStatefulSetCall(
                    createJobName(tenant, namespace, name),
                    kubernetesConfig.getJobNamespace(), options, null, null, null, null, null, null)
                    .execute();

            if (!response.isSuccessful()) {
                throw new RuntimeException("Error killing k8 job " + response.message());
            } else {
                System.out.println("Killed Successfully");
            }
        } catch (IOException | ApiException e) {
            throw new RuntimeException(e);
        }

    }

    protected List<String> getExecutorCommand(FunctionConfig functionConfig,
                                              String bkPath,
                                              String userCodeFilePath) {
        adjustFunctionConfig(functionConfig);
        String functionConfigFileName = "functionConfig.yml";
        return Arrays.asList(
                "sh",
                "-c",
                String.join(" ", getDownloadCommand(bkPath, userCodeFilePath))
                + " && " + setShardIdEnvironmentVariableCommand()
                + " && " + String.join(" ", getWriteFunctionConfigToFileCommand(functionConfig, functionConfigFileName))
                + " && " + String.join(" ", getLocalRunCommand(functionConfig, userCodeFilePath, functionConfigFileName))
        );
    }

    private void adjustFunctionConfig(FunctionConfig functionConfig) {
        functionConfig.setParallelism(1);
        functionConfig.setJar(null);
        functionConfig.setPy(null);
    }

    private List<String> getWriteFunctionConfigToFileCommand(FunctionConfig functionConfig,
                                                             String functionConfigFileName) {
        List<String> retval = new LinkedList<>();
        retval.add("echo");
        String functionConfigString = "'" + new Gson().toJson(functionConfig) + "'";
        retval.add(functionConfigString);
        retval.add(">>");
        retval.add(functionConfigFileName);
        return retval;
    }

    private List<String> getDownloadCommand(String bkPath, String userCodeFilePath) {
        return Arrays.asList(
                kubernetesConfig.getPulsarRootDir() + "/bin/pulsar-admin",
                "--admin-url",
                kubernetesConfig.getPulsarAdminUri(),
                "functions",
                "download",
                "--path",
                bkPath,
                "--destinationFile",
                userCodeFilePath);
    }

    private List<String> getLocalRunCommand(FunctionConfig functionConfig,
                                            String userCodeFilePath, String functionConfigFileName) {
        List<String> retval = new LinkedList<>();
        retval.add(kubernetesConfig.getPulsarRootDir() + "/bin/pulsar-admin");
        retval.add("functions");
        retval.add("localrun");
        retval.add("--brokerServiceUrl");
        retval.add(kubernetesConfig.getPulsarServiceUri());
        switch (functionConfig.getRuntime()) {
            case JAVA:
                retval.add("--jar");
                break;
            case PYTHON:
                retval.add("--py");
        }
        retval.add(userCodeFilePath);
        // The parallelism is always one since each container runs only one instance
        retval.add("--parallelism");
        retval.add("1");
        retval.add("--instanceIdOffset");
        retval.add("$" + ENV_SHARD_ID);
        retval.add("--functionConfigFile");
        retval.add(functionConfigFileName);
        return retval;
    }

    private static String setShardIdEnvironmentVariableCommand() {
        return String.format("%s=${POD_NAME##*-} && echo shardId=${%s}", ENV_SHARD_ID, ENV_SHARD_ID);
    }


    private V1beta2StatefulSet createStatefulSet(FunctionConfig functionConfig,
                                                 int parallelism,
                                                 String bkPath,
                                                 String fileBaseName) {
        final String jobName = createJobName(functionConfig);

        final V1beta2StatefulSet statefulSet = new V1beta2StatefulSet();

        // setup stateful set metadata
        final V1ObjectMeta objectMeta = new V1ObjectMeta();
        objectMeta.name(jobName);
        statefulSet.metadata(objectMeta);

        // create the stateful set spec
        final V1beta2StatefulSetSpec statefulSetSpec = new V1beta2StatefulSetSpec();
        statefulSetSpec.serviceName(jobName);
        statefulSetSpec.setReplicas(parallelism);

        // Parallel pod management tells the StatefulSet controller to launch or terminate
        // all Pods in parallel, and not to wait for Pods to become Running and Ready or completely
        // terminated prior to launching or terminating another Pod.
        statefulSetSpec.setPodManagementPolicy("Parallel");

        // add selector match labels
        // so the we know which pods to manage
        final V1LabelSelector selector = new V1LabelSelector();
        selector.matchLabels(getLabels(functionConfig));
        statefulSetSpec.selector(selector);

        // create a pod template
        final V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();

        // set up pod meta
        final V1ObjectMeta templateMetaData = new V1ObjectMeta().labels(getLabels(functionConfig));
        /*
        TODO:- Figure out the metrics collection later.
        templateMetaData.annotations(getPrometheusAnnotations());
        */
        podTemplateSpec.setMetadata(templateMetaData);

        final List<String> command = getExecutorCommand(functionConfig, bkPath, fileBaseName);
        podTemplateSpec.spec(getPodSpec(command, functionConfig.getResources()));

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

    private Map<String, String> getLabels(FunctionConfig functionConfig) {
        final Map<String, String> labels = new HashMap<>();
        labels.put("app", "pulsarfunction");
        labels.put("name", functionConfig.getName());
        labels.put("namespace", functionConfig.getNamespace());
        labels.put("tenant", functionConfig.getTenant());
        return labels;
    }

    private V1PodSpec getPodSpec(List<String> executorCommand, Resources resource) {
        final V1PodSpec podSpec = new V1PodSpec();

        // set the termination period to 0 so pods can be deleted quickly
        podSpec.setTerminationGracePeriodSeconds(0L);

        // set the pod tolerations so pods are rescheduled when nodes go down
        // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
        podSpec.setTolerations(getTolerations());

        podSpec.containers(Collections.singletonList(
                getContainer(executorCommand, resource)));

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

    private V1Container getContainer(List<String> executorCommand, Resources resource) {
        final V1Container container = new V1Container().name("executor");

        // set up the container images
        container.setImage(kubernetesConfig.getPulsarDockerImageName());

        // set up the container command
        container.setCommand(executorCommand);

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
        requests.put("memory", Quantity.fromString(Long.toString(resource != null && resource.getRam() != null && resource.getRam() != 0 ? resource.getRam() : 1073741824)));
        requests.put("cpu", Quantity.fromString(Double.toString(resource != null && resource.getCpu() != null && resource.getCpu() != 0 ? resource.getCpu() : 1)));
        resourceRequirements.setRequests(requests);
        container.setResources(resourceRequirements);

        // set container ports
        container.setPorts(getContainerPorts());

        return container;
    }

    private List<V1ContainerPort> getContainerPorts() {
        return new ArrayList<>();
    }

    private String createJobName(FunctionConfig functionConfig) {
        return createJobName(functionConfig.getTenant(),
                functionConfig.getNamespace(),
                functionConfig.getName());
    }

    private String createJobName(String tenant, String namespace, String functionName) {
        return functionName;
    }
}