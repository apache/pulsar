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
package org.apache.pulsar.tests.integration.k8s;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.CoreV1EventList;
import io.kubernetes.client.openapi.models.RbacV1SubjectBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PolicyRuleBuilder;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1RoleBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1ServiceAccountBuilder;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.secretsproviderconfigurator.KubernetesSecretsProviderConfigurator;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Abstract base class for integration tests that use Kubernetes (K8s) and Pulsar standalone.
 * This class sets up a Kubernetes environment using the k3s lightweight Kubernetes container
 * and deploys a standalone Pulsar instance within this Kubernetes cluster.
 * It provides configuration and utility methods to allow test classes to interact
 * with the deployed Pulsar instance and Kubernetes cluster.
 * The main reason to use this base class is to test features in Pulsar which are integrated into Kubernetes
 * APIs.
 */
@Slf4j
public abstract class AbstractPulsarStandaloneK8STest {
    private static final String DEFAULT_IMAGE_NAME = System.getenv().getOrDefault("PULSAR_TEST_IMAGE_NAME",
            "apachepulsar/java-test-image:latest");
    private static final int PULSAR_NODE_PORT = 30101;
    private static final int PULSAR_HTTP_NODE_PORT = 30102;
    private static final String K3S_IMAGE_NAME = "rancher/k3s:v1.33.5-k3s1";
    private static final String PULSAR_STANDALONE_POD = "pulsar-standalone-pod";
    K3sContainer k3sContainer;
    KubeConfig kubeConfig;
    @Getter
    ApiClient apiClient;
    DockerClient dockerClient;
    String dockerHostName;
    @Getter
    String pulsarBrokerUrl;
    @Getter
    String pulsarWebServiceUrl;
    @Getter
    File kubeConfigFile;

    @BeforeClass
    public final void setupCluster() throws IOException, ApiException, InterruptedException {
        k3sContainer = new K3sContainer(DockerImageName.parse(K3S_IMAGE_NAME));
        k3sContainer.addExposedPort(PULSAR_NODE_PORT);
        k3sContainer.addExposedPort(PULSAR_HTTP_NODE_PORT);
        k3sContainer.start();
        dockerHostName = k3sContainer.getHost();
        pulsarBrokerUrl = "pulsar://" + dockerHostName + ":" + k3sContainer.getMappedPort(PULSAR_NODE_PORT);
        pulsarWebServiceUrl = "http://" + dockerHostName + ":" + k3sContainer.getMappedPort(PULSAR_HTTP_NODE_PORT);
        dockerClient = k3sContainer.getDockerClient();
        String kubeConfigYaml = k3sContainer.getKubeConfigYaml();
        kubeConfig = KubeConfig.loadKubeConfig(new StringReader(kubeConfigYaml));
        apiClient = Config.fromConfig(kubeConfig);
        kubeConfigFile = File.createTempFile("kubeconfig", ".yaml");
        Files.writeString(kubeConfigFile.toPath(), kubeConfigYaml);
        log.info("Pulsar broker URL: {} http URL: {}", pulsarBrokerUrl, pulsarWebServiceUrl);
        log.info("For debugging k8s, use KUBECONFIG={}", kubeConfigFile.getAbsolutePath());
        importPulsarImage();
        deployPulsarStandalonePod();
        log.info("Waiting for Pulsar cluster to be ready");
        Wait.forHttp("/admin/v2/tenants")
                .forPort(PULSAR_HTTP_NODE_PORT)
                // when Pulsar Functions are ready, there will also be a pulsar tenant created
                .forResponsePredicate("[\"public\",\"pulsar\"]"::equals)
                .withStartupTimeout(Duration.ofMinutes(5))
                .waitUntilReady(k3sContainer);
        log.info("Pulsar cluster ready. Waiting 3 seconds before continuing for Functions Leader to be ready.");
        Thread.sleep(3000);
    }

    @AfterClass(alwaysRun = true)
    public final void cleanupCluster() throws InterruptedException {
        if (k3sContainer != null) {
            copyLogsToTargetDirectory();
            k3sContainer.stop();
        }
    }

    private void copyLogsToTargetDirectory() {
        if (apiClient != null) {
            File targetDirectoryForLogs = getTargetDirectoryForLogs();
            Exec exec = new Exec(apiClient);
            try {
                log.info("Copying logs from Pulsar standalone pod to target directory: {}", targetDirectoryForLogs);
                Process process = exec.newExecutionBuilder(getNamespace(), PULSAR_STANDALONE_POD,
                        new String[]{"sh", "-c", "cd /pulsar/logs && tar cf - *"}
                ).setTty(false).setStdin(false).setStderr(false).setStdout(true).execute();
                try (TarInputStream tarInputStream = new TarInputStream(process.getInputStream())) {
                    TarEntry tarEntry;
                    while ((tarEntry = tarInputStream.getNextEntry()) != null) {
                        if (tarEntry.isFile()) {
                            File targetFile = new File(targetDirectoryForLogs, new File(tarEntry.getName()).getName());
                            Files.copy(tarInputStream, targetFile.toPath());
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error copying logs from Pulsar standalone pod to target directory.", e);
            }

            CoreV1Api coreApi = new CoreV1Api(apiClient);
            File eventsFile = new File(targetDirectoryForLogs, "k8s_events.json");
            try {
                log.info("Copying events from Kubernetes cluster namespace {} to {}.", getNamespace(), eventsFile);
                CoreV1EventList eventList = coreApi.listNamespacedEvent(getNamespace()).execute();
                Files.writeString(eventsFile.toPath(), eventList.toJson());
            } catch (Exception e) {
                log.error("Error copying events from Kubernetes cluster to {}.", eventsFile, e);
            }
        }
    }

    private File getTargetDirectoryForLogs() {
        String base = System.getProperty("maven.buildDirectory");
        if (base == null) {
            base = "target";
        }
        // use the container-logs directory since it's used in CI for integration tests as the file location
        File directory = new File(new File(base, "container-logs"),
                "k8s_" + getClass().getSimpleName() + "_" + System.currentTimeMillis());
        if (!directory.exists() && !directory.mkdirs()) {
            log.error("Error creating directory for logs.");
        }
        return directory;
    }

    protected String getPulsarImageName() {
        return DEFAULT_IMAGE_NAME;
    }

    private void deployPulsarStandalonePod() throws ApiException {
        createServiceAccountAndRoleAndRoleBinding();
        CoreV1Api coreApi = new CoreV1Api(apiClient);
        String namespace = getNamespace();
        V1Pod pod = createPulsarPod();
        coreApi.createNamespacedPod(namespace, pod).execute();
        V1Service service = createPulsarService();
        coreApi.createNamespacedService(namespace, service).execute();
    }

    protected String getNamespace() {
        return "default";
    }

    // This method imports the local image into the k3s container's containerd registry so that it can be used
    // to create pods. image pull policy should be set to Never when using the image.
    private void importPulsarImage() {
        // Save local image from Docker
        InputStream imageStream = dockerClient.saveImageCmd(getPulsarImageName()).exec();

        // Create exec instance with stdin
        String containerId = k3sContainer.getContainerId();

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
                .withAttachStdin(true)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd("ctr", "images", "import", "/dev/stdin")
                .exec();

        // Start exec and stream data
        try (InputStream is = imageStream) {
            dockerClient.execStartCmd(execCreateCmdResponse.getId())
                    .withStdIn(is)
                    .exec(new ExecStartResultCallback(System.out, System.err))
                    .awaitCompletion();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void createServiceAccountAndRoleAndRoleBinding() throws ApiException {
        CoreV1Api coreApi = new CoreV1Api(apiClient);
        V1ServiceAccount serviceAccount = new V1ServiceAccountBuilder()
                .withNewMetadata()
                .withName("pulsar-standalone-sa")
                .endMetadata()
                .build();
        coreApi.createNamespacedServiceAccount(getNamespace(), serviceAccount).execute();
        V1Role role = new V1RoleBuilder()
                .withNewMetadata()
                .withName("pulsar-functions-role")
                .endMetadata()
                .withRules(
                        new V1PolicyRuleBuilder()
                                .addToApiGroups("")
                                .addToResources("services", "configmaps", "pods")
                                .addToVerbs("*").build(),
                        new V1PolicyRuleBuilder()
                                .addToApiGroups("apps")
                                .addToResources("statefulsets")
                                .addToVerbs("*").build()
                )
                .build();
        RbacAuthorizationV1Api rbacApi = new RbacAuthorizationV1Api(apiClient);
        rbacApi.createNamespacedRole(getNamespace(), role).execute();
        V1RoleBinding roleBinding = new V1RoleBindingBuilder()
                .withNewMetadata()
                .withName("pulsar-functions-role-binding")
                .endMetadata()
                .withNewRoleRef()
                .withKind("Role")
                .withName("pulsar-functions-role")
                .endRoleRef()
                .withSubjects(new RbacV1SubjectBuilder()
                        .withKind("ServiceAccount")
                        .withName("pulsar-standalone-sa")
                        .withNamespace(getNamespace())
                        .build())
                .build();
        rbacApi.createNamespacedRoleBinding(getNamespace(), roleBinding).execute();
    }

    private V1Pod createPulsarPod() {
        V1PodBuilder podBuilder = new V1PodBuilder();
        var containerBuilder = podBuilder
                .withNewMetadata()
                .withName(PULSAR_STANDALONE_POD)
                .addToLabels("app", "pulsar")
                .addToLabels("component", "standalone")
                .endMetadata()
                .withNewSpec()
                .withServiceAccountName("pulsar-standalone-sa")
                .addNewContainer();

        // Container
        containerBuilder
                .withName("pulsar")
                .withImage(getPulsarImageName())
                .withImagePullPolicy("Never")
                .withCommand("sh", "-c",
                        "bin/apply-config-from-env.py conf/standalone.conf && "
                                + "bin/gen-yml-from-env.py conf/functions_worker.yml && "
                                + "bin/update-rocksdb-conf-from-env.py conf/entry_location_rocksdb.conf && "
                                + "bin/pulsar standalone")
                // Ports
                .addNewPort()
                .withName("pulsar")
                .withContainerPort(6650)
                .withProtocol("TCP")
                .endPort()
                .addNewPort()
                .withName("http")
                .withContainerPort(8080)
                .withProtocol("TCP")
                .endPort()
                .addNewPort()
                .withName("pulsar-nodeport")
                .withContainerPort(16650)
                .withProtocol("TCP")
                .endPort();

        // Environment variables
        for (Map.Entry<String, String> env : getBrokerEnv().entrySet()) {
            containerBuilder.addNewEnv()
                    .withName(env.getKey())
                    .withValue(env.getValue())
                    .endEnv();
        }

        // Resource limits
        containerBuilder.withNewResources()
                .withLimits(null)
                .addToRequests("memory", new Quantity("128Mi"))
                .addToRequests("cpu", new Quantity("100m"))
                .endResources();

        return containerBuilder.endContainer()
                .withOverhead(null)
                .endSpec().build();
    }

    protected Map<String, String> getBrokerEnv() {
        Map<String, String> envVars = new LinkedHashMap<>() {
            {
                put("PULSAR_MEM", "-Xmx256m");
                put("PULSAR_GC", "-XX:+UseG1GC");
                put("bindAddresses", "nodeport:pulsar://0.0.0.0:16650");
                put("PULSAR_PREFIX_advertisedListeners",
                        "internal:pulsar://pulsar.default.svc.cluster.local:6650,nodeport:" + pulsarBrokerUrl);
                // log to file since docker logs will get truncated
                put("PULSAR_ROUTING_APPENDER_DEFAULT", "RollingFile");
                put("dbStorage_writeCacheMaxSizeMb", "32");
                put("dbStorage_readAheadCacheMaxSizeMb", "32");
                put("PF_functionsDirectory", "/pulsar/examples");
                put("PF_functionRuntimeFactoryClassName", KubernetesRuntimeFactory.class.getName());
                put("PF_secretsProviderConfiguratorClassName", KubernetesSecretsProviderConfigurator.class.getName());
                put("PF_functionRuntimeFactoryConfigs_pulsarDockerImageName", getPulsarImageName());
                put("PF_kubernetesContainerFactory_imagePullPolicy", "Never");
                put("PF_functionRuntimeFactoryConfigs_submittingInsidePod", "true");
                put("PF_functionRuntimeFactoryConfigs_installUserCodeDependencies", "true");
                put("PF_functionRuntimeFactoryConfigs_jobNamespace", getNamespace());
                put("PF_functionRuntimeFactoryConfigs_pulsarAdminUrl", "http://pulsar.default.svc.cluster.local");
                put("PF_functionRuntimeFactoryConfigs_pulsarServiceUrl",
                        "pulsar://pulsar.default.svc.cluster.local:6650");
            }
        };
        return envVars;
    }

    public V1Service createPulsarService() {
        V1Service service = new V1ServiceBuilder()
                .withNewMetadata()
                .withName("pulsar")
                .endMetadata()
                .withNewSpec()
                .withType("NodePort")
                .withSelector(Map.of("app", "pulsar", "component", "standalone"))
                // Ports
                .addNewPort()
                .withName("pulsar-nodeport")
                .withNodePort(PULSAR_NODE_PORT)
                .withPort(16650)
                .withNewTargetPort(16650)
                .withProtocol("TCP")
                .endPort()
                .addNewPort()
                .withName("http")
                .withPort(80)
                .withNodePort(PULSAR_HTTP_NODE_PORT)
                .withNewTargetPort(8080)
                .withProtocol("TCP")
                .endPort()
                .addNewPort()
                .withName("pulsar")
                .withPort(6650)
                .withNewTargetPort(6650)
                .withProtocol("TCP")
                .endPort()
                .endSpec()
                .build();
        return service;
    }
}
