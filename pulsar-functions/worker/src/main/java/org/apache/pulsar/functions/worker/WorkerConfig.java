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
package org.apache.pulsar.functions.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.functions.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerConfig implements Serializable, PulsarConfiguration {

    private static final long serialVersionUID = 1L;

    @Category
    private static final String CATEGORY_WORKER = "Worker Settings";
    @Category
    private static final String CATEGORY_FUNC_PKG = "Function Package Management";
    @Category
    private static final String CATEGORY_FUNC_METADATA_MNG = "Function Metadata Management";
    @Category
    private static final String CATEGORY_FUNC_RUNTIME_MNG = "Function Runtime Management";
    @Category
    private static final String CATEGORY_SECURITY = "Common Security Settings (applied for both worker and client)";
    @Category
    private static final String CATEGORY_WORKER_SECURITY = "Worker Security Settings";
    @Category
    private static final String CATEGORY_CLIENT_SECURITY = "Security settings for clients talking to brokers";
    @Category
    private static final String CATEGORY_STATE = "State Management";
    @Category
    private static final String CATEGORY_CONNECTORS = "Connectors";

    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "Id to identify a worker instance"
    )
    private String workerId;
    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "Hostname of the worker instance"
    )
    private String workerHostname;
    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "The port for serving worker http requests"
    )
    private Integer workerPort;
    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "The port for serving worker https requests"
    )
    private Integer workerPortTls;
    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "Classname of Pluggable JVM GC metrics logger that can log GC specific metrics")
    private String jvmGCMetricsLoggerClassName;
    @FieldContext(
        category = CATEGORY_WORKER,
        doc = "Number of threads to use for HTTP requests processing"
    )
    private int numHttpServerThreads = 8;
    @FieldContext(
            category = CATEGORY_WORKER,
            required = false,
            doc = "Configuration store connection string (as a comma-separated list)"
    )
    private String configurationStoreServers;
    @FieldContext(
            category = CATEGORY_WORKER,
            doc = "ZooKeeper session timeout in milliseconds"
    )
    private long zooKeeperSessionTimeoutMillis = 30000;
    @FieldContext(
            category = CATEGORY_WORKER,
            doc = "ZooKeeper operation timeout in seconds"
    )
    private int zooKeeperOperationTimeoutSeconds = 30;
    @FieldContext(
        category = CATEGORY_CONNECTORS,
        doc = "The path to the location to locate builtin connectors"
    )
    private String connectorsDirectory = "./connectors";
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar topic used for storing function metadata"
    )
    private String functionMetadataTopicName;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The web service url for function workers"
    )
    private String functionWebServiceUrl;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulser binary service url that function metadata manager talks to"
    )
    private String pulsarServiceUrl;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar web service url that function metadata manager talks to"
    )
    private String pulsarWebServiceUrl;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar topic used for cluster coordination"
    )
    private String clusterCoordinationTopicName;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar namespace for storing metadata topics"
    )
    private String pulsarFunctionsNamespace;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar cluster name. Used for creating pulsar namespace during worker initialization"
    )
    private String pulsarFunctionsCluster;
    @FieldContext(
        category = CATEGORY_FUNC_PKG,
        doc = "The number of replicas for storing functions"
    )
    private int numFunctionPackageReplicas;
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "The directory to download functions by runtime manager"
    )
    private String downloadDirectory;
    @FieldContext(
        category = CATEGORY_STATE,
        doc = "The service url of state storage"
    )
    private String stateStorageServiceUrl;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The pulsar topic used for storing function assignment informations"
    )
    private String functionAssignmentTopicName;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The scheduler class used by assigning functions to workers"
    )
    private String schedulerClassName;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The frequency of failure checks, in milliseconds"
    )
    private long failureCheckFreqMs;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The reschedule timeout of function assignment, in milliseconds"
    )
    private long rescheduleTimeoutMs;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The max number of retries for initial broker reconnects when function metadata manager"
            + " tries to create producer on metadata topics"
    )
    private int initialBrokerReconnectMaxRetries;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "The max number of retries for writing assignment to assignment topic"
    )
    private int assignmentWriteMaxRetries;
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "The frequency of instance liveness check, in milliseconds"
    )
    private long instanceLivenessCheckFreqMs;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "The authentication plugin used by function workers to talk to brokers"
    )
    private String clientAuthenticationPlugin;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "The parameters of the authentication plugin used by function workers to talk to brokers"
    )
    private String clientAuthenticationParameters;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "Authentication plugin to use when connecting to bookies"
    )
    private String bookkeeperClientAuthenticationPlugin;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "BookKeeper auth plugin implementatation specifics parameters name and values"
    )
    private String bookkeeperClientAuthenticationParametersName;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "Parameters for bookkeeper auth plugin"
    )
    private String bookkeeperClientAuthenticationParameters;
    @FieldContext(
        category = CATEGORY_FUNC_METADATA_MNG,
        doc = "Frequency how often worker performs compaction on function-topics, in seconds"
    )
    private long topicCompactionFrequencySec = 30 * 60; // 30 minutes
    /***** --- TLS --- ****/
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Enable TLS"
    )
    @Deprecated
    private boolean tlsEnabled = false;
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Path for the TLS certificate file"
    )
    private String tlsCertificateFilePath;
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Path for the TLS private key file"
    )
    private String tlsKeyFilePath;
    @FieldContext(
        category = CATEGORY_SECURITY,
        doc = "Path for the trusted TLS certificate file"
    )
    private String tlsTrustCertsFilePath = "";
    @FieldContext(
        category = CATEGORY_SECURITY,
        doc = "Accept untrusted TLS certificate from client"
    )
    private boolean tlsAllowInsecureConnection = false;
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Require trusted client cert on connect"
    )
    private boolean tlsRequireTrustedClientCertOnConnect = false;
    @FieldContext(
        category = CATEGORY_CLIENT_SECURITY,
        doc = "Whether to enable TLS when clients connect to broker",
        deprecated = true
    )
    // TLS for Functions -> Broker
    // @deprecated use "pulsar+ssl://" in serviceUrl to enable
    @Deprecated
    private boolean useTls = false;
    @FieldContext(
        category = CATEGORY_SECURITY,
        doc = "Whether to enable hostname verification on TLS connections"
    )
    private boolean tlsHostnameVerificationEnable = false;
    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "Tls cert refresh duration in seconds (set 0 to check on every new connection)"
        )
        private long tlsCertRefreshCheckDurationSec = 300;
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Enforce authentication"
    )
    private boolean authenticationEnabled = false;
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Authentication provider name list, which is a list of class names"
    )
    private Set<String> authenticationProviders = Sets.newTreeSet();
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Enforce authorization on accessing functions admin-api"
    )
    private boolean authorizationEnabled = false;
    @FieldContext(
            category = CATEGORY_WORKER_SECURITY,
            doc = "Authorization provider fully qualified class-name"
    )
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();
    @FieldContext(
        category = CATEGORY_WORKER_SECURITY,
        doc = "Role names that are treated as `super-user`, meaning they will be able to access any admin-api"
    )
    private Set<String> superUserRoles = Sets.newTreeSet();

    private Properties properties = new Properties();

    public boolean getTlsEnabled() {
    	return tlsEnabled || workerPortTls != null;
    }


    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ThreadContainerFactory {
        @FieldContext(
            doc = "The name of thread group running function threads"
        )
        private String threadGroupName;
    }
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "Thread based runtime settings"
    )
    private ThreadContainerFactory threadContainerFactory;

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ProcessContainerFactory {
        @FieldContext(
            doc = "The path to the java instance. Change the jar location only when you put"
                + " the java instance jar in a different location"
        )
        private String javaInstanceJarLocation;
        @FieldContext(
            doc = "The path to the python instance. Change the python instance location only"
                + " when you put the python instance in a different location"
        )
        private String pythonInstanceLocation;
        @FieldContext(
            doc = "The path to the log directory"
        )
        private String logDirectory;
        @FieldContext(
            doc = "the directory for dropping extra function dependencies"
        )
        private String extraFunctionDependenciesDir;
    }
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "Process based runtime settings"
    )
    private ProcessContainerFactory processContainerFactory;

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class KubernetesContainerFactory {
        @FieldContext(
            doc = "Uri to kubernetes cluster, leave it to empty and it will use the kubernetes settings in"
                + " function worker machine"
        )
        private String k8Uri;
        @FieldContext(
            doc = "The Kubernetes namespace to run the function instances. It is `default`,"
                + " if this setting is left to be empty"
        )
        private String jobNamespace;
        @FieldContext(
            doc = "The docker image used to run function instance. By default it is `apachepulsar/pulsar`"
        )
        private String pulsarDockerImageName;

        @FieldContext(
                doc = "The image pull policy for image used to run function instance. By default it is `IfNotPresent`"
        )
        private String imagePullPolicy;
        @FieldContext(
                doc = "The root directory of pulsar home directory in the pulsar docker image specified"
                        + " `pulsarDockerImageName`. By default it is under `/pulsar`. If you are using your own"
                        + " customized image in `pulsarDockerImageName`, you need to set this setting accordingly"
        )
        private String pulsarRootDir;
        @FieldContext(
            doc = "This setting only takes effects if `k8Uri` is set to null. If your function worker is"
                + " also running as a k8s pod, set this to `true` is let function worker to submit functions to"
                + " the same k8s cluster as function worker is running. Set this to `false` if your function worker"
                + " is not running as a k8s pod"
        )
        private Boolean submittingInsidePod;
        @FieldContext(
            doc = "The pulsar service url that pulsar functions should use to connect to pulsar."
                + " If it is not set, it will use the pulsar service url configured in function worker."
        )
        private String pulsarServiceUrl;
        @FieldContext(
            doc = "The pulsar admin url that pulsar functions should use to connect to pulsar."
                + " If it is not set, it will use the pulsar admin url configured in function worker."
        )
        private String pulsarAdminUrl;
        @FieldContext(
            doc = "The flag indicates to install user code dependencies. (applied to python package)"
        )
        private Boolean installUserCodeDependencies;
        @FieldContext(
            doc = "The repository that pulsar functions use to download python dependencies"
        )
        private String pythonDependencyRepository;
        @FieldContext(
            doc = "The repository that pulsar functions use to download extra python dependencies"
        )
        private String pythonExtraDependencyRepository;

        @FieldContext(
            doc = "the directory for dropping extra function dependencies. "
                + "If it is not absolute path, it is relative to `pulsarRootDir`"
        )
        private String extraFunctionDependenciesDir;
        @FieldContext(
            doc = "The custom labels that function worker uses to select the nodes for pods"
        )
        private Map<String, String> customLabels;

        @FieldContext(
            doc = "The expected metrics collection interval, in seconds"
        )
        private Integer expectedMetricsCollectionInterval = 30;
        @FieldContext(
            doc = "Kubernetes Runtime will periodically checkback on"
                + " this configMap if defined and if there are any changes"
                + " to the kubernetes specific stuff, we apply those changes"
        )
        private String changeConfigMap;
        @FieldContext(
            doc = "The namespace for storing change config map"
        )
        private String changeConfigMapNamespace;

        @FieldContext(
                doc = "Additional memory padding added on top of the memory requested by the function per on a per instance basis"
        )
        private int percentMemoryPadding;
    }
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "Kubernetes based runtime settings"
    )
    private KubernetesContainerFactory kubernetesContainerFactory;

    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "The classname of the secrets provider configurator."
    )
    private String secretsProviderConfiguratorClassName;
    @FieldContext(
        category = CATEGORY_FUNC_RUNTIME_MNG,
        doc = "Any config the secret provider configurator might need. \n\nThis is passed on"
            + " to the init method of the SecretsProviderConfigurator"
    )
    private Map<String, String> secretsProviderConfiguratorConfig;
    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "A set of the minimum amount of resources functions must request.  Support for this depends on function runtime."
    )
    private Resources functionInstanceMinResources;

    public String getFunctionMetadataTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionMetadataTopicName);
    }

    public String getClusterCoordinationTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, clusterCoordinationTopicName);
    }

    public String getFunctionAssignmentTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionAssignmentTopicName);
    }

    public static WorkerConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), WorkerConfig.class);
    }

    public String getWorkerId() {
        if (StringUtils.isBlank(this.workerId)) {
            this.workerId = String.format("%s-%s", this.getWorkerHostname(), this.getWorkerPort());
        }
        return this.workerId;
    }

    public String getWorkerHostname() {
        if (StringUtils.isBlank(this.workerHostname)) {
            this.workerHostname = unsafeLocalhostResolve();
        }
        return this.workerHostname;
    }

    public String getWorkerWebAddress() {
        return String.format("http://%s:%d", this.getWorkerHostname(), this.getWorkerPort());
    }

    public static String unsafeLocalhostResolve() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new IllegalStateException("Failed to resolve localhost name.", ex);
        }
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
