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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.functions.Resources;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;

@Data
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
            category = CATEGORY_WORKER,
            doc = "ZooKeeper cache expiry time in seconds"
        )
    private int zooKeeperCacheExpirySeconds = 300;
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
        category = CATEGORY_WORKER_SECURITY,
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

    /******** security settings for pulsar broker client **********/

    @FieldContext(
            category = CATEGORY_CLIENT_SECURITY,
            doc = "The path to trusted certificates used by the Pulsar client to authenticate with Pulsar brokers"
    )
    private String brokerClientTrustCertsFilePath;


    /******** Function Runtime configurations **********/


    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "The classname of the function runtime factory."
    )
    private String functionRuntimeFactoryClassName;

    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "A map of configs for function runtime factory."
    )
    private Map<String, Object> functionRuntimeFactoryConfigs;

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

    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "The class name of the Function Authentication Provider to use." +
                    "  The Function Authentication Provider is responsible to distributing the necessary" +
                    " authentication information to individual functions e.g. user tokens"
    )
    @Getter(AccessLevel.NONE) private String functionAuthProviderClassName;

    public String getFunctionAuthProviderClassName() {
        // if we haven't set a value and are running kubernetes, we default to the SecretsTokenAuthProvider
        // as that matches behavior before this property could be overridden
        if (!StringUtils.isEmpty(functionAuthProviderClassName)) {
            return functionAuthProviderClassName;
        } else {
            if (StringUtils.equals(this.getFunctionRuntimeFactoryClassName(), KubernetesRuntimeFactory.class.getName()) || getKubernetesContainerFactory() != null) {
                return KubernetesSecretsTokenAuthProvider.class.getName();
            }
            return null;
        }
    }

    @FieldContext(
            doc = "The full class-name of an instance of RuntimeCustomizer." +
                    " This class receives the 'customRuntimeOptions string and can customize" +
                    " details of how the runtime operates"
    )
    protected String runtimeCustomizerClassName;

    @FieldContext(
            doc = "A map of config passed to the RuntimeCustomizer." +
                    " This config is distinct from the `customRuntimeOptions` provided by functions" +
                    " as this config is the the same across all functions"
    )
    private Map<String, Object> runtimeCustomizerConfig = Collections.emptyMap();

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

    public byte[] getTlsTrustChainBytes() {
        if (StringUtils.isNotEmpty(getTlsTrustCertsFilePath()) && Files.exists(Paths.get(getTlsTrustCertsFilePath()))) {
            try {
                return Files.readAllBytes(Paths.get(getTlsTrustCertsFilePath()));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read CA bytes", e);
            }
        } else {
            return null;
        }
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

    /********* DEPRECATED CONFIGS *********/

    @Deprecated
    @Data
    /**
     * @Deprecated in favor for using functionRuntimeFactoryClassName and functionRuntimeFactoryConfigs
     * for specifying the function runtime and configs to use
     */
    public static class ThreadContainerFactory extends ThreadRuntimeFactoryConfig {

    }
    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "Thread based runtime settings"
    )
    @Deprecated
    private ThreadContainerFactory threadContainerFactory;

    @Deprecated
    @Data
    /**
     * @Deprecated in favor for using functionRuntimeFactoryClassName and functionRuntimeFactoryConfigs
     * for specifying the function runtime and configs to use
     */
    public static class ProcessContainerFactory extends ProcessRuntimeFactoryConfig {

    }
    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "Process based runtime settings"
    )
    @Deprecated
    private ProcessContainerFactory processContainerFactory;

    @Deprecated
    @Data
    /**
     * @Deprecated in favor for using functionRuntimeFactoryClassName and functionRuntimeFactoryConfigs
     * for specifying the function runtime and configs to use
     */
    public static class KubernetesContainerFactory extends KubernetesRuntimeFactoryConfig {

    }
    @FieldContext(
            category = CATEGORY_FUNC_RUNTIME_MNG,
            doc = "Kubernetes based runtime settings"
    )
    @Deprecated
    private KubernetesContainerFactory kubernetesContainerFactory;
}
