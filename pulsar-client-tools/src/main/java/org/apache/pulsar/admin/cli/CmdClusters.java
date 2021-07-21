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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;

@Parameters(commandDescription = "Operations about clusters")
public class CmdClusters extends CmdBase {

    @Parameters(commandDescription = "List the existing clusters")
    private class List extends CliCommand {
        void run() throws PulsarAdminException {
            print(getAdmin().clusters().getClusters());
        }
    }

    @Parameters(commandDescription = "Get the configuration data for the specified cluster")
    private class Get extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(getAdmin().clusters().getCluster(cluster));
        }
    }

    @Parameters(commandDescription = "Provisions a new cluster. This operation requires Pulsar super-user privileges")
    private class Create extends ClusterDetailsCommand {

        @Override
        void runCmd() throws Exception {
            String cluster = getOneArgument(params);
            getAdmin().clusters().createCluster(cluster, clusterData);
        }

    }

    protected void validateClusterData(ClusterData clusterData) {
        if (clusterData.isBrokerClientTlsEnabled()) {
            if (clusterData.isBrokerClientTlsEnabledWithKeyStore()) {
                if (StringUtils.isAnyBlank(clusterData.getBrokerClientTlsTrustStoreType(), clusterData.getBrokerClientTlsTrustStore(),
                        clusterData.getBrokerClientTlsTrustStorePassword())) {
                    throw new RuntimeException(
                            "You must specify tls-trust-store-type, tls-trust-store and tls-trust-store-pwd"
                                    + " when enable tls-enable-keystore");
                }
            } else {
                if (StringUtils.isBlank(clusterData.getBrokerClientTrustCertsFilePath())) {
                    throw new RuntimeException("You must specify tls-trust-certs-filepath"
                            + " when tls-enable-keystore is not enable");
                }
            }
        }
    }

    @Parameters(commandDescription = "Update the configuration for a cluster")
    private class Update extends ClusterDetailsCommand {

        @Override
        void runCmd() throws Exception {
            String cluster = getOneArgument(params);
            getAdmin().clusters().updateCluster(cluster, clusterData);
        }

    }

    @Parameters(commandDescription = "Deletes an existing cluster")
    private class Delete extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-a", "--all" }, description = "Delete all data (tenants) of the cluster", required = false)
        private boolean deleteAll = false;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);

            if (deleteAll) {
                for (String tenant : getAdmin().tenants().getTenants()) {
                    for (String namespace : getAdmin().namespaces().getNamespaces(tenant)) {
                        // Partitioned topic's schema must be deleted by deletePartitionedTopic() but not delete() for each partition
                        for (String topic : getAdmin().topics().getPartitionedTopicList(namespace)) {
                            getAdmin().topics().deletePartitionedTopic(topic, true, true);
                        }
                        for (String topic : getAdmin().topics().getList(namespace)) {
                            getAdmin().topics().delete(topic, true, true);
                        }
                        getAdmin().namespaces().deleteNamespace(namespace, true);
                    }
                    getAdmin().tenants().deleteTenant(tenant);
                }
            }

            getAdmin().clusters().deleteCluster(cluster);
        }
    }

    @Parameters(commandDescription = "Update peer cluster names")
    private class UpdatePeerClusters extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--peer-clusters", description = "Comma separated peer-cluster names [Pass empty string \"\" to delete list]", required = true)
        private String peerClusterNames;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            java.util.LinkedHashSet<String> clusters = StringUtils.isBlank(peerClusterNames) ? null
                    : Sets.newLinkedHashSet(Arrays.asList(peerClusterNames.split(",")));
            getAdmin().clusters().updatePeerClusterNames(cluster, clusters);
        }
    }

    @Parameters(commandDescription = "Get list of peer-clusters")
    private class GetPeerClusters extends CliCommand {

        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(getAdmin().clusters().getPeerClusterNames(cluster));
        }
    }


    @Parameters(commandDescription = "Create a new failure-domain for a cluster. updates it if already created.")
    private class CreateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = FailureDomainImpl.builder()
                    .brokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null))
                    .build();
            getAdmin().clusters().createFailureDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Update failure-domain for a cluster. Creates a new one if not exist.")
    private class UpdateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = FailureDomainImpl.builder()
                    .brokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null))
                    .build();
            getAdmin().clusters().updateFailureDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Deletes an existing failure-domain")
    private class DeleteFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            getAdmin().clusters().deleteFailureDomain(cluster, domainName);
        }
    }

    @Parameters(commandDescription = "List the existing failure-domains for a cluster")
    private class ListFailureDomains extends CliCommand {

        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(getAdmin().clusters().getFailureDomains(cluster));
        }
    }

    @Parameters(commandDescription = "Get the configuration brokers of a failure-domain")
    private class GetFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(getAdmin().clusters().getFailureDomain(cluster, domainName));
        }
    }

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
            try {
                processArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println();
                String chosenCommand = jcommander.getParsedCommand();
                getUsageFormatter().usage(chosenCommand);
                return;
            }
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    abstract class ClusterDetailsCommand extends BaseCommand {
        @Parameter(description = "cluster-name", required = true)
        protected java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = false)
        protected String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        protected String serviceUrlTls;

        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        protected String brokerServiceUrl;

        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        protected String brokerServiceUrlTls;

        @Parameter(names = "--proxy-url", description = "Proxy-service url when client would like to connect to broker via proxy.", required = false)
        protected String proxyServiceUrl;

        @Parameter(names = "--auth-plugin", description = "authentication plugin", required = false)
        protected String authenticationPlugin;

        @Parameter(names = "--auth-parameters", description = "authentication parameters", required = false)
        protected String authenticationParameters;

        @Parameter(names = "--proxy-protocol", description = "protocol to decide type of proxy routing eg: SNI", required = false)
        protected ProxyProtocol proxyProtocol;

        @Parameter(names = "--tls-enable", description = "Enable tls connection", required = false)
        protected Boolean brokerClientTlsEnabled;

        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection", required = false)
        protected Boolean tlsAllowInsecureConnection;

        @Parameter(names = "--tls-enable-keystore", description = "Whether use KeyStore type to authenticate", required = false)
        protected Boolean brokerClientTlsEnabledWithKeyStore;

        @Parameter(names = "--tls-trust-store-type", description = "TLS TrustStore type configuration for internal client eg: JKS", required = false)
        protected String brokerClientTlsTrustStoreType;

        @Parameter(names = "--tls-trust-store", description = "TLS TrustStore path for internal client", required = false)
        protected String brokerClientTlsTrustStore;

        @Parameter(names = "--tls-trust-store-pwd", description = "TLS TrustStore password for internal client", required = false)
        protected String brokerClientTlsTrustStorePassword;

        @Parameter(names = "--tls-trust-certs-filepath", description = "path for the trusted TLS certificate file", required = false)
        protected String brokerClientTrustCertsFilePath;

        @Parameter(names = "--listener-name", description = "listenerName when client would like to connect to cluster", required = false)
        protected String listenerName;

        @Parameter(names = "--cluster-config-file", description = "The path to a YAML config file specifying the "
                + "cluster's configuration")
        protected String clusterConfigFile;

        protected ClusterData clusterData;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            ClusterData.Builder builder;
            if (null != clusterConfigFile) {
                builder = CmdUtils.loadConfig(clusterConfigFile, ClusterDataImpl.ClusterDataImplBuilder.class);
            } else {
                builder = ClusterData.builder();
            }

            if (serviceUrl != null) {
                builder.serviceUrl(serviceUrl);
            }
            if (serviceUrlTls != null) {
                builder.serviceUrlTls(serviceUrlTls);
            }
            if (brokerServiceUrl != null) {
                builder.brokerServiceUrl(brokerServiceUrl);
            }
            if (brokerServiceUrlTls != null) {
                builder.brokerServiceUrlTls(brokerServiceUrlTls);
            }
            if (proxyServiceUrl != null) {
                builder.proxyServiceUrl(proxyServiceUrl);
            }
            if (authenticationPlugin != null) {
                builder.authenticationPlugin(authenticationPlugin);
            }
            if (authenticationParameters != null) {
                builder.authenticationParameters(authenticationParameters);
            }
            if (proxyProtocol != null) {
                builder.proxyProtocol(proxyProtocol);
            }
            if (brokerClientTlsEnabled != null) {
                builder.brokerClientTlsEnabled(brokerClientTlsEnabled);
            }
            if (tlsAllowInsecureConnection != null) {
                builder.tlsAllowInsecureConnection(tlsAllowInsecureConnection);
            }
            if (brokerClientTlsEnabledWithKeyStore != null) {
                builder.brokerClientTlsEnabledWithKeyStore(brokerClientTlsEnabledWithKeyStore);
            }
            if (brokerClientTlsTrustStoreType != null) {
                builder.brokerClientTlsTrustStoreType(brokerClientTlsTrustStoreType);
            }
            if (brokerClientTlsTrustStore != null) {
                builder.brokerClientTlsTrustStore(brokerClientTlsTrustStore);
            }
            if (brokerClientTlsTrustStorePassword != null) {
                builder.brokerClientTlsTrustStorePassword(brokerClientTlsTrustStorePassword);
            }
            if (brokerClientTrustCertsFilePath != null) {
                builder.brokerClientTrustCertsFilePath(brokerClientTrustCertsFilePath);
            }

            if (listenerName != null) {
                builder.listenerName(listenerName);
            }

            this.clusterData = builder.build();
            validateClusterData(clusterData);
        }
    }

    public CmdClusters(Supplier<PulsarAdmin> admin) {
        super("clusters", admin);
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
        jcommander.addCommand("list", new List());
        jcommander.addCommand("update-peer-clusters", new UpdatePeerClusters());
        jcommander.addCommand("get-peer-clusters", new GetPeerClusters());
        jcommander.addCommand("get-failure-domain", new GetFailureDomain());
        jcommander.addCommand("create-failure-domain", new CreateFailureDomain());
        jcommander.addCommand("update-failure-domain", new UpdateFailureDomain());
        jcommander.addCommand("delete-failure-domain", new DeleteFailureDomain());
        jcommander.addCommand("list-failure-domains", new ListFailureDomains());
    }

}
