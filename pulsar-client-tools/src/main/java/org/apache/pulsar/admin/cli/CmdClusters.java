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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about clusters")
public class CmdClusters extends CmdBase {

    @Command(description = "List the existing clusters")
    private class List extends CliCommand {
        @Option(names = {"-c", "--current"},
                description = "Print the current cluster with (*)", required = false, defaultValue = "false")
        private boolean current;

        void run() throws Exception {
            java.util.List<String> clusters = getAdmin().clusters().getClusters();
            String clusterName = getAdmin().brokers().getRuntimeConfigurations().get("clusterName");
            final java.util.List<String> result = clusters.stream().map(c ->
                    c.equals(clusterName) ? (current ? c + "(*)" : c) : c
            ).collect(Collectors.toList());
            print(result);
        }
    }

    @Command(description = "Get the configuration data for the specified cluster")
    private class Get extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Override
        void run() throws Exception {
            print(getAdmin().clusters().getCluster(cluster));
        }
    }

    @Command(description = "Provisions a new cluster. This operation requires Pulsar super-user privileges")
    private class Create extends CliCommand {
        @ArgGroup(exclusive = false)
        ClusterDetails clusterDetails = new ClusterDetails();

        @Override
        void run() throws PulsarAdminException, IOException {
            getAdmin().clusters().createCluster(clusterDetails.clusterName, clusterDetails.getClusterData());
        }

    }

    protected static void validateClusterData(ClusterData clusterData) {
        if (clusterData.isBrokerClientTlsEnabled()) {
            if (clusterData.isBrokerClientTlsEnabledWithKeyStore()) {
                if (StringUtils.isAnyBlank(clusterData.getBrokerClientTlsTrustStoreType(),
                        clusterData.getBrokerClientTlsTrustStore(),
                        clusterData.getBrokerClientTlsTrustStorePassword())) {
                    throw new RuntimeException(
                            "You must specify tls-trust-store-type, tls-trust-store and tls-trust-store-pwd"
                                    + " when enable tls-enable-keystore");
                }
            }
        }
    }

    @Command(description = "Update the configuration for a cluster")
    private class Update extends CliCommand {
        @ArgGroup(exclusive = false)
        ClusterDetails clusterDetails = new ClusterDetails();

        @Override
        void run() throws PulsarAdminException, IOException {
            getAdmin().clusters().updateCluster(clusterDetails.clusterName, clusterDetails.getClusterData());
        }

    }

    @Command(description = "Deletes an existing cluster")
    private class Delete extends CliCommand {
        @Parameters(description = "cluster-name", index = "0", arity = "1")
        private String cluster;

        @Option(names = {"-a", "--all"},
                description = "Delete all data (tenants) of the cluster", required = false, defaultValue = "false")
        private boolean deleteAll;

        @Override
        void run() throws PulsarAdminException {
            if (deleteAll) {
                for (String tenant : getAdmin().tenants().getTenants()) {
                    for (String namespace : getAdmin().namespaces().getNamespaces(tenant)) {
                        // Partitioned topic's schema must be deleted by deletePartitionedTopic()
                        // but not delete() for each partition
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

    @Command(description = "Update peer cluster names")
    private class UpdatePeerClusters extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--peer-clusters", description = "Comma separated peer-cluster names "
                + "[Pass empty string \"\" to delete list]", required = true)
        private String peerClusterNames;

        @Override
        void run() throws PulsarAdminException {
            java.util.LinkedHashSet<String> clusters = StringUtils.isBlank(peerClusterNames) ? null
                    : Sets.newLinkedHashSet(Arrays.asList(peerClusterNames.split(",")));
            getAdmin().clusters().updatePeerClusterNames(cluster, clusters);
        }
    }

    @Command(description = "Get the cluster migration configuration data for the specified cluster")
    private class GetClusterMigration extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().clusters().getClusterMigration(cluster));
        }
    }

    @Command(description = "Update cluster migration")
    private class UpdateClusterMigration extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--migrated", description = "Is cluster migrated")
        private boolean migrated;

        @Option(names = "--service-url", description = "New migrated cluster service url")
        private String serviceUrl;

        @Option(names = "--service-url-secure",
                description = "New migrated cluster service url secure")
        private String serviceUrlTls;

        @Option(names = "--broker-url", description = "New migrated cluster broker service url")
        private String brokerServiceUrl;

        @Option(names = "--broker-url-secure", description = "New migrated cluster broker service url secure")
        private String brokerServiceUrlTls;

        @Override
        void run() throws PulsarAdminException {
            ClusterUrl clusterUrl = new ClusterUrl(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls);
            getAdmin().clusters().updateClusterMigration(cluster, migrated, clusterUrl);
        }
    }

    @Command(description = "Get list of peer-clusters")
    private class GetPeerClusters extends CliCommand {

        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().clusters().getPeerClusterNames(cluster));
        }
    }


    @Command(description = "Create a new failure-domain for a cluster. updates it if already created.")
    private class CreateFailureDomain extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Option(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        @Override
        void run() throws PulsarAdminException {
            FailureDomain domain = FailureDomainImpl.builder()
                    .brokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null))
                    .build();
            getAdmin().clusters().createFailureDomain(cluster, domainName, domain);
        }
    }

    @Command(description = "Update failure-domain for a cluster. Creates a new one if not exist.")
    private class UpdateFailureDomain extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Option(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        @Override
        void run() throws PulsarAdminException {
            FailureDomain domain = FailureDomainImpl.builder()
                    .brokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null))
                    .build();
            getAdmin().clusters().updateFailureDomain(cluster, domainName, domain);
        }
    }

    @Command(description = "Deletes an existing failure-domain")
    private class DeleteFailureDomain extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Override
        void run() throws PulsarAdminException {
            getAdmin().clusters().deleteFailureDomain(cluster, domainName);
        }
    }

    @Command(description = "List the existing failure-domains for a cluster")
    private class ListFailureDomains extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().clusters().getFailureDomains(cluster));
        }
    }

    @Command(description = "Get the configuration brokers of a failure-domain")
    private class GetFailureDomain extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().clusters().getFailureDomain(cluster, domainName));
        }
    }

    private static class ClusterDetails {
        @Parameters(description = "cluster-name", arity = "1")
        protected String clusterName;

        @Option(names = "--url", description = "service-url", required = false)
        protected String serviceUrl;

        @Option(names = "--url-secure", description = "service-url for secure connection", required = false)
        protected String serviceUrlTls;

        @Option(names = "--broker-url", description = "broker-service-url", required = false)
        protected String brokerServiceUrl;

        @Option(names = "--broker-url-secure",
                description = "broker-service-url for secure connection", required = false)
        protected String brokerServiceUrlTls;

        @Option(names = "--proxy-url",
                description = "Proxy-service url when client would like to connect to broker via proxy.")
        protected String proxyServiceUrl;

        @Option(names = "--auth-plugin", description = "authentication plugin", required = false)
        protected String authenticationPlugin;

        @Option(names = "--auth-parameters", description = "authentication parameters", required = false)
        protected String authenticationParameters;

        @Option(names = "--proxy-protocol",
                description = "protocol to decide type of proxy routing eg: SNI", required = false)
        protected ProxyProtocol proxyProtocol;

        @Option(names = "--tls-enable", description = "Enable tls connection", required = false)
        protected Boolean brokerClientTlsEnabled;

        @Option(names = "--tls-allow-insecure", description = "Allow insecure tls connection", required = false)
        protected Boolean tlsAllowInsecureConnection;

        @Option(names = "--tls-enable-keystore",
                description = "Whether use KeyStore type to authenticate", required = false)
        protected Boolean brokerClientTlsEnabledWithKeyStore;

        @Option(names = "--tls-trust-store-type",
                description = "TLS TrustStore type configuration for internal client eg: JKS", required = false)
        protected String brokerClientTlsTrustStoreType;

        @Option(names = "--tls-trust-store",
                description = "TLS TrustStore path for internal client", required = false)
        protected String brokerClientTlsTrustStore;

        @Option(names = "--tls-trust-store-pwd",
                description = "TLS TrustStore password for internal client", required = false)
        protected String brokerClientTlsTrustStorePassword;

        @Option(names = "--tls-key-store-type",
                description = "TLS TrustStore type configuration for internal client eg: JKS", required = false)
        protected String brokerClientTlsKeyStoreType;

        @Option(names = "--tls-key-store",
                description = "TLS KeyStore path for internal client", required = false)
        protected String brokerClientTlsKeyStore;

        @Option(names = "--tls-key-store-pwd",
                description = "TLS KeyStore password for internal client", required = false)
        protected String brokerClientTlsKeyStorePassword;

        @Option(names = "--tls-trust-certs-filepath",
                description = "path for the trusted TLS certificate file", required = false)
        protected String brokerClientTrustCertsFilePath;

        @Option(names = "--tls-key-filepath",
                description = "path for the TLS private key file", required = false)
        protected String brokerClientKeyFilePath;

        @Option(names = "--tls-certs-filepath",
                description = "path for the TLS certificate file", required = false)
        protected String brokerClientCertificateFilePath;

        @Option(names = "--tls-factory-plugin",
                description = "TLS Factory Plugin to be used to generate SSL Context and SSL Engine")
        protected String brokerClientSslFactoryPlugin;

        @Option(names = "--tls-factory-plugin-params",
                description = "Parameters used by the TLS Factory Plugin")
        protected String brokerClientSslFactoryPluginParams;

        @Option(names = "--listener-name",
                description = "listenerName when client would like to connect to cluster", required = false)
        protected String listenerName;

        @Option(names = "--cluster-config-file", description = "The path to a YAML config file specifying the "
                + "cluster's configuration")
        protected String clusterConfigFile;

        protected ClusterData getClusterData() throws IOException {
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
            if (brokerClientTlsKeyStoreType != null) {
                builder.brokerClientTlsKeyStoreType(brokerClientTlsKeyStoreType);
            }
            if (brokerClientTlsKeyStore != null) {
                builder.brokerClientTlsKeyStore(brokerClientTlsKeyStore);
            }
            if (brokerClientTlsKeyStorePassword != null) {
                builder.brokerClientTlsKeyStorePassword(brokerClientTlsKeyStorePassword);
            }
            if (brokerClientTrustCertsFilePath != null) {
                builder.brokerClientTrustCertsFilePath(brokerClientTrustCertsFilePath);
            }
            if (brokerClientKeyFilePath != null) {
                builder.brokerClientKeyFilePath(brokerClientKeyFilePath);
            }
            if (brokerClientCertificateFilePath != null) {
                builder.brokerClientCertificateFilePath(brokerClientCertificateFilePath);
            }
            if (StringUtils.isNotBlank(brokerClientSslFactoryPlugin)) {
                builder.brokerClientSslFactoryPlugin(brokerClientSslFactoryPlugin);
            }
            if (StringUtils.isNotBlank(brokerClientSslFactoryPluginParams)) {
                builder.brokerClientSslFactoryPluginParams(brokerClientSslFactoryPluginParams);
            }

            if (listenerName != null) {
                builder.listenerName(listenerName);
            }

            ClusterData clusterData = builder.build();
            validateClusterData(clusterData);

            return clusterData;
        }
    }

    public CmdClusters(Supplier<PulsarAdmin> admin) {
        super("clusters", admin);
        addCommand("get", new Get());
        addCommand("create", new Create());
        addCommand("update", new Update());
        addCommand("delete", new Delete());
        addCommand("list", new List());
        addCommand("update-peer-clusters", new UpdatePeerClusters());
        addCommand("get-cluster-migration", new GetClusterMigration());
        addCommand("update-cluster-migration", new UpdateClusterMigration());
        addCommand("get-peer-clusters", new GetPeerClusters());
        addCommand("get-failure-domain", new GetFailureDomain());
        addCommand("create-failure-domain", new CreateFailureDomain());
        addCommand("update-failure-domain", new UpdateFailureDomain());
        addCommand("delete-failure-domain", new DeleteFailureDomain());
        addCommand("list-failure-domains", new ListFailureDomains());
    }

}
