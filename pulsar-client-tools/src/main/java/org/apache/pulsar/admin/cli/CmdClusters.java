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

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

@Parameters(commandDescription = "Operations about clusters")
public class CmdClusters extends CmdBase {

    @Parameters(commandDescription = "List the existing clusters")
    private class List extends CliCommand {
        void run() throws PulsarAdminException {
            print(admin.clusters().getClusters());
        }
    }

    @Parameters(commandDescription = "Get the configuration data for the specified cluster")
    private class Get extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getCluster(cluster));
        }
    }

    @Parameters(commandDescription = "Provisions a new cluster. This operation requires Pulsar super-user privileges")
    private class Create extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;
        
        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;
        
        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        @Parameter(names = "--proxy-url", description = "Proxy-service url when client would like to connect to broker via proxy.", required = false)
        private String proxyServiceUrl;

        @Parameter(names = "--proxy-protocol", description = "protocol to decide type of proxy routing eg: SNI", required = false)
        private ProxyProtocol proxyProtocol;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().createCluster(cluster,
                    new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls, proxyServiceUrl,
                            proxyProtocol));
        }
    }

    @Parameters(commandDescription = "Update the configuration for a cluster")
    private class Update extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;
        
        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;
        
        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        @Parameter(names = "--proxy-url", description = "Proxy-service url when client would like to connect to broker via proxy.", required = false)
        private String proxyServiceUrl;

        @Parameter(names = "--proxy-protocol", description = "protocol to decide type of proxy routing eg: SNI", required = false)
        private ProxyProtocol proxyProtocol;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().updateCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl,
                    brokerServiceUrlTls, proxyServiceUrl, proxyProtocol));
        }
    }

    @Parameters(commandDescription = "Deletes an existing cluster")
    private class Delete extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().deleteCluster(cluster);
        }
    }

    @Parameters(commandDescription = "Update peer cluster names")
    private class UpdatePeerClusters extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--peer-clusters", description = "Comma separated peer-cluster names [Pass empty string \"\" to delete list]", required = true)
        private String peerClusterNames;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            java.util.LinkedHashSet<String> clusters = StringUtils.isBlank(peerClusterNames) ? null
                    : Sets.newLinkedHashSet(Arrays.asList(peerClusterNames.split(",")));
            admin.clusters().updatePeerClusterNames(cluster, clusters);
        }
    }

    @Parameters(commandDescription = "Get list of peer-clusters")
    private class GetPeerClusters extends CliCommand {
        
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getPeerClusterNames(cluster));
        }
    }
    
    
    @Parameters(commandDescription = "Create a new failure-domain for a cluster. updates it if already created.")
    private class CreateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;
        
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = new FailureDomain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")): null));
            admin.clusters().createFailureDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Update failure-domain for a cluster. Creates a new one if not exist.")
    private class UpdateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = new FailureDomain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null));
            admin.clusters().updateFailureDomain(cluster, domainName, domain);
        }
    }
    
    @Parameters(commandDescription = "Deletes an existing failure-domain")
    private class DeleteFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().deleteFailureDomain(cluster, domainName);
        }
    }

    @Parameters(commandDescription = "List the existing failure-domains for a cluster")
    private class ListFailureDomains extends CliCommand {
        
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getFailureDomains(cluster));
        }
    }

    @Parameters(commandDescription = "Get the configuration brokers of a failure-domain")
    private class GetFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        
        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getFailureDomain(cluster, domainName));
        }
    }
    
    public CmdClusters(PulsarAdmin admin) {
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
