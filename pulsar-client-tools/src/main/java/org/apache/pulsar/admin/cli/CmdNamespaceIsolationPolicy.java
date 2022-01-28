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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.CommaParameterSplitter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.NameValueParameterSplitter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;

@Parameters(commandDescription = "Operations about namespace isolation policy")
public class CmdNamespaceIsolationPolicy extends CmdBase {
    @Parameters(commandDescription = "Create/Update a namespace isolation policy for a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class SetPolicy extends CliCommand {
        @Parameter(description = "cluster-name policy-name", required = true)
        private List<String> params;

        @Parameter(names = "--namespaces", description = "comma separated namespaces-regex list",
                required = true, splitter = CommaParameterSplitter.class)
        private List<String> namespaces;

        @Parameter(names = "--primary", description = "comma separated  primary-broker-regex list. "
                + "In Pulsar, when namespaces (more specifically, namespace bundles) are assigned dynamically to "
                + "brokers, the namespace isolation policy limits the set of brokers that can be used for assignment. "
                + "Before topics are assigned to brokers, you can set the namespace isolation policy with a primary or "
                + "a secondary regex to select desired brokers. If no broker matches the specified regex, you cannot "
                + "create a topic. If there are not enough primary brokers, topics are assigned to secondary brokers. "
                + "If there are not enough secondary brokers, topics are assigned to other brokers which do not have "
                + "any isolation policies.", required = true, splitter = CommaParameterSplitter.class)
        private List<String> primary;

        @Parameter(names = "--secondary", description = "comma separated secondary-broker-regex list",
                required = false, splitter = CommaParameterSplitter.class)
        private List<String> secondary = new ArrayList<String>(); // optional

        @Parameter(names = "--auto-failover-policy-type",
                description = "auto failover policy type name ['min_available']", required = true)
        private String autoFailoverPolicyTypeName;

        @Parameter(names = "--auto-failover-policy-params",
                description = "comma separated name=value auto failover policy parameters",
                required = true, converter = NameValueParameterSplitter.class)
        private Map<String, String> autoFailoverPolicyParams;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params, 0, 2);
            String policyName = getOneArgument(params, 1, 2);

            // validate and create the POJO
            NamespaceIsolationData namespaceIsolationData = createNamespaceIsolationData(namespaces, primary, secondary,
                    autoFailoverPolicyTypeName, autoFailoverPolicyParams);

            getAdmin().clusters().createNamespaceIsolationPolicy(clusterName, policyName, namespaceIsolationData);
        }
    }

    @Parameters(commandDescription = "List all namespace isolation policies of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class GetAllPolicies extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private List<String> params;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params);

            Map<String, ? extends NamespaceIsolationData> policyMap =
                    getAdmin().clusters().getNamespaceIsolationPolicies(clusterName);

            print(policyMap);
        }
    }

    @Parameters(commandDescription = "List all brokers with namespace-isolation policies attached to it. "
            + "This operation requires Pulsar super-user privileges")
    private class GetAllBrokersWithPolicies extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private List<String> params;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params);

            List<BrokerNamespaceIsolationData> brokers = getAdmin().clusters()
                    .getBrokersWithNamespaceIsolationPolicy(clusterName);
            List<BrokerNamespaceIsolationDataImpl> data = new ArrayList<>();
            brokers.forEach(v -> data.add((BrokerNamespaceIsolationDataImpl) v));
            print(data);
        }
    }

    @Parameters(commandDescription = "Get broker with namespace-isolation policies attached to it. "
            + "This operation requires Pulsar super-user privileges")
    private class GetBrokerWithPolicies extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private List<String> params;

        @Parameter(names = "--broker",
                description = "Broker-name to get namespace-isolation policies attached to it", required = true)
        private String broker;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params);

            BrokerNamespaceIsolationDataImpl brokerData = (BrokerNamespaceIsolationDataImpl) getAdmin().clusters()
                    .getBrokerWithNamespaceIsolationPolicy(clusterName, broker);

            print(brokerData);
        }
    }

    @Parameters(commandDescription = "Get namespace isolation policy of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class GetPolicy extends CliCommand {
        @Parameter(description = "cluster-name policy-name", required = true)
        private List<String> params;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params, 0, 2);
            String policyName = getOneArgument(params, 1, 2);

            NamespaceIsolationDataImpl nsIsolationData = (NamespaceIsolationDataImpl) getAdmin().clusters()
                    .getNamespaceIsolationPolicy(clusterName, policyName);

            print(nsIsolationData);
        }
    }

    @Parameters(commandDescription = "Delete namespace isolation policy of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class DeletePolicy extends CliCommand {
        @Parameter(description = "cluster-name policy-name", required = true)
        private List<String> params;

        void run() throws PulsarAdminException {
            String clusterName = getOneArgument(params, 0, 2);
            String policyName = getOneArgument(params, 1, 2);

            getAdmin().clusters().deleteNamespaceIsolationPolicy(clusterName, policyName);
        }
    }

    private List<String> validateList(List<String> list) {
        return list.stream()
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    private NamespaceIsolationData createNamespaceIsolationData(List<String> namespaces,
                                                                List<String> primary,
                                                                List<String> secondary,
                                                                String autoFailoverPolicyTypeName,
                                                                Map<String, String> autoFailoverPolicyParams) {

        // validate
        namespaces = validateList(namespaces);
        if (namespaces.isEmpty()) {
            throw new ParameterException("unable to parse namespaces parameter list: " + namespaces);
        }

        primary = validateList(primary);
        if (primary.isEmpty()) {
            throw new ParameterException("unable to parse primary parameter list: " + namespaces);
        }

        secondary = validateList(secondary);

        // System.out.println("namespaces = " + namespaces);
        // System.out.println("primary = " + primary);
        // System.out.println("secondary = " + secondary);
        // System.out.println("autoFailoverPolicyTypeName = " + autoFailoverPolicyTypeName);
        // System.out.println("autoFailoverPolicyParams = " + autoFailoverPolicyParams);

        NamespaceIsolationData.Builder nsIsolationDataBuilder = NamespaceIsolationData.builder();

        if (namespaces != null) {
            nsIsolationDataBuilder.namespaces(namespaces);
        }

        if (primary != null) {
            nsIsolationDataBuilder.primary(primary);
        }

        if (secondary != null) {
            nsIsolationDataBuilder.secondary(secondary);
        }

        AutoFailoverPolicyType policyType = AutoFailoverPolicyType.fromString(autoFailoverPolicyTypeName);

        nsIsolationDataBuilder.autoFailoverPolicy(AutoFailoverPolicyData.builder()
                .policyType(policyType)
                .parameters(autoFailoverPolicyParams)
                .build());

        // validation if necessary
        if (policyType == AutoFailoverPolicyType.min_available) {
            // ignore
            boolean error = true;
            String[] expectParamKeys = { "min_limit", "usage_threshold" };

            if (autoFailoverPolicyParams.size() == expectParamKeys.length) {
                for (String paramKey : expectParamKeys) {
                    if (!autoFailoverPolicyParams.containsKey(paramKey)) {
                        break;
                    }
                }
                error = false;
            }

            if (error) {
                throw new ParameterException(
                        "Unknown auto failover policy params specified : " + autoFailoverPolicyParams);
            }

        } else {
            // either we don't handle the new type or user has specified a bad type
            throw new ParameterException("Unknown auto failover policy type specified : " + autoFailoverPolicyTypeName);
        }

        return nsIsolationDataBuilder.build();
    }

    public CmdNamespaceIsolationPolicy(Supplier<PulsarAdmin> admin) {
        super("ns-isolation-policy", admin);
        jcommander.addCommand("set", new SetPolicy());
        jcommander.addCommand("get", new GetPolicy());
        jcommander.addCommand("list", new GetAllPolicies());
        jcommander.addCommand("delete", new DeletePolicy());
        jcommander.addCommand("brokers", new GetAllBrokersWithPolicies());
        jcommander.addCommand("broker", new GetBrokerWithPolicies());
    }

}
