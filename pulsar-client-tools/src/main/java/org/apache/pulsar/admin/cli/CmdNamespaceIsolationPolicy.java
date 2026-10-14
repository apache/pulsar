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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about namespace isolation policy")
public class CmdNamespaceIsolationPolicy extends CmdBase {
    @Command(description = "Create/Update a namespace isolation policy for a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class SetPolicy extends CliCommand {
        @Parameters(description = "cluster-name", index = "0", arity = "1")
        private String clusterName;
        @Parameters(description = "policy-name", index = "1", arity = "1")
        private String policyName;

        @Option(names = "--namespaces", description = "comma separated namespaces-regex list",
                required = true, split = ",")
        private List<String> namespaces;

        @Option(names = "--primary", description = "comma separated  primary-broker-regex list. "
                + "In Pulsar, when namespaces (more specifically, namespace bundles) are assigned dynamically to "
                + "brokers, the namespace isolation policy limits the set of brokers that can be used for assignment. "
                + "Before topics are assigned to brokers, you can set the namespace isolation policy with a primary or "
                + "a secondary regex to select desired brokers. If no broker matches the specified regex, you cannot "
                + "create a topic. If there are not enough primary brokers, topics are assigned to secondary brokers. "
                + "If there are not enough secondary brokers, topics are assigned to other brokers which do not have "
                + "any isolation policies.", required = true, split = ",")
        private List<String> primary;

        @Option(names = "--secondary", description = "comma separated secondary-broker-regex list",
                required = false, split = ",")
        private List<String> secondary = new ArrayList<String>(); // optional

        @Option(names = "--auto-failover-policy-type",
                description = "auto failover policy type name ['min_available']", required = true)
        private String autoFailoverPolicyTypeName;

        @Option(names = "--auto-failover-policy-params",
                description = "comma separated name=value auto failover policy parameters",
                required = true, split = ",")
        private Map<String, String> autoFailoverPolicyParams;

        @Option(names = "--unload-scope", description = "configure the type of unload to do -"
                + " ['all_matching', 'none', 'changed'] namespaces. By default, only namespaces whose placement will"
                + " actually change would be unloaded and placed again. You can choose to not unload any namespace"
                + " while setting this new policy by choosing `none` or choose to unload all namespaces matching"
                + " old (if any) and new namespace regex. If you chose 'none', you will need to manually unload the"
                + " namespaces for them to be placed correctly, or wait till some namespaces get load balanced"
                + " automatically based on load shedding configurations.")
        private NamespaceIsolationPolicyUnloadScope unloadScope;

        void run() throws PulsarAdminException {
            // validate and create the POJO
            NamespaceIsolationData namespaceIsolationData = createNamespaceIsolationData(namespaces, primary, secondary,
                    autoFailoverPolicyTypeName, autoFailoverPolicyParams, unloadScope);

            getAdmin().clusters().createNamespaceIsolationPolicy(clusterName, policyName, namespaceIsolationData);
        }
    }

    @Command(description = "List all namespace isolation policies of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class GetAllPolicies extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String clusterName;

        void run() throws PulsarAdminException {
            Map<String, ? extends NamespaceIsolationData> policyMap =
                    getAdmin().clusters().getNamespaceIsolationPolicies(clusterName);

            print(policyMap);
        }
    }

    @Command(description = "List all brokers with namespace-isolation policies attached to it. "
            + "This operation requires Pulsar super-user privileges")
    private class GetAllBrokersWithPolicies extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String clusterName;

        void run() throws PulsarAdminException {
            List<BrokerNamespaceIsolationData> brokers = getAdmin().clusters()
                    .getBrokersWithNamespaceIsolationPolicy(clusterName);
            List<BrokerNamespaceIsolationDataImpl> data = new ArrayList<>();
            brokers.forEach(v -> data.add((BrokerNamespaceIsolationDataImpl) v));
            print(data);
        }
    }

    @Command(description = "Get broker with namespace-isolation policies attached to it. "
            + "This operation requires Pulsar super-user privileges")
    private class GetBrokerWithPolicies extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String clusterName;
        @Option(names = "--broker",
                description = "Broker-name to get namespace-isolation policies attached to it", required = true)
        private String broker;

        void run() throws PulsarAdminException {
            BrokerNamespaceIsolationDataImpl brokerData = (BrokerNamespaceIsolationDataImpl) getAdmin().clusters()
                    .getBrokerWithNamespaceIsolationPolicy(clusterName, broker);

            print(brokerData);
        }
    }

    @Command(description = "Get namespace isolation policy of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class GetPolicy extends CliCommand {
        @Parameters(description = "cluster-name", index = "0", arity = "1")
        private String clusterName;
        @Parameters(description = "policy-name", index = "1", arity = "1")
        private String policyName;

        void run() throws PulsarAdminException {
            NamespaceIsolationDataImpl nsIsolationData = (NamespaceIsolationDataImpl) getAdmin().clusters()
                    .getNamespaceIsolationPolicy(clusterName, policyName);

            print(nsIsolationData);
        }
    }

    @Command(description = "Delete namespace isolation policy of a cluster. "
            + "This operation requires Pulsar super-user privileges")
    private class DeletePolicy extends CliCommand {
        @Parameters(description = "cluster-name", index = "0", arity = "1")
        private String clusterName;
        @Parameters(description = "policy-name", index = "1", arity = "1")
        private String policyName;

        void run() throws PulsarAdminException {
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
                                                                Map<String, String> autoFailoverPolicyParams,
                                                                NamespaceIsolationPolicyUnloadScope unload) {

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

        nsIsolationDataBuilder.unloadScope(unload);

        return nsIsolationDataBuilder.build();
    }

    public CmdNamespaceIsolationPolicy(Supplier<PulsarAdmin> admin) {
        super("ns-isolation-policy", admin);
        addCommand("set", new SetPolicy());
        addCommand("get", new GetPolicy());
        addCommand("list", new GetAllPolicies());
        addCommand("delete", new DeletePolicy());
        addCommand("brokers", new GetAllBrokersWithPolicies());
        addCommand("broker", new GetBrokerWithPolicies());
    }

}
