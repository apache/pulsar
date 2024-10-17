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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.IOUtils;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToIntegerConverter;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToSecondsConverter;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.Mode;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.BundleType;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TopicType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about namespaces")
public class CmdNamespaces extends CmdBase {
    @Command(description = "Get the namespaces for a tenant")
    private class GetNamespacesPerProperty extends CliCommand {
        @Parameters(description = "tenant-name", arity = "1")
        private String tenant;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().namespaces().getNamespaces(tenant));
        }
    }

    @Command(description = "Get the namespaces for a tenant in a cluster", hidden = true)
    private class GetNamespacesPerCluster extends CliCommand {
        @Parameters(description = "tenant/cluster", arity = "1")
        private String params;

        @Override
        void run() throws PulsarAdminException {
            String[] parts = validatePropertyCluster(params);
            print(getAdmin().namespaces().getNamespaces(parts[0], parts[1]));
        }
    }

    @Command(description = "Get the list of topics for a namespace")
    private class GetTopics extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"-m", "--mode"},
                description = "Allowed topic domain mode (persistent, non_persistent, all).")
        private Mode mode;

        @Option(names = { "-ist",
                "--include-system-topic" }, description = "Include system topic")
        private boolean includeSystemTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            ListNamespaceTopicsOptions options = ListNamespaceTopicsOptions.builder()
                    .mode(mode)
                    .includeSystemTopic(includeSystemTopic)
                    .build();
            print(getAdmin().namespaces().getTopics(namespace, options));
        }
    }

    @Command(description = "Get the list of bundles for a namespace")
    private class GetBundles extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getBundles(namespace));
        }
    }

    @Command(description = "Get the list of destinations for a namespace", hidden = true)
    private class GetDestinations extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getTopics(namespace));
        }
    }

    @Command(description = "Get the configuration policies of a namespace")
    private class GetPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getPolicies(namespace));
        }
    }

    @Command(description = "Creates a new namespace")
    private class Create extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--clusters", "-c" },
                description = "List of clusters this namespace will be assigned", required = false, split = ",")
        private java.util.List<String> clusters;

        @Option(names = { "--bundles", "-b" }, description = "number of bundles to activate", required = false)
        private int numBundles = 0;

        private static final long MAX_BUNDLES = ((long) 1) << 32;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (numBundles < 0 || numBundles > MAX_BUNDLES) {
                throw new ParameterException(
                        "Invalid number of bundles. Number of bundles has to be in the range of (0, 2^32].");
            }

            NamespaceName namespaceName = NamespaceName.get(namespace);
            if (namespaceName.isV2()) {
                Policies policies = new Policies();
                policies.bundles = numBundles > 0 ? BundlesData.builder()
                        .numBundles(numBundles).build() : null;

                if (clusters != null) {
                    policies.replication_clusters = new HashSet<>(clusters);
                }

                getAdmin().namespaces().createNamespace(namespace, policies);
            } else {
                if (numBundles == 0) {
                    getAdmin().namespaces().createNamespace(namespace);
                } else {
                    getAdmin().namespaces().createNamespace(namespace, numBundles);
                }

                if (clusters != null && !clusters.isEmpty()) {
                    getAdmin().namespaces().setNamespaceReplicationClusters(namespace, new HashSet<>(clusters));
                }
            }
        }
    }

    @Command(description = "Deletes a namespace.")
    private class Delete extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-f",
                "--force" }, description = "Delete namespace forcefully by force deleting all topics under it")
        private boolean force = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().deleteNamespace(namespace, force);
        }
    }

    @Command(description = "Grant permissions on a namespace")
    private class GrantPermissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Option(names = "--actions", description = "Actions to be granted (produce,consume,sources,sinks,"
                + "functions,packages)", required = true, split = ",")
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
        }
    }

    @Command(description = "Revoke permissions on a namespace")
    private class RevokePermissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().revokePermissionsOnNamespace(namespace, role);
        }
    }

    @Command(description = "Get permissions to access subscription admin-api")
    private class SubscriptionPermissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getPermissionOnSubscription(namespace));
        }
    }

    @Command(description = "Grant permissions to access subscription admin-api")
    private class GrantSubscriptionPermissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"-s", "--subscription"},
                description = "Subscription name for which permission will be granted to roles", required = true)
        private String subscription;

        @Option(names = {"-rs", "--roles"},
                description = "Client roles to which grant permissions (comma separated roles)",
                required = true, split = ",")
        private List<String> roles;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().grantPermissionOnSubscription(namespace, subscription, Sets.newHashSet(roles));
        }
    }

    @Command(description = "Revoke permissions to access subscription admin-api")
    private class RevokeSubscriptionPermissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"-s", "--subscription"}, description = "Subscription name for which permission "
                + "will be revoked to roles", required = true)
        private String subscription;

        @Option(names = {"-r", "--role"},
                description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().revokePermissionOnSubscription(namespace, subscription, role);
        }
    }

    @Command(description = "Get the permissions on a namespace")
    private class Permissions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getPermissions(namespace));
        }
    }

    @Command(description = "Set replication clusters for a namespace")
    private class SetReplicationClusters extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            getAdmin().namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(clusters));
        }
    }

    @Command(description = "Get replication clusters for a namespace")
    private class GetReplicationClusters extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceReplicationClusters(namespace));
        }
    }

    @Command(description = "Set subscription types enabled for a namespace")
    private class SetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--types", "-t"}, description = "Subscription types enabled list (comma separated values)."
                + " Possible values: (Exclusive, Shared, Failover, Key_Shared).", required = true, split = ",")
        private List<String> subTypes;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            Set<SubscriptionType> types = new HashSet<>();
            subTypes.forEach(s -> {
                SubscriptionType subType;
                try {
                    subType = SubscriptionType.valueOf(s);
                } catch (IllegalArgumentException exception) {
                    throw new ParameterException(String.format("Illegal subscription type %s. Possible values: %s.", s,
                            Arrays.toString(SubscriptionType.values())));
                }
                types.add(subType);
            });
            getAdmin().namespaces().setSubscriptionTypesEnabled(namespace, types);
        }
    }

    @Command(description = "Get subscription types enabled for a namespace")
    private class GetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getSubscriptionTypesEnabled(namespace));
        }
    }

    @Command(description = "Remove subscription types enabled for a namespace")
    private class RemoveSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeSubscriptionTypesEnabled(namespace);
        }
    }

    @Command(description = "Set Message TTL for a namespace")
    private class SetMessageTTL extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--messageTTL", "-ttl"},
                description = "Message TTL in seconds (or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w). "
                        + "When the value is set to `0`, TTL is disabled.", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long messageTTLInSecond;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setNamespaceMessageTTL(namespace, messageTTLInSecond.intValue());
        }
    }

    @Command(description = "Remove Message TTL for a namespace")
    private class RemoveMessageTTL extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeNamespaceMessageTTL(namespace);
        }
    }

    @Command(description = "Get max subscriptions per topic for a namespace")
    private class GetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxSubscriptionsPerTopic(namespace));
        }
    }

    @Command(description = "Set max subscriptions per topic for a namespace")
    private class SetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--max-subscriptions-per-topic", "-m" }, description = "Max subscriptions per topic",
                required = true)
        private int maxSubscriptionsPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxSubscriptionsPerTopic(namespace, maxSubscriptionsPerTopic);
        }
    }

    @Command(description = "Remove max subscriptions per topic for a namespace")
    private class RemoveMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxSubscriptionsPerTopic(namespace);
        }
    }

    @Command(description = "Set subscription expiration time for a namespace")
    private class SetSubscriptionExpirationTime extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-t", "--time" }, description = "Subscription expiration time in minutes", required = true)
        private int expirationTime;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setSubscriptionExpirationTime(namespace, expirationTime);
        }
    }

    @Command(description = "Remove subscription expiration time for a namespace")
    private class RemoveSubscriptionExpirationTime extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeSubscriptionExpirationTime(namespace);
        }
    }

    @Command(description = "Set Anti-affinity group name for a namespace")
    private class SetAntiAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--group", "-g" }, description = "Anti-affinity group name", required = true)
        private String antiAffinityGroup;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setNamespaceAntiAffinityGroup(namespace, antiAffinityGroup);
        }
    }

    @Command(description = "Get Anti-affinity group name for a namespace")
    private class GetAntiAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceAntiAffinityGroup(namespace));
        }
    }

    @Command(description = "Get Anti-affinity namespaces grouped with the given anti-affinity group name")
    private class GetAntiAffinityNamespaces extends CliCommand {

        @Option(names = { "--tenant",
                "-p" }, description = "tenant is only used for authorization. "
                + "Client has to be admin of any of the tenant to access this api", required = false)
        private String tenant;

        @Option(names = { "--cluster", "-c" }, description = "Cluster name", required = true)
        private String cluster;

        @Option(names = { "--group", "-g" }, description = "Anti-affinity group name", required = true)
        private String antiAffinityGroup;

        @Override
        void run() throws PulsarAdminException {
            print(getAdmin().namespaces().getAntiAffinityNamespaces(tenant, cluster, antiAffinityGroup));
        }
    }

    @Command(description = "Remove Anti-affinity group name for a namespace")
    private class DeleteAntiAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().deleteNamespaceAntiAffinityGroup(namespace);
        }
    }

    @Command(description = "Get Deduplication for a namespace")
    private class GetDeduplication extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getDeduplicationStatus(namespace));
        }
    }

    @Command(description = "Remove Deduplication for a namespace")
    private class RemoveDeduplication extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeDeduplicationStatus(namespace);
        }
    }

    @Command(description = "Enable or disable deduplication for a namespace")
    private class SetDeduplication extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getAdmin().namespaces().setDeduplicationStatus(namespace, enable);
        }
    }

    @Command(description = "Enable or disable autoTopicCreation for a namespace, overriding broker settings")
    private class SetAutoTopicCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable allowAutoTopicCreation on namespace")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable allowAutoTopicCreation on namespace")
        private boolean disable = false;

        @Option(names = { "--type", "-t" }, description = "Type of topic to be auto-created. "
                + "Possible values: (partitioned, non-partitioned). Default value: non-partitioned")
        private String type = "non-partitioned";

        @Option(names = { "--num-partitions", "-n" }, description = "Default number of partitions of topic to "
                + "be auto-created, applicable to partitioned topics only", required = false)
        private Integer defaultNumPartitions = null;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            type = type.toLowerCase().trim();

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            if (enable) {
                if (!TopicType.isValidTopicType(type)) {
                    throw new ParameterException("Must specify type of topic to be created. "
                            + "Possible values: (partitioned, non-partitioned)");
                }

                if (TopicType.PARTITIONED.toString().equals(type)
                        && (defaultNumPartitions == null || defaultNumPartitions <= 0)) {
                    throw new ParameterException("Must specify num-partitions or num-partitions > 0 "
                            + "for partitioned topic type.");
                }
            }
            getAdmin().namespaces().setAutoTopicCreation(namespace,
                    AutoTopicCreationOverride.builder()
                            .allowAutoTopicCreation(enable)
                            .topicType(type)
                            .defaultNumPartitions(defaultNumPartitions)
                            .build());
        }
    }

    @Command(description = "Get autoTopicCreation info for a namespace")
    private class GetAutoTopicCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getAutoTopicCreation(namespace));
        }
    }

    @Command(description = "Remove override of autoTopicCreation for a namespace")
    private class RemoveAutoTopicCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            getAdmin().namespaces().removeAutoTopicCreation(namespace);
        }
    }

    @Command(description = "Enable autoSubscriptionCreation for a namespace, overriding broker settings")
    private class SetAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--enable", "-e"}, description = "Enable allowAutoSubscriptionCreation on namespace")
        private boolean enable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setAutoSubscriptionCreation(namespace,
                    AutoSubscriptionCreationOverride.builder()
                            .allowAutoSubscriptionCreation(enable)
                            .build());
        }
    }

    @Command(description = "Get the autoSubscriptionCreation for a namespace")
    private class GetAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getAutoSubscriptionCreation(namespace));
        }
    }

    @Command(description = "Remove override of autoSubscriptionCreation for a namespace")
    private class RemoveAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeAutoSubscriptionCreation(namespace);
        }
    }

    @Command(description = "Remove the retention policy for a namespace")
    private class RemoveRetention extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeRetention(namespace);
        }
    }

    @Command(description = "Set the retention policy for a namespace")
    private class SetRetention extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--time",
                "-t" }, description = "Retention time with optional time unit suffix. "
                        + "For example, 100m, 3h, 2d, 5w. "
                        + "If the time unit is not specified, the default unit is seconds. For example, "
                        + "-t 120 sets retention to 2 minutes. "
                        + "0 means no retention and -1 means infinite time retention.", required = true,
                        converter = TimeUnitToSecondsConverter.class)
        private Long retentionTimeInSec;

        @Option(names = { "--size", "-s" }, description = "Retention size limit with optional size unit suffix. "
                + "For example, 4096, 10M, 16G, 3T.  The size unit suffix character can be k/K, m/M, g/G, or t/T.  "
                + "If the size unit suffix is not specified, the default unit is bytes. "
                + "0 or less than 1MB means no retention and -1 means infinite size retention", required = true,
                converter = ByteUnitToLongConverter.class)
        private Long sizeLimit;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            final int retentionTimeInMin = retentionTimeInSec !=  -1
                    ? (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec)
                    : retentionTimeInSec.intValue();
            final long retentionSizeInMB = sizeLimit != -1
                    ? (sizeLimit / (1024 * 1024))
                    : sizeLimit;
            getAdmin().namespaces()
                    .setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Command(description = "Get the retention policy for a namespace")
    private class GetRetention extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getRetention(namespace));
        }
    }

    @Command(description = "Set the bookie-affinity group name")
    private class SetBookieAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--primary-group",
                "-pg" }, description = "Bookie-affinity primary-groups (comma separated) name "
                + "where namespace messages should be written", required = true)
        private String bookieAffinityGroupNamePrimary;
        @Option(names = { "--secondary-group",
                "-sg" }, description = "Bookie-affinity secondary-group (comma separated) name where namespace "
                + "messages should be written. If you want to verify whether there are enough bookies in groups, "
                + "use `--secondary-group` flag. Messages in this namespace are stored in secondary groups. "
                + "If a group does not contain enough bookies, a topic cannot be created.", required = false)
        private String bookieAffinityGroupNameSecondary;


        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setBookieAffinityGroup(namespace,
                    BookieAffinityGroupData.builder()
                            .bookkeeperAffinityGroupPrimary(bookieAffinityGroupNamePrimary)
                            .bookkeeperAffinityGroupSecondary(bookieAffinityGroupNameSecondary)
                            .build());
        }
    }

    @Command(description = "Set the bookie-affinity group name")
    private class DeleteBookieAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().deleteBookieAffinityGroup(namespace);
        }
    }

    @Command(description = "Get the bookie-affinity group name")
    private class GetBookieAffinityGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getBookieAffinityGroup(namespace));
        }
    }

    @Command(description = "Get message TTL for a namespace")
    private class GetMessageTTL extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceMessageTTL(namespace));
        }
    }

    @Command(description = "Get subscription expiration time for a namespace")
    private class GetSubscriptionExpirationTime extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getSubscriptionExpirationTime(namespace));
        }
    }

    @Command(description = "Unload a namespace from the current serving broker")
    private class Unload extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}")
        private String bundle;

        @Option(names = { "--destinationBroker", "-d" },
                description = "Target brokerWebServiceAddress to which the bundle has to be allocated to. "
                        + "--destinationBroker cannot be set when --bundle is not specified.")
        private String destinationBroker;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);


            if (bundle == null) {
                if (StringUtils.isNotBlank(destinationBroker)) {
                    throw new ParameterException("--destinationBroker cannot be set when --bundle is not specified.");
                }
                getAdmin().namespaces().unload(namespace);
            } else {
                getAdmin().namespaces().unloadNamespaceBundle(namespace, bundle, destinationBroker);
            }
        }
    }

    @Command(description = "Split a namespace-bundle from the current serving broker")
    private class SplitBundle extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--bundle",
                "-b" }, description = "{start-boundary}_{end-boundary} "
                        + "(mutually exclusive with --bundle-type)", required = false)
        private String bundle;

        @Option(names = { "--bundle-type",
        "-bt" }, description = "bundle type (mutually exclusive with --bundle)", required = false)
        private BundleType bundleType;

        @Option(names = { "--unload",
                "-u" }, description = "Unload newly split bundles after splitting old bundle", required = false)
        private boolean unload;

        @Option(names = { "--split-algorithm-name", "-san" }, description = "Algorithm name for split "
                + "namespace bundle. Valid options are: [range_equally_divide, topic_count_equally_divide, "
                + "specified_positions_divide, flow_or_qps_equally_divide]. Use broker side config if absent"
                , required = false)
        private String splitAlgorithmName;

        @Option(names = { "--split-boundaries",
                "-sb" }, description = "Specified split boundary for bundle split, will split one bundle "
                + "to multi bundles only works with specified_positions_divide algorithm", required = false)
        private List<Long> splitBoundaries;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (StringUtils.isBlank(bundle) && bundleType == null) {
                throw new ParameterException("Must pass one of the params: --bundle / --bundle-type");
            }
            if (StringUtils.isNotBlank(bundle) && bundleType != null) {
                throw new ParameterException("--bundle and --bundle-type are mutually exclusive");
            }
            bundle = bundleType != null ? bundleType.toString() : bundle;
            if (splitBoundaries == null || splitBoundaries.size() == 0) {
                getAdmin().namespaces().splitNamespaceBundle(
                        namespace, bundle, unload, splitAlgorithmName);
            } else {
                getAdmin().namespaces().splitNamespaceBundle(
                        namespace, bundle, unload, splitAlgorithmName, splitBoundaries);
            }
        }
    }

    @Command(description = "Get the positions for one or more topic(s) in a namespace bundle")
    private class GetTopicHashPositions extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(
                names = { "--bundle", "-b" },
                description = "{start-boundary}_{end-boundary} format namespace bundle",
                required = false)
        private String bundle;

        @Option(
                names = { "--topic-list",  "-tl" },
                description = "The list of topics(both non-partitioned topic and partitioned topic) to get positions "
                        + "in this bundle, if none topic provided, will get the positions of all topics in this bundle",
                required = false)
        private List<String> topics;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (StringUtils.isBlank(bundle)) {
                throw new ParameterException("Must pass one of the params: --bundle ");
            }
            print(getAdmin().namespaces().getTopicHashPositions(namespace, bundle, topics));
        }
    }

    @Command(description = "Set message-dispatch-rate for all topics of the namespace")
    private class SetDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Option(names = { "--dispatch-rate-period",
                "-dt" }, description = "dispatch-rate-period in second type "
                + "(default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Option(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setDispatchRate(namespace,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove configured message-dispatch-rate for all topics of the namespace")
    private class RemoveDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeDispatchRate(namespace);
        }
    }

    @Command(description = "Get configured message-dispatch-rate for all topics of the namespace "
            + "(Disabled if value < 0)")
    private class GetDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getDispatchRate(namespace));
        }
    }

    @Command(description = "Set subscribe-rate per consumer for all topics of the namespace")
    private class SetSubscribeRate extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)", required = false)
        private int subscribeRate = -1;

        @Option(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type "
                + "(default 30 second will be overwrite if not passed)", required = false)
        private int subscribeRatePeriodSec = 30;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setSubscribeRate(namespace,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Command(description = "Get configured subscribe-rate per consumer for all topics of the namespace")
    private class GetSubscribeRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getSubscribeRate(namespace));
        }
    }

    @Command(description = "Remove configured subscribe-rate per consumer for all topics of the namespace")
    private class RemoveSubscribeRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeSubscribeRate(namespace);
        }
    }


    @Command(description = "Set subscription message-dispatch-rate for all subscription of the namespace")
    private class SetSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Option(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type "
                + "(default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Option(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setSubscriptionDispatchRate(namespace,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove subscription configured message-dispatch-rate "
            + "for all topics of the namespace")
    private class RemoveSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeSubscriptionDispatchRate(namespace);
        }
    }

    @Command(description = "Get subscription configured message-dispatch-rate for all topics of "
            + "the namespace (Disabled if value < 0)")
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getSubscriptionDispatchRate(namespace));
        }
    }

    @Command(description = "Set publish-rate for all topics of the namespace")
    private class SetPublishRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

         @Option(names = { "--msg-publish-rate",
            "-m" }, description = "message-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgPublishRate = -1;

         @Option(names = { "--byte-publish-rate",
            "-b" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private long bytePublishRate = -1;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setPublishRate(namespace,
                new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

    @Command(description = "Remove publish-rate for all topics of the namespace")
    private class RemovePublishRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removePublishRate(namespace);
        }
    }

    @Command(name = "get-publish-rate",
            description = "Get configured message-publish-rate for all topics of the namespace (Disabled if value < 0)")
    private class GetPublishRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getPublishRate(namespace));
        }
    }

    @Command(description = "Set replicator message-dispatch-rate for all topics of the namespace")
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Option(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type "
                + "(default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setReplicatorDispatchRate(namespace,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .build());
        }
    }

    @Command(description = "Get replicator configured message-dispatch-rate for all topics of the namespace "
            + "(Disabled if value < 0)")
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getReplicatorDispatchRate(namespace));
        }
    }

    @Command(description = "Remove replicator configured message-dispatch-rate "
            + "for all topics of the namespace")
    private class RemoveReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeReplicatorDispatchRate(namespace);
        }
    }

    @Command(description = "Get the backlog quota policies for a namespace")
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getBacklogQuotaMap(namespace));
        }
    }

    @Command(description = "Set a backlog quota policy for a namespace")
    private class SetBacklogQuota extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-l", "--limit" }, description = "Size limit (eg: 10M, 16G)",
                converter = ByteUnitToLongConverter.class)
        private Long limit;

        @Option(names = { "-lt", "--limitTime" },
                description = "Time limit in second (or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w), "
                        + "non-positive number for disabling time limit.",
                converter = TimeUnitToSecondsConverter.class)
        private Long limitTimeInSec;

        @Option(names = { "-p", "--policy" }, description = "Retention policy to enforce when the limit is reached. "
                + "Valid options are: [producer_request_hold, producer_exception, consumer_backlog_eviction]",
                required = true)
        private String policyStr;

        @Option(names = {"-t", "--type"}, description = "Backlog quota type to set. Valid options are: "
                + "destination_storage (default) and message_age. "
                + "destination_storage limits backlog by size. "
                + "message_age limits backlog by time, that is, message timestamp (broker or publish timestamp). "
                + "You can set size or time to control the backlog, or combine them together to control the backlog. ")
        private String backlogQuotaTypeStr = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Override
        void run() throws PulsarAdminException {
            BacklogQuota.RetentionPolicy policy;
            BacklogQuota.BacklogQuotaType backlogQuotaType;

            try {
                policy = BacklogQuota.RetentionPolicy.valueOf(policyStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid retention policy type '%s'. Valid options are: %s",
                        policyStr, Arrays.toString(BacklogQuota.RetentionPolicy.values())));
            }

            try {
                backlogQuotaType = BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaTypeStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid backlog quota type '%s'. Valid options are: %s",
                        backlogQuotaTypeStr, Arrays.toString(BacklogQuota.BacklogQuotaType.values())));
            }

            String namespace = validateNamespace(namespaceName);

            BacklogQuota.Builder builder = BacklogQuota.builder().retentionPolicy(policy);
            if (backlogQuotaType == BacklogQuota.BacklogQuotaType.destination_storage) {
                // set quota by storage size
                if (limit == null) {
                    throw new ParameterException("Quota type of 'destination_storage' needs a size limit");
                }
                builder.limitSize(limit);
            } else {
                // set quota by time
                if (limitTimeInSec == null) {
                    throw new ParameterException("Quota type of 'message_age' needs a time limit");
                }
                builder.limitTime(limitTimeInSec.intValue());
            }
            getAdmin().namespaces().setBacklogQuota(namespace, builder.build(), backlogQuotaType);
        }
    }

    @Command(description = "Remove a backlog quota policy from a namespace")
    private class RemoveBacklogQuota extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"-t", "--type"}, description = "Backlog quota type to remove. Valid options are: "
                + "destination_storage, message_age")
        private String backlogQuotaTypeStr = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            BacklogQuota.BacklogQuotaType backlogQuotaType;
            try {
                backlogQuotaType = BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaTypeStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid backlog quota type '%s'. Valid options are: %s",
                        backlogQuotaTypeStr, Arrays.toString(BacklogQuota.BacklogQuotaType.values())));
            }
            getAdmin().namespaces().removeBacklogQuota(namespace, backlogQuotaType);
        }
    }

    @Command(description = "Get the persistence policies for a namespace")
    private class GetPersistence extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getPersistence(namespace));
        }
    }

    @Command(description = "Remove the persistence policies for a namespace")
    private class RemovePersistence extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removePersistence(namespace);
        }
    }

    @Command(description = "Set the persistence policies for a namespace")
    private class SetPersistence extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic")
        private int bookkeeperEnsemble = 2;

        @Option(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry")
        private int bookkeeperWriteQuorum = 2;

        @Option(names = { "-a",
                "--bookkeeper-ack-quorum" },
                description = "Number of acks (guaranteed copies) to wait for each entry")
        private int bookkeeperAckQuorum = 2;

        @Option(names = { "-r",
                "--ml-mark-delete-max-rate" },
                description = "Throttling rate of mark-delete operation (0 means no throttle)")
        private double managedLedgerMaxMarkDeleteRate = 0;

        @Option(names = { "-c",
                "--ml-storage-class" },
                description = "Managed ledger storage class name")
        private String managedLedgerStorageClassName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (bookkeeperEnsemble <= 0 || bookkeeperWriteQuorum <= 0 || bookkeeperAckQuorum <= 0) {
                throw new ParameterException("[--bookkeeper-ensemble], [--bookkeeper-write-quorum] "
                        + "and [--bookkeeper-ack-quorum] must greater than 0.");
            }
            if (managedLedgerMaxMarkDeleteRate < 0) {
                throw new ParameterException("[--ml-mark-delete-max-rate] cannot less than 0.");
            }
            getAdmin().namespaces().setPersistence(namespace, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate,
                    managedLedgerStorageClassName));
        }
    }

    @Command(description = "Clear backlog for a namespace")
    private class ClearBacklog extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--sub", "-s" }, description = "subscription name")
        private String subscription;

        @Option(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}")
        private String bundle;

        @Option(names = { "--force", "-force" }, description = "Whether to force clear backlog without prompt")
        private boolean force;

        @Override
        void run() throws PulsarAdminException, IOException {
            if (!force) {
                String prompt = "Are you sure you want to clear the backlog?";
                boolean confirm = IOUtils.confirmPrompt(prompt);
                if (!confirm) {
                    return;
                }
            }
            String namespace = validateNamespace(namespaceName);
            if (subscription != null && bundle != null) {
                getAdmin().namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription);
            } else if (subscription != null) {
                getAdmin().namespaces().clearNamespaceBacklogForSubscription(namespace, subscription);
            } else if (bundle != null) {
                getAdmin().namespaces().clearNamespaceBundleBacklog(namespace, bundle);
            } else {
                getAdmin().namespaces().clearNamespaceBacklog(namespace);
            }
        }
    }

    @Command(description = "Unsubscribe the given subscription on all topics on a namespace")
    private class Unsubscribe extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--sub", "-s" }, description = "subscription name", required = true)
        private String subscription;

        @Option(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}")
        private String bundle;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            if (bundle != null) {
                getAdmin().namespaces().unsubscribeNamespaceBundle(namespace, bundle, subscription);
            } else {
                getAdmin().namespaces().unsubscribeNamespace(namespace, subscription);
            }
        }

    }

    @Command(description = "Enable or disable message encryption required for a namespace")
    private class SetEncryptionRequired extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable message encryption required")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable message encryption required")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getAdmin().namespaces().setEncryptionRequiredStatus(namespace, enable);
        }
    }

    @Command(description = "Get encryption required for a namespace")
    private class GetEncryptionRequired extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getEncryptionRequiredStatus(namespace));
        }
    }

    @Command(description = "Get the delayed delivery policy for a namespace")
    private class GetDelayedDelivery extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getDelayedDelivery(namespace));
        }
    }

    @Command(description = "Remove delayed delivery policies from a namespace")
    private class RemoveDelayedDelivery extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeDelayedDeliveryMessages(namespace);
        }
    }

    @Command(description = "Get the inactive topic policy for a namespace")
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getInactiveTopicPolicies(namespace));
        }
    }

    @Command(description = "Remove inactive topic policies from a namespace")
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeInactiveTopicPolicies(namespace);
        }
    }

    @Command(description = "Set the inactive topic policies on a namespace")
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable-delete-while-inactive", "-e" }, description = "Enable delete while inactive")
        private boolean enableDeleteWhileInactive = false;

        @Option(names = { "--disable-delete-while-inactive", "-d" }, description = "Disable delete while inactive")
        private boolean disableDeleteWhileInactive = false;

        @Option(names = {"--max-inactive-duration", "-t"}, description = "Max duration of topic inactivity in "
                + "seconds, topics that are inactive for longer than this value will be deleted "
                + "(eg: 1s, 10s, 1m, 5h, 3d)", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long maxInactiveDurationInSeconds;

        @Option(names = { "--delete-mode", "-m" }, description = "Mode of delete inactive topic, Valid options are: "
                + "[delete_when_no_subscriptions, delete_when_subscriptions_caught_up]", required = true)
        private String inactiveTopicDeleteMode;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new ParameterException("Need to specify either enable-delete-while-inactive or "
                        + "disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode = null;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions or "
                        + "delete_when_subscriptions_caught_up");
            }
            getAdmin().namespaces().setInactiveTopicPolicies(namespace, new InactiveTopicPolicies(deleteMode,
                    maxInactiveDurationInSeconds.intValue(), enableDeleteWhileInactive));
        }
    }

    @Command(description = "Set the delayed delivery policy on a namespace")
    private class SetDelayedDelivery extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable delayed delivery messages")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable delayed delivery messages")
        private boolean disable = false;

        @Option(names = { "--time", "-t" }, description = "The tick time for when retrying on "
                + "delayed delivery messages, affecting the accuracy of the delivery time compared to "
                + "the scheduled time. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryTimeInMills = 1000L;

        @Option(names = { "--maxDelay", "-md" },
                description = "The max allowed delay for delayed delivery. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryMaxDelayInMillis = 0L;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            getAdmin().namespaces().setDelayedDeliveryMessages(namespace, DelayedDeliveryPolicies.builder()
                    .tickTime(delayedDeliveryTimeInMills)
                    .active(enable)
                    .maxDeliveryDelayInMillis(delayedDeliveryMaxDelayInMillis)
                    .build());
        }
    }

    @Command(description = "Set subscription auth mode on a namespace")
    private class SetSubscriptionAuthMode extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-m", "--subscription-auth-mode" }, description = "Subscription authorization mode for "
                + "Pulsar policies. Valid options are: [None, Prefix]", required = true)
        private String mode;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.valueOf(mode));
        }
    }

    @Command(description = "Get subscriptionAuthMod for a namespace")
    private class GetSubscriptionAuthMode extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getSubscriptionAuthMode(namespace));
        }
    }

    @Command(description = "Get deduplicationSnapshotInterval for a namespace")
    private class GetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getDeduplicationSnapshotInterval(namespace));
        }
    }

    @Command(description = "Remove deduplicationSnapshotInterval for a namespace")
    private class RemoveDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeDeduplicationSnapshotInterval(namespace);
        }
    }

    @Command(description = "Set deduplicationSnapshotInterval for a namespace")
    private class SetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--interval", "-i"}
                , description = "deduplicationSnapshotInterval for a namespace", required = true)
        private int interval;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setDeduplicationSnapshotInterval(namespace, interval);
        }
    }

    @Command(description = "Get maxProducersPerTopic for a namespace")
    private class GetMaxProducersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxProducersPerTopic(namespace));
        }
    }

    @Command(description = "Remove max producers per topic for a namespace")
    private class RemoveMaxProducersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxProducersPerTopic(namespace);
        }
    }

    @Command(description = "Set maxProducersPerTopic for a namespace")
    private class SetMaxProducersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--max-producers-per-topic", "-p" },
                description = "maxProducersPerTopic for a namespace", required = true)
        private int maxProducersPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxProducersPerTopic(namespace, maxProducersPerTopic);
        }
    }

    @Command(description = "Get maxConsumersPerTopic for a namespace")
    private class GetMaxConsumersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxConsumersPerTopic(namespace));
        }
    }

    @Command(description = "Set maxConsumersPerTopic for a namespace")
    private class SetMaxConsumersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--max-consumers-per-topic", "-c" },
                description = "maxConsumersPerTopic for a namespace", required = true)
        private int maxConsumersPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxConsumersPerTopic(namespace, maxConsumersPerTopic);
        }
    }

    @Command(description = "Remove max consumers per topic for a namespace")
    private class RemoveMaxConsumersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxConsumersPerTopic(namespace);
        }
    }

    @Command(description = "Get maxConsumersPerSubscription for a namespace")
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxConsumersPerSubscription(namespace));
        }
    }

    @Command(description = "Remove maxConsumersPerSubscription for a namespace")
    private class RemoveMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxConsumersPerSubscription(namespace);
        }
    }

    @Command(description = "Set maxConsumersPerSubscription for a namespace")
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--max-consumers-per-subscription", "-c" },
                description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxConsumersPerSubscription(namespace, maxConsumersPerSubscription);
        }
    }

    @Command(description = "Get maxUnackedMessagesPerConsumer for a namespace")
    private class GetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        }
    }

    @Command(description = "Set maxUnackedMessagesPerConsumer for a namespace")
    private class SetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--max-unacked-messages-per-topic", "-c" },
                description = "maxUnackedMessagesPerConsumer for a namespace", required = true)
        private int maxUnackedMessagesPerConsumer;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxUnackedMessagesPerConsumer(namespace, maxUnackedMessagesPerConsumer);
        }
    }

    @Command(description = "Remove maxUnackedMessagesPerConsumer for a namespace")
    private class RemoveMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxUnackedMessagesPerConsumer(namespace);
        }
    }

    @Command(description = "Get maxUnackedMessagesPerSubscription for a namespace")
    private class GetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        }
    }

    @Command(description = "Set maxUnackedMessagesPerSubscription for a namespace")
    private class SetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--max-unacked-messages-per-subscription", "-c"},
                description = "maxUnackedMessagesPerSubscription for a namespace", required = true)
        private int maxUnackedMessagesPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxUnackedMessagesPerSubscription(namespace, maxUnackedMessagesPerSubscription);
        }
    }

    @Command(description = "Remove maxUnackedMessagesPerSubscription for a namespace")
    private class RemoveMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxUnackedMessagesPerSubscription(namespace);
        }
    }

    @Command(description = "Get compactionThreshold for a namespace")
    private class GetCompactionThreshold extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getCompactionThreshold(namespace));
        }
    }

    @Command(description = "Remove compactionThreshold for a namespace")
    private class RemoveCompactionThreshold extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeCompactionThreshold(namespace);
        }
    }

    @Command(description = "Set compactionThreshold for a namespace")
    private class SetCompactionThreshold extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--threshold", "-t" },
                   description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                                 + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
                   required = true,
                    converter = ByteUnitToLongConverter.class)
        private Long threshold = 0L;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setCompactionThreshold(namespace, threshold);
        }
    }

    @Command(description = "Get offloadThreshold for a namespace")
    private class GetOffloadThreshold extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print("offloadThresholdInBytes: " + getAdmin().namespaces().getOffloadThreshold(namespace));
            print("offloadThresholdInSeconds: " + getAdmin().namespaces().getOffloadThresholdInSeconds(namespace));
        }
    }

    @Command(description = "Set offloadThreshold for a namespace")
    private class SetOffloadThreshold extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--size", "-s" },
                   description = "Maximum number of bytes stored in the pulsar cluster for a topic before data will"
                                 + " start being automatically offloaded to longterm storage (eg: 10M, 16G, 3T, 100)."
                                 + " -1 falls back to the cluster's namespace default."
                                 + " Negative values disable automatic offload."
                                 + " 0 triggers offloading as soon as possible.",
                   required = true,
                    converter = ByteUnitToLongConverter.class)
        private Long threshold = -1L;

        @Option(names = {"--time", "-t"},
            description = "Maximum number of seconds stored on the pulsar cluster for a topic"
                + " before the broker will start offloading to longterm storage (eg: 10m, 5h, 3d, 2w).",
            converter = TimeUnitToSecondsConverter.class)
        private Long thresholdInSeconds = -1L;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setOffloadThreshold(namespace, threshold);
            getAdmin().namespaces().setOffloadThresholdInSeconds(namespace, thresholdInSeconds);
        }
    }

    @Command(description = "Get offloadDeletionLag, in minutes, for a namespace")
    private class GetOffloadDeletionLag extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            Long lag = getAdmin().namespaces().getOffloadDeleteLagMs(namespace);
            if (lag != null) {
                System.out.println(TimeUnit.MINUTES.convert(lag, TimeUnit.MILLISECONDS) + " minute(s)");
            } else {
                System.out.println("Unset for namespace. Defaulting to broker setting.");
            }
        }
    }

    @Command(description = "Set offloadDeletionLag for a namespace")
    private class SetOffloadDeletionLag extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--lag", "-l" },
                   description = "Duration to wait after offloading a ledger segment, before deleting the copy of that"
                                  + " segment from cluster local storage. (eg: 10m, 5h, 3d, 2w).",
                   required = true,
                    converter = TimeUnitToSecondsConverter.class)
        private Long lagInSec = -1L;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setOffloadDeleteLag(namespace, lagInSec,
                    TimeUnit.SECONDS);
        }
    }

    @Command(description = "Clear offloadDeletionLag for a namespace")
    private class ClearOffloadDeletionLag extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().clearOffloadDeleteLag(namespace);
        }
    }

    @Command(description = "Get the schema auto-update strategy for a namespace")
    private class GetSchemaAutoUpdateStrategy extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            System.out.println(getAdmin().namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace)
                               .toString().toUpperCase());
        }
    }

    @Command(description = "Set the schema auto-update strategy for a namespace")
    private class SetSchemaAutoUpdateStrategy extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--compatibility", "-c" },
                   description = "Compatibility level required for new schemas created via a Producer. "
                                 + "Possible values (Full, Backward, Forward).")
        private String strategyParam = null;

        @Option(names = { "--disabled", "-d" }, description = "Disable automatic schema updates")
        private boolean disabled = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            SchemaAutoUpdateCompatibilityStrategy strategy = null;
            String strategyStr = strategyParam != null ? strategyParam.toUpperCase() : "";
            if (disabled) {
                strategy = SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled;
            } else if (strategyStr.equals("FULL")) {
                strategy = SchemaAutoUpdateCompatibilityStrategy.Full;
            } else if (strategyStr.equals("BACKWARD")) {
                strategy = SchemaAutoUpdateCompatibilityStrategy.Backward;
            } else if (strategyStr.equals("FORWARD")) {
                strategy = SchemaAutoUpdateCompatibilityStrategy.Forward;
            } else if (strategyStr.equals("NONE")) {
                strategy = SchemaAutoUpdateCompatibilityStrategy.AlwaysCompatible;
            } else {
                throw new ParameterException("Either --compatibility or --disabled must be specified");
            }
            getAdmin().namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace, strategy);
        }
    }

    @Command(description = "Get the schema compatibility strategy for a namespace")
    private class GetSchemaCompatibilityStrategy extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            System.out.println(getAdmin().namespaces().getSchemaCompatibilityStrategy(namespace)
                    .toString().toUpperCase());
        }
    }

    @Command(description = "Set the schema compatibility strategy for a namespace")
    private class SetSchemaCompatibilityStrategy extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--compatibility", "-c" },
                description = "Compatibility level required for new schemas created via a Producer. "
                        + "Possible values (FULL, BACKWARD, FORWARD, "
                        + "UNDEFINED, BACKWARD_TRANSITIVE, "
                        + "FORWARD_TRANSITIVE, FULL_TRANSITIVE, "
                        + "ALWAYS_INCOMPATIBLE,"
                        + "ALWAYS_COMPATIBLE).")
        private String strategyParam = null;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            String strategyStr = strategyParam != null ? strategyParam.toUpperCase() : "";
            SchemaCompatibilityStrategy strategy;
            try {
                strategy = SchemaCompatibilityStrategy.valueOf(strategyStr);
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(String.format("Illegal schema compatibility strategy %s. "
                        + "Possible values: %s", strategyStr, Arrays.toString(SchemaCompatibilityStrategy.values())));
            }
            getAdmin().namespaces().setSchemaCompatibilityStrategy(namespace, strategy);
        }
    }

    @Command(description = "Get the namespace whether allow auto update schema")
    private class GetIsAllowAutoUpdateSchema extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            System.out.println(getAdmin().namespaces().getIsAllowAutoUpdateSchema(namespace));
        }
    }

    @Command(description = "Set the namespace whether allow auto update schema")
    private class SetIsAllowAutoUpdateSchema extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable schema validation enforced")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable schema validation enforced")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getAdmin().namespaces().setIsAllowAutoUpdateSchema(namespace, enable);
        }
    }

    @Command(description = "Get the schema validation enforced")
    private class GetSchemaValidationEnforced extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the namespace")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            System.out.println(getAdmin().namespaces().getSchemaValidationEnforced(namespace, applied));
        }
    }

    @Command(description = "Set the schema whether open schema validation enforced")
    private class SetSchemaValidationEnforced extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--enable", "-e" }, description = "Enable schema validation enforced")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable schema validation enforced")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getAdmin().namespaces().setSchemaValidationEnforced(namespace, enable);
        }
    }

    @Command(description = "Set the offload policies for a namespace")
    private class SetOffloadPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(
                names = {"--driver", "-d"},
                description = "Driver to use to offload old data to long term storage, "
                        + "(Possible values: S3, aws-s3, google-cloud-storage, filesystem, azureblob)",
                required = true)
        private String driver;

        @Option(
                names = {"--region", "-r"},
                description = "The long term storage region, "
                        + "default is s3ManagedLedgerOffloadRegion or gcsManagedLedgerOffloadRegion in broker.conf",
                required = false)
        private String region;

        @Option(
                names = {"--bucket", "-b"},
                description = "Bucket to place offloaded ledger into",
                required = false)
        private String bucket;

        @Option(
                names = {"--endpoint", "-e"},
                description = "Alternative endpoint to connect to, "
                        + "s3 default is s3ManagedLedgerOffloadServiceEndpoint in broker.conf",
                required = false)
        private String endpoint;

        @Option(
                names = {"--aws-id", "-i"},
                description = "AWS Credential Id to use when using driver S3 or aws-s3",
                required = false)
        private String awsId;

        @Option(
                names = {"--aws-secret", "-s"},
                description = "AWS Credential Secret to use when using driver S3 or aws-s3",
                required = false)
        private String awsSecret;

        @Option(
                names = {"--s3-role", "-ro"},
                description = "S3 Role used for STSAssumeRoleSessionCredentialsProvider",
                required = false)
        private String s3Role;

        @Option(
                names = {"--s3-role-session-name", "-rsn"},
                description = "S3 role session name used for STSAssumeRoleSessionCredentialsProvider",
                required = false)
        private String s3RoleSessionName;

        @Option(
                names = {"--maxBlockSize", "-mbs"},
                description = "Max block size (eg: 32M, 64M), default is 64MB"
                  + "s3 and google-cloud-storage requires this parameter",
                required = false,
                converter = ByteUnitToIntegerConverter.class)
        private Integer maxBlockSizeInBytes = OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

        @Option(
                names = {"--readBufferSize", "-rbs"},
                description = "Read buffer size (eg: 1M, 5M), default is 1MB",
                required = false,
                converter = ByteUnitToIntegerConverter.class)
        private Integer readBufferSizeInBytes = OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES;

        @Option(
                names = {"--offloadAfterElapsed", "-oae"},
                description = "Delay time in Millis for deleting the bookkeeper ledger after offload "
                    + "(or seconds,minutes,hours,days,weeks eg: 10s, 100m, 3h, 2d, 5w).",
                required = false,
                converter = TimeUnitToMillisConverter.class)
        private Long offloadAfterElapsedInMillis = OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

        @Option(
                names = {"--offloadAfterThreshold", "-oat"},
                description = "Offload after threshold size (eg: 1M, 5M)",
                required = false,
                converter = ByteUnitToLongConverter.class)
        private Long offloadAfterThresholdInBytes = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;

        @Option(
                names = {"--offloadAfterThresholdInSeconds", "-oats"},
                description = "Offload after threshold seconds (or minutes,hours,days,weeks eg: 100m, 3h, 2d, 5w).",
                required = false,
                converter = TimeUnitToSecondsConverter.class)
        private Long offloadThresholdInSeconds = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS;

        @Option(
                names = {"--offloadedReadPriority", "-orp"},
                description = "Read priority for offloaded messages. By default, once messages are offloaded to "
                        + "long-term storage, brokers read messages from long-term storage, but messages can "
                        + "still exist in BookKeeper for a period depends on your configuration. "
                        + "For messages that exist in both long-term storage and BookKeeper, you can set where to "
                        + "read messages from with the option `tiered-storage-first` or `bookkeeper-first`.",
                required = false
        )
        private String offloadReadPriorityStr;

        public final List<String> driverNames = OffloadPoliciesImpl.DRIVER_NAMES;

        public boolean driverSupported(String driver) {
            return driverNames.stream().anyMatch(d -> d.equalsIgnoreCase(driver));
        }

        public boolean isS3Driver(String driver) {
            if (StringUtils.isEmpty(driver)) {
                return false;
            }
            return driver.equalsIgnoreCase(driverNames.get(0)) || driver.equalsIgnoreCase(driverNames.get(1));
        }

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            if (!driverSupported(driver)) {
                throw new ParameterException("The driver " + driver + " is not supported, "
                        + "(Possible values: " + String.join(",", driverNames) + ").");
            }

            if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
                throw new ParameterException(
                        "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                                + " if s3 offload enabled");
            }

            OffloadedReadPriority offloadedReadPriority = OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY;
            if (this.offloadReadPriorityStr != null) {
                try {
                    offloadedReadPriority = OffloadedReadPriority.fromString(this.offloadReadPriorityStr);
                } catch (Exception e) {
                    throw new ParameterException("--offloadedReadPriority parameter must be one of "
                            + Arrays.stream(OffloadedReadPriority.values())
                            .map(OffloadedReadPriority::toString)
                            .collect(Collectors.joining(","))
                            + " but got: " + this.offloadReadPriorityStr, e);
                }
            }

            OffloadPolicies offloadPolicies = OffloadPoliciesImpl.create(driver, region, bucket, endpoint,
                    s3Role, s3RoleSessionName,
                    awsId, awsSecret,
                    maxBlockSizeInBytes, readBufferSizeInBytes, offloadAfterThresholdInBytes,
                    offloadThresholdInSeconds, offloadAfterElapsedInMillis, offloadedReadPriority);

            getAdmin().namespaces().setOffloadPolicies(namespace, offloadPolicies);
        }
    }

    @Command(description = "Remove the offload policies for a namespace")
    private class RemoveOffloadPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);

            getAdmin().namespaces().removeOffloadPolicies(namespace);
        }
    }


    @Command(description = "Get the offload policies for a namespace")
    private class GetOffloadPolicies extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getOffloadPolicies(namespace));
        }
    }

    @Command(description = "Set max topics per namespace")
    private class SetMaxTopicsPerNamespace extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--max-topics-per-namespace", "-t"},
                description = "max topics per namespace", required = true)
        private int maxTopicsPerNamespace;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setMaxTopicsPerNamespace(namespace, maxTopicsPerNamespace);
        }
    }

    @Command(description = "Get max topics per namespace")
    private class GetMaxTopicsPerNamespace extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getMaxTopicsPerNamespace(namespace));
        }
    }

    @Command(description = "Remove max topics per namespace")
    private class RemoveMaxTopicsPerNamespace extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeMaxTopicsPerNamespace(namespace);
        }
    }

    @Command(description = "Set property for a namespace")
    private class SetPropertyForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--key", "-k"}, description = "Key of the property", required = true)
        private String key;

        @Option(names = {"--value", "-v"}, description = "Value of the property", required = true)
        private String value;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setProperty(namespace, key, value);
        }
    }

    @Command(description = "Set properties of a namespace")
    private class SetPropertiesForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--properties", "-p"}, description = "key value pair properties(a=a,b=b,c=c)",
                required = true)
        private java.util.List<String> properties;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            if (properties.size() == 0) {
                throw new ParameterException(String.format("Required at least one property for the namespace, "
                        + "but found %d.", properties.size()));
            }
            Map<String, String> map = parseListKeyValueMap(properties);
            getAdmin().namespaces().setProperties(namespace, map);
        }
    }

    @Command(description = "Get property for a namespace")
    private class GetPropertyForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--key", "-k"}, description = "Key of the property", required = true)
        private String key;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getProperty(namespace, key));
        }
    }

    @Command(description = "Get properties of a namespace")
    private class GetPropertiesForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws Exception {
            final String namespace = validateNamespace(namespaceName);
            final Map<String, String> properties = getAdmin().namespaces().getProperties(namespace);
            prettyPrint(properties);
        }
    }

    @Command(description = "Remove property for a namespace")
    private class RemovePropertyForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--key", "-k"}, description = "Key of the property", required = true)
        private String key;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().removeProperty(namespace, key));
        }
    }

    @Command(description = "Clear all properties for a namespace")
    private class ClearPropertiesForNamespace extends CliCommand {

        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().clearProperties(namespace);
        }
    }

    @Command(description = "Get ResourceGroup for a namespace")
    private class GetResourceGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceResourceGroup(namespace));
        }
    }

    @Command(description = "Set ResourceGroup for a namespace")
    private class SetResourceGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"--resource-group-name", "-rgn"}, description = "ResourceGroup name", required = true)
        private String rgName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setNamespaceResourceGroup(namespace, rgName);
        }
    }

    @Command(description = "Remove ResourceGroup from a namespace")
    private class RemoveResourceGroup extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeNamespaceResourceGroup(namespace);
        }
    }

    @Command(description = "Update migration state for a namespace")
    private class UpdateMigrationState extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = "--migrated", description = "Is namespace migrated")
        private boolean migrated;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().updateMigrationState(namespace, migrated);
        }
    }

    @Command(description = "Get entry filters for a namespace")
    private class GetEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceEntryFilters(namespace));
        }
    }

    @Command(description = "Set entry filters for a namespace")
    private class SetEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--entry-filters-name", "-efn" },
                description = "The class name for the entry filter.", required = true)
        private String entryFiltersName = "";

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setNamespaceEntryFilters(namespace, new EntryFilters(entryFiltersName));
        }
    }

    @Command(description = "Remove entry filters for a namespace")
    private class RemoveEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeNamespaceEntryFilters(namespace);
        }
    }

    @Command(description = "Enable dispatcherPauseOnAckStatePersistent for a namespace")
    private class SetDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().setDispatcherPauseOnAckStatePersistent(namespace);
        }
    }

    @Command(description = "Get the dispatcherPauseOnAckStatePersistent for a namespace")
    private class GetDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getDispatcherPauseOnAckStatePersistent(namespace));
        }
    }

    @Command(description = "Remove dispatcherPauseOnAckStatePersistent for a namespace")
    private class RemoveDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            getAdmin().namespaces().removeDispatcherPauseOnAckStatePersistent(namespace);
        }
    }

    @Command(description = "Set allowed clusters for a namespace")
    private class SetAllowedClusters extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            getAdmin().namespaces().setNamespaceAllowedClusters(namespace, Sets.newHashSet(clusters));
        }
    }

    @Command(description = "Get allowed clusters for a namespace")
    private class GetAllowedClusters extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getAdmin().namespaces().getNamespaceAllowedClusters(namespace));
        }
    }

    public CmdNamespaces(Supplier<PulsarAdmin> admin) {
        super("namespaces", admin);
        addCommand("list", new GetNamespacesPerProperty());
        addCommand("list-cluster", new GetNamespacesPerCluster());

        addCommand("topics", new GetTopics());
        addCommand("bundles", new GetBundles());
        addCommand("destinations", new GetDestinations());
        addCommand("policies", new GetPolicies());
        addCommand("create", new Create());
        addCommand("delete", new Delete());

        addCommand("permissions", new Permissions());
        addCommand("grant-permission", new GrantPermissions());
        addCommand("revoke-permission", new RevokePermissions());

        addCommand("subscription-permission", new SubscriptionPermissions());
        addCommand("grant-subscription-permission", new GrantSubscriptionPermissions());
        addCommand("revoke-subscription-permission", new RevokeSubscriptionPermissions());

        addCommand("set-clusters", new SetReplicationClusters());
        addCommand("get-clusters", new GetReplicationClusters());

        addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());

        addCommand("set-allowed-clusters", new SetAllowedClusters());
        addCommand("get-allowed-clusters", new GetAllowedClusters());

        addCommand("get-backlog-quotas", new GetBacklogQuotaMap());
        addCommand("set-backlog-quota", new SetBacklogQuota());
        addCommand("remove-backlog-quota", new RemoveBacklogQuota());

        addCommand("get-persistence", new GetPersistence());
        addCommand("set-persistence", new SetPersistence());
        addCommand("remove-persistence", new RemovePersistence());

        addCommand("get-message-ttl", new GetMessageTTL());
        addCommand("set-message-ttl", new SetMessageTTL());
        addCommand("remove-message-ttl", new RemoveMessageTTL());

        addCommand("get-max-subscriptions-per-topic", new GetMaxSubscriptionsPerTopic());
        addCommand("set-max-subscriptions-per-topic", new SetMaxSubscriptionsPerTopic());
        addCommand("remove-max-subscriptions-per-topic", new RemoveMaxSubscriptionsPerTopic());

        addCommand("get-subscription-expiration-time", new GetSubscriptionExpirationTime());
        addCommand("set-subscription-expiration-time", new SetSubscriptionExpirationTime());
        addCommand("remove-subscription-expiration-time", new RemoveSubscriptionExpirationTime());

        addCommand("get-anti-affinity-group", new GetAntiAffinityGroup());
        addCommand("set-anti-affinity-group", new SetAntiAffinityGroup());
        addCommand("get-anti-affinity-namespaces", new GetAntiAffinityNamespaces());
        addCommand("delete-anti-affinity-group", new DeleteAntiAffinityGroup());

        addCommand("set-deduplication", new SetDeduplication());
        addCommand("get-deduplication", new GetDeduplication());
        addCommand("remove-deduplication", new RemoveDeduplication());

        addCommand("set-auto-topic-creation", new SetAutoTopicCreation());
        addCommand("get-auto-topic-creation", new GetAutoTopicCreation());
        addCommand("remove-auto-topic-creation", new RemoveAutoTopicCreation());

        addCommand("set-auto-subscription-creation", new SetAutoSubscriptionCreation());
        addCommand("get-auto-subscription-creation", new GetAutoSubscriptionCreation());
        addCommand("remove-auto-subscription-creation", new RemoveAutoSubscriptionCreation());

        addCommand("get-retention", new GetRetention());
        addCommand("set-retention", new SetRetention());
        addCommand("remove-retention", new RemoveRetention());

        addCommand("set-bookie-affinity-group", new SetBookieAffinityGroup());
        addCommand("get-bookie-affinity-group", new GetBookieAffinityGroup());
        addCommand("delete-bookie-affinity-group", new DeleteBookieAffinityGroup());

        addCommand("unload", new Unload());

        addCommand("split-bundle", new SplitBundle());
        addCommand("get-topic-positions", new GetTopicHashPositions());

        addCommand("set-dispatch-rate", new SetDispatchRate());
        addCommand("remove-dispatch-rate", new RemoveDispatchRate());
        addCommand("get-dispatch-rate", new GetDispatchRate());

        addCommand("set-subscribe-rate", new SetSubscribeRate());
        addCommand("get-subscribe-rate", new GetSubscribeRate());
        addCommand("remove-subscribe-rate", new RemoveSubscribeRate());

        addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());
        addCommand("remove-subscription-dispatch-rate", new RemoveSubscriptionDispatchRate());

        addCommand("set-publish-rate", new SetPublishRate());
        addCommand("get-publish-rate", new GetPublishRate());
        addCommand("remove-publish-rate", new RemovePublishRate());

        addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());
        addCommand("remove-replicator-dispatch-rate", new RemoveReplicatorDispatchRate());

        addCommand("clear-backlog", new ClearBacklog());

        addCommand("unsubscribe", new Unsubscribe());

        addCommand("set-encryption-required", new SetEncryptionRequired());
        addCommand("get-encryption-required", new GetEncryptionRequired());
        addCommand("set-subscription-auth-mode", new SetSubscriptionAuthMode());
        addCommand("get-subscription-auth-mode", new GetSubscriptionAuthMode());

        addCommand("set-delayed-delivery", new SetDelayedDelivery());
        addCommand("get-delayed-delivery", new GetDelayedDelivery());
        addCommand("remove-delayed-delivery", new RemoveDelayedDelivery());

        addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        addCommand("get-max-producers-per-topic", new GetMaxProducersPerTopic());
        addCommand("set-max-producers-per-topic", new SetMaxProducersPerTopic());
        addCommand("remove-max-producers-per-topic", new RemoveMaxProducersPerTopic());

        addCommand("get-max-consumers-per-topic", new GetMaxConsumersPerTopic());
        addCommand("set-max-consumers-per-topic", new SetMaxConsumersPerTopic());
        addCommand("remove-max-consumers-per-topic", new RemoveMaxConsumersPerTopic());

        addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        addCommand("remove-max-consumers-per-subscription", new RemoveMaxConsumersPerSubscription());

        addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesPerSubscription());
        addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesPerSubscription());
        addCommand("remove-max-unacked-messages-per-subscription",
                new RemoveMaxUnackedMessagesPerSubscription());

        addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesPerConsumer());
        addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesPerConsumer());
        addCommand("remove-max-unacked-messages-per-consumer", new RemoveMaxUnackedMessagesPerConsumer());

        addCommand("get-compaction-threshold", new GetCompactionThreshold());
        addCommand("set-compaction-threshold", new SetCompactionThreshold());
        addCommand("remove-compaction-threshold", new RemoveCompactionThreshold());

        addCommand("get-offload-threshold", new GetOffloadThreshold());
        addCommand("set-offload-threshold", new SetOffloadThreshold());

        addCommand("get-offload-deletion-lag", new GetOffloadDeletionLag());
        addCommand("set-offload-deletion-lag", new SetOffloadDeletionLag());
        addCommand("clear-offload-deletion-lag", new ClearOffloadDeletionLag());

        addCommand("get-schema-autoupdate-strategy", new GetSchemaAutoUpdateStrategy());
        addCommand("set-schema-autoupdate-strategy", new SetSchemaAutoUpdateStrategy());

        addCommand("get-schema-compatibility-strategy", new GetSchemaCompatibilityStrategy());
        addCommand("set-schema-compatibility-strategy", new SetSchemaCompatibilityStrategy());

        addCommand("get-is-allow-auto-update-schema", new GetIsAllowAutoUpdateSchema());
        addCommand("set-is-allow-auto-update-schema", new SetIsAllowAutoUpdateSchema());

        addCommand("get-schema-validation-enforce", new GetSchemaValidationEnforced());
        addCommand("set-schema-validation-enforce", new SetSchemaValidationEnforced());

        addCommand("set-offload-policies", new SetOffloadPolicies());
        addCommand("remove-offload-policies", new RemoveOffloadPolicies());
        addCommand("get-offload-policies", new GetOffloadPolicies());

        addCommand("set-deduplication-snapshot-interval", new SetDeduplicationSnapshotInterval());
        addCommand("get-deduplication-snapshot-interval", new GetDeduplicationSnapshotInterval());
        addCommand("remove-deduplication-snapshot-interval", new RemoveDeduplicationSnapshotInterval());

        addCommand("set-max-topics-per-namespace", new SetMaxTopicsPerNamespace());
        addCommand("get-max-topics-per-namespace", new GetMaxTopicsPerNamespace());
        addCommand("remove-max-topics-per-namespace", new RemoveMaxTopicsPerNamespace());

        addCommand("set-property", new SetPropertyForNamespace());
        addCommand("get-property", new GetPropertyForNamespace());
        addCommand("remove-property", new RemovePropertyForNamespace());
        addCommand("set-properties", new SetPropertiesForNamespace());
        addCommand("get-properties", new GetPropertiesForNamespace());
        addCommand("clear-properties", new ClearPropertiesForNamespace());

        addCommand("get-resource-group", new GetResourceGroup());
        addCommand("set-resource-group", new SetResourceGroup());
        addCommand("remove-resource-group", new RemoveResourceGroup());

        addCommand("get-entry-filters", new GetEntryFiltersPerTopic());
        addCommand("set-entry-filters", new SetEntryFiltersPerTopic());
        addCommand("remove-entry-filters", new RemoveEntryFiltersPerTopic());

        addCommand("update-migration-state", new UpdateMigrationState());

        addCommand("set-dispatcher-pause-on-ack-state-persistent",
                new SetDispatcherPauseOnAckStatePersistent());
        addCommand("get-dispatcher-pause-on-ack-state-persistent",
                new GetDispatcherPauseOnAckStatePersistent());
        addCommand("remove-dispatcher-pause-on-ack-state-persistent",
                new RemoveDispatcherPauseOnAckStatePersistent());
    }
}
