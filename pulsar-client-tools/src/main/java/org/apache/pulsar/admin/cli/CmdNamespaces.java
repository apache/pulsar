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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations about namespaces")
public class CmdNamespaces extends CmdBase {
    @Parameters(commandDescription = "Get the namespaces for a tenant")
    private class GetNamespacesPerProperty extends CliCommand {
        @Parameter(description = "tenant-name\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String tenant = getOneArgument(params);
            print(admin.namespaces().getNamespaces(tenant));
        }
    }

    @Parameters(commandDescription = "Get the namespaces for a tenant in a cluster", hidden = true)
    private class GetNamespacesPerCluster extends CliCommand {
        @Parameter(description = "tenant/cluster\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String[] parts = validatePropertyCluster(params);
            print(admin.namespaces().getNamespaces(parts[0], parts[1]));
        }
    }

    @Parameters(commandDescription = "Get the list of topics for a namespace")
    private class GetTopics extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getTopics(namespace));
        }
    }

    @Parameters(commandDescription = "Get the list of destinations for a namespace", hidden = true)
    private class GetDestinations extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getTopics(namespace));
        }
    }

    @Parameters(commandDescription = "Get the configuration policies of a namespace")
    private class GetPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getPolicies(namespace));
        }
    }

    @Parameters(commandDescription = "Creates a new namespace")
    private class Create extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--clusters", "-c" }, description = "List of clusters this namespace will be assigned", required = false)
        private java.util.List<String> clusters;

        @Parameter(names = { "--bundles", "-b" }, description = "number of bundles to activate", required = false)
        private int numBundles = 0;

        private static final long MAX_BUNDLES = ((long) 1) << 32;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            if (numBundles < 0 || numBundles > MAX_BUNDLES) {
                throw new ParameterException(
                        "Invalid number of bundles. Number of numbles has to be in the range of (0, 2^32].");
            }

            NamespaceName namespaceName = NamespaceName.get(namespace);
            if (namespaceName.isV2()) {
                Policies policies = new Policies();
                policies.bundles = numBundles > 0 ? new BundlesData(numBundles) : null;

                if (clusters != null) {
                    policies.replication_clusters = new HashSet<>(clusters);
                }

                admin.namespaces().createNamespace(namespace, policies);
            } else {
                if (numBundles == 0) {
                    admin.namespaces().createNamespace(namespace);
                } else {
                    admin.namespaces().createNamespace(namespace, numBundles);
                }

                if (clusters != null && !clusters.isEmpty()) {
                    admin.namespaces().setNamespaceReplicationClusters(namespace, new HashSet<>(clusters));
                }
            }
        }
    }

    @Parameters(commandDescription = "Deletes a namespace. The namespace needs to be empty")
    private class Delete extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().deleteNamespace(namespace);
        }
    }

    @Parameters(commandDescription = "Grant permissions on a namespace")
    private class GrantPermissions extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Parameter(names = "--actions", description = "Actions to be granted (produce,consume)", required = true, splitter = CommaParameterSplitter.class)
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
        }
    }

    @Parameters(commandDescription = "Revoke permissions on a namespace")
    private class RevokePermissions extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().revokePermissionsOnNamespace(namespace, role);
        }
    }

    @Parameters(commandDescription = "Grant permissions to access subscription admin-api")
    private class GrantSubscriptionPermissions extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--subscription", description = "Subscription name for which permission will be granted to roles", required = true)
        private String subscription;

        @Parameter(names = "--roles", description = "Client roles to which grant permissions (comma separated roles)", required = true, splitter = CommaParameterSplitter.class)
        private List<String> roles;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().grantPermissionOnSubscription(namespace, subscription, Sets.newHashSet(roles));
        }
    }

    @Parameters(commandDescription = "Revoke permissions to access subscription admin-api")
    private class RevokeSubscriptionPermissions extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--subscription", description = "Subscription name for which permission will be revoked to roles", required = true)
        private String subscription;

        @Parameter(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().revokePermissionOnSubscription(namespace, subscription, role);
        }
    }

    @Parameters(commandDescription = "Get the permissions on a namespace")
    private class Permissions extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getPermissions(namespace));
        }
    }

    @Parameters(commandDescription = "Set replication clusters for a namespace")
    private class SetReplicationClusters extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(clusters));
        }
    }

    @Parameters(commandDescription = "Get replication clusters for a namespace")
    private class GetReplicationClusters extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getNamespaceReplicationClusters(namespace));
        }
    }

    @Parameters(commandDescription = "Set Message TTL for a namespace")
    private class SetMessageTTL extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--messageTTL", "-ttl" }, description = "Message TTL in seconds", required = true)
        private int messageTTL;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL);
        }
    }

    @Parameters(commandDescription = "Set subscription expiration time for a namespace")
    private class SetSubscriptionExpirationTime extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--time" }, description = "Subscription expiration time in minutes", required = true)
        private int expirationTime;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setSubscriptionExpirationTime(namespace, expirationTime);
        }
    }

    @Parameters(commandDescription = "Set Anti-affinity group name for a namespace")
    private class SetAntiAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--group", "-g" }, description = "Anti-affinity group name", required = true)
        private String antiAffinityGroup;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setNamespaceAntiAffinityGroup(namespace, antiAffinityGroup);
        }
    }

    @Parameters(commandDescription = "Get Anti-affinity group name for a namespace")
    private class GetAntiAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getNamespaceAntiAffinityGroup(namespace));
        }
    }

    @Parameters(commandDescription = "Get Anti-affinity namespaces grouped with the given anti-affinity group name")
    private class GetAntiAffinityNamespaces extends CliCommand {

        @Parameter(names = { "--tenant",
                "-p" }, description = "tenant is only used for authorization. Client has to be admin of any of the tenant to access this api", required = false)
        private String tenant;

        @Parameter(names = { "--cluster", "-c" }, description = "Cluster name", required = true)
        private String cluster;

        @Parameter(names = { "--group", "-g" }, description = "Anti-affinity group name", required = true)
        private String antiAffinityGroup;

        @Override
        void run() throws PulsarAdminException {
            print(admin.namespaces().getAntiAffinityNamespaces(tenant, cluster, antiAffinityGroup));
        }
    }

    @Parameters(commandDescription = "Remove Anti-affinity group name for a namespace")
    private class DeleteAntiAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().deleteNamespaceAntiAffinityGroup(namespace);
        }
    }


    @Parameters(commandDescription = "Enable or disable deduplication for a namespace")
    private class SetDeduplication extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            admin.namespaces().setDeduplicationStatus(namespace, enable);
        }
    }

    @Parameters(commandDescription = "Enable or disable autoTopicCreation for a namespace, overriding broker settings")
    private class SetAutoTopicCreation extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable allowAutoTopicCreation on namespace")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable allowAutoTopicCreation on namespace")
        private boolean disable = false;

        @Parameter(names = { "--type", "-t" }, description = "Type of topic to be auto-created. " +
                "Possible values: (partitioned, non-partitioned). Default value: non-partitioned")
        private String type = "non-partitioned";

        @Parameter(names = { "--num-partitions", "-n" }, description = "Default number of partitions of topic to be auto-created," +
                " applicable to partitioned topics only", required = false)
        private Integer defaultNumPartitions = null;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            type = type.toLowerCase().trim();

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            if (enable) {
                if (!TopicType.isValidTopicType(type)) {
                    throw new ParameterException("Must specify type of topic to be created. " +
                            "Possible values: (partitioned, non-partitioned)");
                }

                if (TopicType.PARTITIONED.toString().equals(type) && !(defaultNumPartitions > 0)) {
                    throw new ParameterException("Must specify num-partitions > 0 for partitioned topic type.");
                }
            }
            admin.namespaces().setAutoTopicCreation(namespace, new AutoTopicCreationOverride(enable, type, defaultNumPartitions));
        }
    }

    @Parameters(commandDescription = "Remove override of autoTopicCreation for a namespace")
    private class RemoveAutoTopicCreation extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            admin.namespaces().removeAutoTopicCreation(namespace);
        }
    }

    @Parameters(commandDescription = "Enable autoSubscriptionCreation for a namespace, overriding broker settings")
    private class SetAutoSubscriptionCreation extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable allowAutoSubscriptionCreation on namespace")
        private boolean enable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setAutoSubscriptionCreation(namespace, new AutoSubscriptionCreationOverride(enable));
        }
    }

    @Parameters(commandDescription = "Remove override of autoSubscriptionCreation for a namespace")
    private class RemoveAutoSubscriptionCreation extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().removeAutoSubscriptionCreation(namespace);
        }
    }

    @Parameters(commandDescription = "Set the retention policy for a namespace")
    private class SetRetention extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--time",
                "-t" }, description = "Retention time in minutes (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w). "
                        + "0 means no retention and -1 means infinite time retention", required = true)
        private String retentionTimeStr;

        @Parameter(names = { "--size", "-s" }, description = "Retention size limit (eg: 10M, 16G, 3T). "
                + "0 or less than 1MB means no retention and -1 means infinite size retention", required = true)
        private String limitStr;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            long sizeLimit = validateSizeString(limitStr);
            long retentionTimeInSec = RelativeTimeUtil.parseRelativeTimeInSeconds(retentionTimeStr);

            final int retentionTimeInMin;
            if (retentionTimeInSec != -1) {
                retentionTimeInMin = (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec);
            } else {
                retentionTimeInMin = -1;
            }

            final int retentionSizeInMB;
            if (sizeLimit != -1) {
                retentionSizeInMB = (int) (sizeLimit / (1024 * 1024));
            } else {
                retentionSizeInMB = -1;
            }
            admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Parameters(commandDescription = "Get the retention policy for a namespace")
    private class GetRetention extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getRetention(namespace));
        }
    }

    @Parameters(commandDescription = "Set the bookie-affinity group name")
    private class SetBookieAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--primary-group",
                "-pg" }, description = "Bookie-affinity primary-groups (comma separated) name where namespace messages should be written", required = true)
        private String bookieAffinityGroupNamePrimary;
        @Parameter(names = { "--secondary-group",
                "-sg" }, description = "Bookie-affinity secondary-group (comma separated) name where namespace messages should be written", required = false)
        private String bookieAffinityGroupNameSecondary;


        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setBookieAffinityGroup(namespace,
                    new BookieAffinityGroupData(bookieAffinityGroupNamePrimary, bookieAffinityGroupNameSecondary));
        }
    }

    @Parameters(commandDescription = "Set the bookie-affinity group name")
    private class DeleteBookieAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().deleteBookieAffinityGroup(namespace);
        }
    }

    @Parameters(commandDescription = "Get the bookie-affinity group name")
    private class GetBookieAffinityGroup extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getBookieAffinityGroup(namespace));
        }
    }

    @Parameters(commandDescription = "Get message TTL for a namespace")
    private class GetMessageTTL extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getNamespaceMessageTTL(namespace));
        }
    }

    @Parameters(commandDescription = "Get subscription expiration time for a namespace")
    private class GetSubscriptionExpirationTime extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getSubscriptionExpirationTime(namespace));
        }
    }

    @Parameters(commandDescription = "Unload a namespace from the current serving broker")
    private class Unload extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}\n")
        private String bundle;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            if (bundle == null) {
                admin.namespaces().unload(namespace);
            } else {
                admin.namespaces().unloadNamespaceBundle(namespace, bundle);
            }
        }
    }

    @Parameters(commandDescription = "Split a namespace-bundle from the current serving broker")
    private class SplitBundle extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}\n", required = true)
        private String bundle;

        @Parameter(names = { "--unload",
                "-u" }, description = "Unload newly split bundles after splitting old bundle", required = false)
        private boolean unload;

        @Parameter(names = { "--split-algorithm-name", "-san" }, description = "Algorithm name for split namespace bundle.\n" +
            " Valid options are: [range_equally_divide, topic_count_equally_divide].\n" +
            " Use broker side config if absent", required = false)
        private String splitAlgorithmName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().splitNamespaceBundle(namespace, bundle, unload, splitAlgorithmName);
        }
    }

    @Parameters(commandDescription = "Set message-dispatch-rate for all topics of the namespace")
    private class SetDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
                "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)\n", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled then broker will apply throttling value to (publish-rate + dispatch rate))\n", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setDispatchRate(namespace,
                    new DispatchRate(msgDispatchRate, byteDispatchRate, dispatchRatePeriodSec, relativeToPublishRate));
        }
    }

    @Parameters(commandDescription = "Get configured message-dispatch-rate for all topics of the namespace (Disabled if value < 0)")
    private class GetDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getDispatchRate(namespace));
        }
    }

    @Parameters(commandDescription = "Set subscribe-rate per consumer for all topics of the namespace")
    private class SetSubscribeRate extends CliCommand {

        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)\n", required = false)
        private int subscribeRate = -1;

        @Parameter(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type (default 30 second will be overwrite if not passed)\n", required = false)
        private int subscribeRatePeriodSec = 30;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setSubscribeRate(namespace,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Parameters(commandDescription = "Get configured subscribe-rate per consumer for all topics of the namespace")
    private class GetSubscribeRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getSubscribeRate(namespace));
        }
    }


    @Parameters(commandDescription = "Set subscription message-dispatch-rate for all subscription of the namespace")
    private class SetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)\n", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled then broker will apply throttling value to (publish-rate + dispatch rate))\n", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setSubscriptionDispatchRate(namespace,
                    new DispatchRate(msgDispatchRate, byteDispatchRate, dispatchRatePeriodSec, relativeToPublishRate));
        }
    }

    @Parameters(commandDescription = "Get subscription configured message-dispatch-rate for all topics of the namespace (Disabled if value < 0)")
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getSubscriptionDispatchRate(namespace));
        }
    }

    @Parameters(commandDescription = "Set publish-rate for all topics of the namespace")
    private class SetPublishRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

         @Parameter(names = { "--msg-publish-rate",
            "-m" }, description = "message-publish-rate (default -1 will be overwrite if not passed)\n", required = false)
        private int msgPublishRate = -1;

         @Parameter(names = { "--byte-publish-rate",
            "-b" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)\n", required = false)
        private long bytePublishRate = -1;

         @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setPublishRate(namespace,
                new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

     @Parameters(commandDescription = "Get configured message-publish-rate for all topics of the namespace (Disabled if value < 0)")
    private class GetPublishRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

         @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getPublishRate(namespace));
        }
    }

    @Parameters(commandDescription = "Set replicator message-dispatch-rate for all topics of the namespace")
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)\n", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)\n", required = false)
        private int dispatchRatePeriodSec = 1;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setReplicatorDispatchRate(namespace,
                new DispatchRate(msgDispatchRate, byteDispatchRate, dispatchRatePeriodSec));
        }
    }

    @Parameters(commandDescription = "Get replicator configured message-dispatch-rate for all topics of the namespace (Disabled if value < 0)")
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getReplicatorDispatchRate(namespace));
        }
    }

    @Parameters(commandDescription = "Get the backlog quota policies for a namespace")
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getBacklogQuotaMap(namespace));
        }
    }

    @Parameters(commandDescription = "Set a backlog quota policy for a namespace")
    private class SetBacklogQuota extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-l", "--limit" }, description = "Size limit (eg: 10M, 16G)", required = true)
        private String limitStr;

        @Parameter(names = { "-p", "--policy" }, description = "Retention policy to enforce when the limit is reached. "
                + "Valid options are: [producer_request_hold, producer_exception, consumer_backlog_eviction]", required = true)
        private String policyStr;

        @Override
        void run() throws PulsarAdminException {
            BacklogQuota.RetentionPolicy policy;
            long limit;

            try {
                policy = BacklogQuota.RetentionPolicy.valueOf(policyStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid retention policy type '%s'. Valid options are: %s",
                        policyStr, Arrays.toString(BacklogQuota.RetentionPolicy.values())));
            }

            limit = validateSizeString(limitStr);

            String namespace = validateNamespace(params);
            admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy));
        }
    }

    @Parameters(commandDescription = "Remove a backlog quota policy from a namespace")
    private class RemoveBacklogQuota extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().removeBacklogQuota(namespace);
        }
    }

    @Parameters(commandDescription = "Get the persistence policies for a namespace")
    private class GetPersistence extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getPersistence(namespace));
        }
    }

    @Parameters(commandDescription = "Set the persistence policies for a namespace")
    private class SetPersistence extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic", required = true)
        private int bookkeeperEnsemble;

        @Parameter(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry", required = true)
        private int bookkeeperWriteQuorum;

        @Parameter(names = { "-a",
                "--bookkeeper-ack-quorum" }, description = "Number of acks (garanteed copies) to wait for each entry", required = true)
        private int bookkeeperAckQuorum;

        @Parameter(names = { "-r",
                "--ml-mark-delete-max-rate" }, description = "Throttling rate of mark-delete operation (0 means no throttle)", required = true)
        private double managedLedgerMaxMarkDeleteRate;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate));
        }
    }

    @Parameters(commandDescription = "Clear backlog for a namespace")
    private class ClearBacklog extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--sub", "-s" }, description = "subscription name")
        private String subscription;

        @Parameter(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}\n")
        private String bundle;

        @Parameter(names = { "--force", "-force" }, description = "Whether to force clear backlog without prompt")
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
            String namespace = validateNamespace(params);
            if (subscription != null && bundle != null) {
                admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription);
            } else if (subscription != null) {
                admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription);
            } else if (bundle != null) {
                admin.namespaces().clearNamespaceBundleBacklog(namespace, bundle);
            } else {
                admin.namespaces().clearNamespaceBacklog(namespace);
            }
        }
    }

    @Parameters(commandDescription = "Unsubscribe the given subscription on all topics on a namespace")
    private class Unsubscribe extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--sub", "-s" }, description = "subscription name", required = true)
        private String subscription;

        @Parameter(names = { "--bundle", "-b" }, description = "{start-boundary}_{end-boundary}\n")
        private String bundle;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(params);
            if (bundle != null) {
                admin.namespaces().unsubscribeNamespaceBundle(namespace, bundle, subscription);
            } else {
                admin.namespaces().unsubscribeNamespace(namespace, subscription);
            }
        }

    }

    @Parameters(commandDescription = "Enable or disable message encryption required for a namespace")
    private class SetEncryptionRequired extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable message encryption required")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable message encryption required")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            admin.namespaces().setEncryptionRequiredStatus(namespace, enable);
        }
    }

    @Parameters(commandDescription = "Get the delayed delivery policy for a namespace")
    private class GetDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getDelayedDelivery(namespace));
        }
    }

    @Parameters(commandDescription = "Get the inactive topic policy for a namespace")
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getInactiveTopicPolicies(namespace));
        }
    }

    @Parameters(commandDescription = "Remove inactive topic policies from a namespace")
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().removeInactiveTopicPolicies(namespace);
        }
    }

    @Parameters(commandDescription = "Set the inactive topic policies on a namespace")
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable-delete-while-inactive", "-e" }, description = "Enable delete while inactive")
        private boolean enableDeleteWhileInactive = false;

        @Parameter(names = { "--disable-delete-while-inactive", "-d" }, description = "Disable delete while inactive")
        private boolean disableDeleteWhileInactive = false;

        @Parameter(names = {"--max-inactive-duration", "-t"}, description = "Max duration of topic inactivity in seconds" +
                ",topics that are inactive for longer than this value will be deleted (eg: 1s, 10s, 1m, 5h, 3d)", required = true)
        private String deleteInactiveTopicsMaxInactiveDuration;

        @Parameter(names = { "--delete-mode", "-m" }, description = "Mode of delete inactive topic" +
                ",Valid options are: [delete_when_no_subscriptions, delete_when_subscriptions_caught_up]", required = true)
        private String inactiveTopicDeleteMode;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            long maxInactiveDurationInSeconds = TimeUnit.SECONDS.toSeconds(RelativeTimeUtil.parseRelativeTimeInSeconds(deleteInactiveTopicsMaxInactiveDuration));

            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new ParameterException("Need to specify either enable-delete-while-inactive or disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode = null;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions or delete_when_subscriptions_caught_up");
            }
            admin.namespaces().setInactiveTopicPolicies(namespace, new InactiveTopicPolicies(deleteMode, (int) maxInactiveDurationInSeconds, enableDeleteWhileInactive));
        }
    }

    @Parameters(commandDescription = "Set the delayed delivery policy on a namespace")
    private class SetDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable delayed delivery messages")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable delayed delivery messages")
        private boolean disable = false;

        @Parameter(names = { "--time", "-t" }, description = "The tick time for when retrying on delayed delivery messages, " +
                "affecting the accuracy of the delivery time compared to the scheduled time. (eg: 1s, 10s, 1m, 5h, 3d)")
        private String delayedDeliveryTimeStr = "1s";

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            long delayedDeliveryTimeInMills = TimeUnit.SECONDS.toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(delayedDeliveryTimeStr));

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            admin.namespaces().setDelayedDeliveryMessages(namespace, new DelayedDeliveryPolicies(delayedDeliveryTimeInMills, enable));
        }
    }

    @Parameters(commandDescription = "Set subscription auth mode on a namespace")
    private class SetSubscriptionAuthMode extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-m", "--subscription-auth-mode" }, description = "Subscription authorization mode for "
                + "Pulsar policies. Valid options are: [None, Prefix]", required = true)
        private String mode;

        @Override
        void run() throws Exception {
            String namespace = validateNamespace(params);
            admin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.valueOf(mode));
        }
    }

    @Parameters(commandDescription = "Get maxProducersPerTopic for a namespace")
    private class GetMaxProducersPerTopic extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getMaxProducersPerTopic(namespace));
        }
    }

    @Parameters(commandDescription = "Set maxProducersPerTopic for a namespace")
    private class SetMaxProducersPerTopic extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-producers-per-topic", "-p" }, description = "maxProducersPerTopic for a namespace", required = true)
        private int maxProducersPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setMaxProducersPerTopic(namespace, maxProducersPerTopic);
        }
    }

    @Parameters(commandDescription = "Get maxConsumersPerTopic for a namespace")
    private class GetMaxConsumersPerTopic extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getMaxConsumersPerTopic(namespace));
        }
    }

    @Parameters(commandDescription = "Set maxConsumersPerTopic for a namespace")
    private class SetMaxConsumersPerTopic extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers-per-topic", "-c" }, description = "maxConsumersPerTopic for a namespace", required = true)
        private int maxConsumersPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setMaxConsumersPerTopic(namespace, maxConsumersPerTopic);
        }
    }

    @Parameters(commandDescription = "Get maxConsumersPerSubscription for a namespace")
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getMaxConsumersPerSubscription(namespace));
        }
    }

    @Parameters(commandDescription = "Set maxConsumersPerSubscription for a namespace")
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers-per-subscription", "-c" }, description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setMaxConsumersPerSubscription(namespace, maxConsumersPerSubscription);
        }
    }

    @Parameters(commandDescription = "Get maxUnackedMessagesPerConsumer for a namespace")
    private class GetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        }
    }

    @Parameters(commandDescription = "Set maxUnackedMessagesPerConsumer for a namespace")
    private class SetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-unacked-messages-per-topic", "-c" }, description = "maxUnackedMessagesPerConsumer for a namespace", required = true)
        private int maxUnackedMessagesPerConsumer;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, maxUnackedMessagesPerConsumer);
        }
    }

    @Parameters(commandDescription = "Get maxUnackedMessagesPerSubscription for a namespace")
    private class GetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        }
    }

    @Parameters(commandDescription = "Set maxUnackedMessagesPerSubscription for a namespace")
    private class SetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-unacked-messages-per-subscription", "-c" }, description = "maxUnackedMessagesPerSubscription for a namespace", required = true)
        private int maxUnackedMessagesPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, maxUnackedMessagesPerSubscription);
        }
    }

    @Parameters(commandDescription = "Get compactionThreshold for a namespace")
    private class GetCompactionThreshold extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getCompactionThreshold(namespace));
        }
    }

    @Parameters(commandDescription = "Set compactionThreshold for a namespace")
    private class SetCompactionThreshold extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--threshold", "-t" },
                   description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                                 + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
                   required = true)
        private String threshold = "0";

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setCompactionThreshold(namespace, validateSizeString(threshold));
        }
    }

    @Parameters(commandDescription = "Get offloadThreshold for a namespace")
    private class GetOffloadThreshold extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getOffloadThreshold(namespace));
        }
    }

    @Parameters(commandDescription = "Set offloadThreshold for a namespace")
    private class SetOffloadThreshold extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--size", "-s" },
                   description = "Maximum number of bytes stored in the pulsar cluster for a topic before data will"
                                 + " start being automatically offloaded to longterm storage (eg: 10M, 16G, 3T, 100)."
                                 + " -1 falls back to the cluster's namespace default."
                                 + " Negative values disable automatic offload."
                                 + " 0 triggers offloading as soon as possible.",
                   required = true)
        private String threshold = "-1";

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setOffloadThreshold(namespace, validateSizeString(threshold));
        }
    }

    @Parameters(commandDescription = "Get offloadDeletionLag, in minutes, for a namespace")
    private class GetOffloadDeletionLag extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            Long lag = admin.namespaces().getOffloadDeleteLagMs(namespace);
            if (lag != null) {
                System.out.println(TimeUnit.MINUTES.convert(lag, TimeUnit.MILLISECONDS) + " minute(s)");
            } else {
                System.out.println("Unset for namespace. Defaulting to broker setting.");
            }
        }
    }

    @Parameters(commandDescription = "Set offloadDeletionLag for a namespace")
    private class SetOffloadDeletionLag extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--lag", "-l" },
                   description = "Duration to wait after offloading a ledger segment, before deleting the copy of that"
                                  + " segment from cluster local storage. (eg: 10m, 5h, 3d, 2w).",
                   required = true)
        private String lag = "-1";

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().setOffloadDeleteLag(namespace, RelativeTimeUtil.parseRelativeTimeInSeconds(lag),
                    TimeUnit.SECONDS);
        }
    }

    @Parameters(commandDescription = "Clear offloadDeletionLag for a namespace")
    private class ClearOffloadDeletionLag extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            admin.namespaces().clearOffloadDeleteLag(namespace);
        }
    }

    @Parameters(commandDescription = "Get the schema auto-update strategy for a namespace", hidden = true)
    private class GetSchemaAutoUpdateStrategy extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            System.out.println(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace)
                               .toString().toUpperCase());
        }
    }

    @Parameters(commandDescription = "Set the schema auto-update strategy for a namespace", hidden = true)
    private class SetSchemaAutoUpdateStrategy extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--compatibility", "-c" },
                   description = "Compatibility level required for new schemas created via a Producer. "
                                 + "Possible values (Full, Backward, Forward).")
        private String strategyParam = null;

        @Parameter(names = { "--disabled", "-d" }, description = "Disable automatic schema updates")
        private boolean disabled = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

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
                throw new PulsarAdminException("Either --compatibility or --disabled must be specified");
            }
            admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace, strategy);
        }
    }

    @Parameters(commandDescription = "Get the schema compatibility strategy for a namespace")
    private class GetSchemaCompatibilityStrategy extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            System.out.println(admin.namespaces().getSchemaCompatibilityStrategy(namespace)
                    .toString().toUpperCase());
        }
    }

    @Parameters(commandDescription = "Set the schema compatibility strategy for a namespace")
    private class SetSchemaCompatibilityStrategy extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--compatibility", "-c" },
                description = "Compatibility level required for new schemas created via a Producer. "
                        + "Possible values (FULL, BACKWARD, FORWARD, " +
                        "UNDEFINED, BACKWARD_TRANSITIVE, " +
                        "FORWARD_TRANSITIVE, FULL_TRANSITIVE, " +
                        "ALWAYS_INCOMPATIBLE," +
                        "ALWAYS_COMPATIBLE).")
        private String strategyParam = null;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            String strategyStr = strategyParam != null ? strategyParam.toUpperCase() : "";
            admin.namespaces().setSchemaCompatibilityStrategy(namespace, SchemaCompatibilityStrategy.valueOf(strategyStr));
        }
    }

    @Parameters(commandDescription = "Get the namespace whether allow auto update schema")
    private class GetIsAllowAutoUpdateSchema extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            System.out.println(admin.namespaces().getIsAllowAutoUpdateSchema(namespace));
        }
    }

    @Parameters(commandDescription = "Set the namespace whether allow auto update schema")
    private class SetIsAllowAutoUpdateSchema extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable schema validation enforced")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable schema validation enforced")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            admin.namespaces().setIsAllowAutoUpdateSchema(namespace, enable);
        }
    }

    @Parameters(commandDescription = "Get the schema validation enforced")
    private class GetSchemaValidationEnforced extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            System.out.println(admin.namespaces().getSchemaValidationEnforced(namespace));
        }
    }

    @Parameters(commandDescription = "Set the schema whether open schema validation enforced")
    private class SetSchemaValidationEnforced extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable schema validation enforced")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable schema validation enforced")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            admin.namespaces().setSchemaValidationEnforced(namespace, enable);
        }
    }

    @Parameters(commandDescription = "Set the offload policies for a namespace")
    private class SetOffloadPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(
                names = {"--driver", "-d"},
                description = "Driver to use to offload old data to long term storage, " +
                        "(Possible values: S3, aws-s3, google-cloud-storage)",
                required = true)
        private String driver;

        @Parameter(
                names = {"--region", "-r"},
                description = "The long term storage region, " +
                        "default is s3ManagedLedgerOffloadRegion or gcsManagedLedgerOffloadRegion in broker.conf",
                required = false)
        private String region;

        @Parameter(
                names = {"--bucket", "-b"},
                description = "Bucket to place offloaded ledger into",
                required = true)
        private String bucket;

        @Parameter(
                names = {"--endpoint", "-e"},
                description = "Alternative endpoint to connect to, " +
                        "s3 default is s3ManagedLedgerOffloadServiceEndpoint in broker.conf",
                required = false)
        private String endpoint;

        @Parameter(
                names = {"--maxBlockSize", "-mbs"},
                description = "Max block size (eg: 32M, 64M), default is 64MB",
                required = false)
        private String maxBlockSizeStr;

        @Parameter(
                names = {"--readBufferSize", "-rbs"},
                description = "Read buffer size (eg: 1M, 5M), default is 1MB",
                required = false)
        private String readBufferSizeStr;

        @Parameter(
                names = {"--offloadAfterElapsed", "-oae"},
                description = "Offload after elapsed in minutes (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w).",
                required = false)
        private String offloadAfterElapsedStr;

        @Parameter(
                names = {"--offloadAfterThreshold", "-oat"},
                description = "Offload after threshold size (eg: 1M, 5M)",
                required = false)
        private String offloadAfterThresholdStr;

        private final String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage"};

        public boolean driverSupported(String driver) {
            return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
        }

        public boolean isS3Driver(String driver) {
            if (StringUtils.isEmpty(driver)) {
                return false;
            }
            return driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1]);
        }

        public boolean positiveCheck(String paramName, long value) {
            if (value <= 0) {
                throw new ParameterException(paramName + " is not be negative or 0!");
            }
            return true;
        }

        public boolean maxValueCheck(String paramName, long value, long maxValue) {
            if (value > maxValue) {
                throw new ParameterException(paramName + " is not bigger than " + maxValue + "!");
            }
            return true;
        }

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);

            if (!driverSupported(driver)) {
                throw new ParameterException(
                        "The driver " + driver + " is not supported, " +
                                "(Possible values: S3, aws-s3, google-cloud-storage).");
            }

            if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
                throw new ParameterException(
                        "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                                + " if s3 offload enabled");
            }

            int maxBlockSizeInBytes = OffloadPolicies.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
            if (StringUtils.isNotEmpty(maxBlockSizeStr)) {
                long maxBlockSize = validateSizeString(maxBlockSizeStr);
                if (positiveCheck("MaxBlockSize", maxBlockSize)
                        && maxValueCheck("MaxBlockSize", maxBlockSize, Integer.MAX_VALUE)) {
                    maxBlockSizeInBytes = new Long(maxBlockSize).intValue();
                }
            }

            int readBufferSizeInBytes = OffloadPolicies.DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
            if (StringUtils.isNotEmpty(readBufferSizeStr) ) {
                long readBufferSize = validateSizeString(readBufferSizeStr);
                if (positiveCheck("ReadBufferSize", readBufferSize)
                        && maxValueCheck("ReadBufferSize", readBufferSize, Integer.MAX_VALUE)) {
                    readBufferSizeInBytes = new Long(readBufferSize).intValue();
                }
            }

            Long offloadAfterElapsedInMillis = OffloadPolicies.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;
            if (StringUtils.isNotEmpty(offloadAfterElapsedStr)) {
                Long offloadAfterElapsed = TimeUnit.SECONDS.toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(offloadAfterElapsedStr));
                if (positiveCheck("OffloadAfterElapsed", offloadAfterElapsed)
                        && maxValueCheck("OffloadAfterElapsed", offloadAfterElapsed, Long.MAX_VALUE)) {
                    offloadAfterElapsedInMillis = new Long(offloadAfterElapsed);
                }
            }

            long offloadAfterThresholdInBytes = OffloadPolicies.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
            if (StringUtils.isNotEmpty(offloadAfterThresholdStr)) {
                long offloadAfterThreshold = validateSizeString(offloadAfterThresholdStr);
                if (positiveCheck("OffloadAfterThreshold", offloadAfterThreshold)
                        && maxValueCheck("OffloadAfterThreshold", offloadAfterThreshold, Long.MAX_VALUE)) {
                    offloadAfterThresholdInBytes = new Long(offloadAfterThreshold);
                }
            }

            OffloadPolicies offloadPolicies = OffloadPolicies.create(driver, region, bucket, endpoint,
                    maxBlockSizeInBytes, readBufferSizeInBytes, offloadAfterThresholdInBytes,
                    offloadAfterElapsedInMillis);
            admin.namespaces().setOffloadPolicies(namespace, offloadPolicies);
        }
    }

    @Parameters(commandDescription = "Get the offload policies for a namespace")
    private class GetOffloadPolicies extends CliCommand {
        @Parameter(description = "tenant/namespace\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(admin.namespaces().getOffloadPolicies(namespace));
        }
    }

    public CmdNamespaces(PulsarAdmin admin) {
        super("namespaces", admin);
        jcommander.addCommand("list", new GetNamespacesPerProperty());
        jcommander.addCommand("list-cluster", new GetNamespacesPerCluster());

        jcommander.addCommand("topics", new GetTopics());
        jcommander.addCommand("destinations", new GetDestinations());
        jcommander.addCommand("policies", new GetPolicies());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("delete", new Delete());

        jcommander.addCommand("permissions", new Permissions());
        jcommander.addCommand("grant-permission", new GrantPermissions());
        jcommander.addCommand("revoke-permission", new RevokePermissions());

        jcommander.addCommand("grant-subscription-permission", new GrantSubscriptionPermissions());
        jcommander.addCommand("revoke-subscription-permission", new RevokeSubscriptionPermissions());

        jcommander.addCommand("set-clusters", new SetReplicationClusters());
        jcommander.addCommand("get-clusters", new GetReplicationClusters());

        jcommander.addCommand("get-backlog-quotas", new GetBacklogQuotaMap());
        jcommander.addCommand("set-backlog-quota", new SetBacklogQuota());
        jcommander.addCommand("remove-backlog-quota", new RemoveBacklogQuota());

        jcommander.addCommand("get-persistence", new GetPersistence());
        jcommander.addCommand("set-persistence", new SetPersistence());

        jcommander.addCommand("get-message-ttl", new GetMessageTTL());
        jcommander.addCommand("set-message-ttl", new SetMessageTTL());

        jcommander.addCommand("get-subscription-expiration-time", new GetSubscriptionExpirationTime());
        jcommander.addCommand("set-subscription-expiration-time", new SetSubscriptionExpirationTime());

        jcommander.addCommand("get-anti-affinity-group", new GetAntiAffinityGroup());
        jcommander.addCommand("set-anti-affinity-group", new SetAntiAffinityGroup());
        jcommander.addCommand("get-anti-affinity-namespaces", new GetAntiAffinityNamespaces());
        jcommander.addCommand("delete-anti-affinity-group", new DeleteAntiAffinityGroup());

        jcommander.addCommand("set-deduplication", new SetDeduplication());

        jcommander.addCommand("set-auto-topic-creation", new SetAutoTopicCreation());
        jcommander.addCommand("remove-auto-topic-creation", new RemoveAutoTopicCreation());

        jcommander.addCommand("set-auto-subscription-creation", new SetAutoSubscriptionCreation());
        jcommander.addCommand("remove-auto-subscription-creation", new RemoveAutoSubscriptionCreation());

        jcommander.addCommand("get-retention", new GetRetention());
        jcommander.addCommand("set-retention", new SetRetention());

        jcommander.addCommand("set-bookie-affinity-group", new SetBookieAffinityGroup());
        jcommander.addCommand("get-bookie-affinity-group", new GetBookieAffinityGroup());
        jcommander.addCommand("delete-bookie-affinity-group", new DeleteBookieAffinityGroup());

        jcommander.addCommand("unload", new Unload());

        jcommander.addCommand("split-bundle", new SplitBundle());

        jcommander.addCommand("set-dispatch-rate", new SetDispatchRate());
        jcommander.addCommand("get-dispatch-rate", new GetDispatchRate());

        jcommander.addCommand("set-subscribe-rate", new SetSubscribeRate());
        jcommander.addCommand("get-subscribe-rate", new GetSubscribeRate());

        jcommander.addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        jcommander.addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());

        jcommander.addCommand("set-publish-rate", new SetPublishRate());
        jcommander.addCommand("get-publish-rate", new GetPublishRate());

        jcommander.addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        jcommander.addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());

        jcommander.addCommand("clear-backlog", new ClearBacklog());

        jcommander.addCommand("unsubscribe", new Unsubscribe());

        jcommander.addCommand("set-encryption-required", new SetEncryptionRequired());
        jcommander.addCommand("set-subscription-auth-mode", new SetSubscriptionAuthMode());

        jcommander.addCommand("set-delayed-delivery", new SetDelayedDelivery());
        jcommander.addCommand("get-delayed-delivery", new GetDelayedDelivery());

        jcommander.addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        jcommander.addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        jcommander.addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        jcommander.addCommand("get-max-producers-per-topic", new GetMaxProducersPerTopic());
        jcommander.addCommand("set-max-producers-per-topic", new SetMaxProducersPerTopic());
        jcommander.addCommand("get-max-consumers-per-topic", new GetMaxConsumersPerTopic());
        jcommander.addCommand("set-max-consumers-per-topic", new SetMaxConsumersPerTopic());
        jcommander.addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        jcommander.addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        jcommander.addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesPerSubscription());
        jcommander.addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesPerSubscription());
        jcommander.addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesPerConsumer());
        jcommander.addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesPerConsumer());

        jcommander.addCommand("get-compaction-threshold", new GetCompactionThreshold());
        jcommander.addCommand("set-compaction-threshold", new SetCompactionThreshold());

        jcommander.addCommand("get-offload-threshold", new GetOffloadThreshold());
        jcommander.addCommand("set-offload-threshold", new SetOffloadThreshold());

        jcommander.addCommand("get-offload-deletion-lag", new GetOffloadDeletionLag());
        jcommander.addCommand("set-offload-deletion-lag", new SetOffloadDeletionLag());
        jcommander.addCommand("clear-offload-deletion-lag", new ClearOffloadDeletionLag());

        jcommander.addCommand("get-schema-autoupdate-strategy", new GetSchemaAutoUpdateStrategy());
        jcommander.addCommand("set-schema-autoupdate-strategy", new SetSchemaAutoUpdateStrategy());

        jcommander.addCommand("get-schema-compatibility-strategy", new GetSchemaCompatibilityStrategy());
        jcommander.addCommand("set-schema-compatibility-strategy", new SetSchemaCompatibilityStrategy());

        jcommander.addCommand("get-is-allow-auto-update-schema", new GetIsAllowAutoUpdateSchema());
        jcommander.addCommand("set-is-allow-auto-update-schema", new SetIsAllowAutoUpdateSchema());

        jcommander.addCommand("get-schema-validation-enforce", new GetSchemaValidationEnforced());
        jcommander.addCommand("set-schema-validation-enforce", new SetSchemaValidationEnforced());

        jcommander.addCommand("set-offload-policies", new SetOffloadPolicies());
        jcommander.addCommand("get-offload-policies", new GetOffloadPolicies());
    }
}
