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

import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Getter;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Getter
@Parameters(commandDescription = "Operations on persistent topics")
public class CmdTopics extends CmdBase {
    private final CmdTopics.PartitionedLookup partitionedLookup;

    public CmdTopics(Supplier<PulsarAdmin> admin) {
        super("topics", admin);
        partitionedLookup = new PartitionedLookup();
        jcommander.addCommand("list", new ListCmd());
        jcommander.addCommand("list-partitioned-topics", new PartitionedTopicListCmd());
        jcommander.addCommand("permissions", new Permissions());
        jcommander.addCommand("grant-permission", new GrantPermissions());
        jcommander.addCommand("revoke-permission", new RevokePermissions());
        jcommander.addCommand("lookup", new Lookup());
        jcommander.addCommand("partitioned-lookup", partitionedLookup);
        jcommander.addCommand("bundle-range", new GetBundleRange());
        jcommander.addCommand("delete", new DeleteCmd());
        jcommander.addCommand("truncate", new TruncateCmd());
        jcommander.addCommand("unload", new UnloadCmd());
        jcommander.addCommand("subscriptions", new ListSubscriptions());
        jcommander.addCommand("unsubscribe", new DeleteSubscription());
        jcommander.addCommand("create-subscription", new CreateSubscription());

        jcommander.addCommand("stats", new GetStats());
        jcommander.addCommand("stats-internal", new GetInternalStats());
        jcommander.addCommand("info-internal", new GetInternalInfo());

        jcommander.addCommand("partitioned-stats", new GetPartitionedStats());
        jcommander.addCommand("partitioned-stats-internal", new GetPartitionedStatsInternal());

        jcommander.addCommand("skip", new Skip());
        jcommander.addCommand("clear-backlog", new ClearBacklog());

        jcommander.addCommand("expire-messages", new ExpireMessages());
        jcommander.addCommand("expire-messages-all-subscriptions", new ExpireMessagesForAllSubscriptions());

        jcommander.addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        jcommander.addCommand("create-missed-partitions", new CreateMissedPartitionsCmd());
        jcommander.addCommand("create", new CreateNonPartitionedCmd());
        jcommander.addCommand("update-partitioned-topic", new UpdatePartitionedCmd());
        jcommander.addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());

        jcommander.addCommand("delete-partitioned-topic", new DeletePartitionedCmd());
        jcommander.addCommand("peek-messages", new PeekMessages());
        jcommander.addCommand("examine-messages", new ExamineMessages());
        jcommander.addCommand("get-message-by-id", new GetMessageById());
        jcommander.addCommand("get-message-id", new GetMessageId());
        jcommander.addCommand("reset-cursor", new ResetCursor());
        jcommander.addCommand("terminate", new Terminate());
        jcommander.addCommand("compact", new Compact());
        jcommander.addCommand("compaction-status", new CompactionStatusCmd());
        jcommander.addCommand("offload", new Offload());
        jcommander.addCommand("offload-status", new OffloadStatusCmd());
        jcommander.addCommand("last-message-id", new GetLastMessageId());
        jcommander.addCommand("get-backlog-quotas", new GetBacklogQuotaMap());
        jcommander.addCommand("set-backlog-quota", new SetBacklogQuota());
        jcommander.addCommand("remove-backlog-quota", new RemoveBacklogQuota());
        jcommander.addCommand("get-message-ttl", new GetMessageTTL());
        jcommander.addCommand("set-message-ttl", new SetMessageTTL());
        jcommander.addCommand("remove-message-ttl", new RemoveMessageTTL());
        jcommander.addCommand("get-retention", new GetRetention());
        jcommander.addCommand("set-retention", new SetRetention());
        jcommander.addCommand("remove-retention", new RemoveRetention());
        //deprecated commands
        jcommander.addCommand("enable-deduplication", new EnableDeduplication());
        jcommander.addCommand("disable-deduplication", new DisableDeduplication());
        jcommander.addCommand("get-deduplication-enabled", new GetDeduplicationStatus());

        jcommander.addCommand("set-deduplication", new SetDeduplicationStatus());
        jcommander.addCommand("get-deduplication", new GetDeduplicationStatus());
        jcommander.addCommand("remove-deduplication", new RemoveDeduplicationStatus());

        jcommander.addCommand("get-deduplication-snapshot-interval", new GetDeduplicationSnapshotInterval());
        jcommander.addCommand("set-deduplication-snapshot-interval", new SetDeduplicationSnapshotInterval());
        jcommander.addCommand("remove-deduplication-snapshot-interval", new RemoveDeduplicationSnapshotInterval());

        jcommander.addCommand("get-delayed-delivery", new GetDelayedDelivery());
        jcommander.addCommand("set-delayed-delivery", new SetDelayedDelivery());
        jcommander.addCommand("remove-delayed-delivery", new RemoveDelayedDelivery());
        jcommander.addCommand("get-persistence", new GetPersistence());
        jcommander.addCommand("set-persistence", new SetPersistence());
        jcommander.addCommand("remove-persistence", new RemovePersistence());
        jcommander.addCommand("get-offload-policies", new GetOffloadPolicies());
        jcommander.addCommand("set-offload-policies", new SetOffloadPolicies());
        jcommander.addCommand("remove-offload-policies", new RemoveOffloadPolicies());

        jcommander.addCommand("get-dispatch-rate", new GetDispatchRate());
        jcommander.addCommand("set-dispatch-rate", new SetDispatchRate());
        jcommander.addCommand("remove-dispatch-rate", new RemoveDispatchRate());

        jcommander.addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());
        jcommander.addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        jcommander.addCommand("remove-subscription-dispatch-rate", new RemoveSubscriptionDispatchRate());

        jcommander.addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());
        jcommander.addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        jcommander.addCommand("remove-replicator-dispatch-rate", new RemoveReplicatorDispatchRate());

        jcommander.addCommand("get-compaction-threshold", new GetCompactionThreshold());
        jcommander.addCommand("set-compaction-threshold", new SetCompactionThreshold());
        jcommander.addCommand("remove-compaction-threshold", new RemoveCompactionThreshold());

        //deprecated commands
        jcommander.addCommand("get-max-unacked-messages-on-consumer", new GetMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("set-max-unacked-messages-on-consumer", new SetMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("remove-max-unacked-messages-on-consumer", new RemoveMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("get-max-unacked-messages-on-subscription", new GetMaxUnackedMessagesOnSubscription());
        jcommander.addCommand("set-max-unacked-messages-on-subscription", new SetMaxUnackedMessagesOnSubscription());
        jcommander.addCommand("remove-max-unacked-messages-on-subscription", new RemoveMaxUnackedMessagesOnSubscription());

        jcommander.addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("remove-max-unacked-messages-per-consumer", new RemoveMaxUnackedMessagesOnConsumer());
        jcommander.addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesOnSubscription());
        jcommander.addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesOnSubscription());
        jcommander.addCommand("remove-max-unacked-messages-per-subscription", new RemoveMaxUnackedMessagesOnSubscription());
        jcommander.addCommand("get-publish-rate", new GetPublishRate());
        jcommander.addCommand("set-publish-rate", new SetPublishRate());
        jcommander.addCommand("remove-publish-rate", new RemovePublishRate());

        jcommander.addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        jcommander.addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        jcommander.addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());

        //deprecated commands
        jcommander.addCommand("get-maxProducers", new GetMaxProducers());
        jcommander.addCommand("set-maxProducers", new SetMaxProducers());
        jcommander.addCommand("remove-maxProducers", new RemoveMaxProducers());

        jcommander.addCommand("get-max-producers", new GetMaxProducers());
        jcommander.addCommand("set-max-producers", new SetMaxProducers());
        jcommander.addCommand("remove-max-producers", new RemoveMaxProducers());

        jcommander.addCommand("get-max-subscriptions", new GetMaxSubscriptionsPerTopic());
        jcommander.addCommand("set-max-subscriptions", new SetMaxSubscriptionsPerTopic());
        jcommander.addCommand("remove-max-subscriptions", new RemoveMaxSubscriptionsPerTopic());

        jcommander.addCommand("get-max-message-size", new GetMaxMessageSize());
        jcommander.addCommand("set-max-message-size", new SetMaxMessageSize());
        jcommander.addCommand("remove-max-message-size", new RemoveMaxMessageSize());

        jcommander.addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        jcommander.addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        jcommander.addCommand("remove-max-consumers-per-subscription", new RemoveMaxConsumersPerSubscription());

        jcommander.addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        jcommander.addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        jcommander.addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        jcommander.addCommand("get-max-consumers", new GetMaxConsumers());
        jcommander.addCommand("set-max-consumers", new SetMaxConsumers());
        jcommander.addCommand("remove-max-consumers", new RemoveMaxConsumers());

        jcommander.addCommand("get-subscribe-rate", new GetSubscribeRate());
        jcommander.addCommand("set-subscribe-rate", new SetSubscribeRate());
        jcommander.addCommand("remove-subscribe-rate", new RemoveSubscribeRate());

        jcommander.addCommand("set-replicated-subscription-status", new SetReplicatedSubscriptionStatus());
        jcommander.addCommand("get-replicated-subscription-status", new GetReplicatedSubscriptionStatus());
        jcommander.addCommand("get-backlog-size", new GetBacklogSizeByMessageId());

        jcommander.addCommand("get-replication-clusters", new GetReplicationClusters());
        jcommander.addCommand("set-replication-clusters", new SetReplicationClusters());
        jcommander.addCommand("remove-replication-clusters", new RemoveReplicationClusters());

        initDeprecatedCommands();
    }

    private void initDeprecatedCommands() {
        IUsageFormatter usageFormatter = jcommander.getUsageFormatter();
        if (usageFormatter instanceof CmdUsageFormatter) {
            CmdUsageFormatter cmdUsageFormatter = (CmdUsageFormatter) usageFormatter;
            cmdUsageFormatter.addDeprecatedCommand("enable-deduplication");
            cmdUsageFormatter.addDeprecatedCommand("disable-deduplication");
            cmdUsageFormatter.addDeprecatedCommand("get-deduplication-enabled");

            cmdUsageFormatter.addDeprecatedCommand("get-max-unacked-messages-on-consumer");
            cmdUsageFormatter.addDeprecatedCommand("remove-max-unacked-messages-on-consumer");
            cmdUsageFormatter.addDeprecatedCommand("set-max-unacked-messages-on-consumer");

            cmdUsageFormatter.addDeprecatedCommand("get-max-unacked-messages-on-subscription");
            cmdUsageFormatter.addDeprecatedCommand("remove-max-unacked-messages-on-subscription");
            cmdUsageFormatter.addDeprecatedCommand("set-max-unacked-messages-on-subscription");

            cmdUsageFormatter.addDeprecatedCommand("get-maxProducers");
            cmdUsageFormatter.addDeprecatedCommand("set-maxProducers");
            cmdUsageFormatter.addDeprecatedCommand("remove-maxProducers");
        }
    }

    @Parameters(commandDescription = "Get the list of topics under a namespace.")
    private class ListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-td", "--topic-domain"}, description = "Allowed topic domain (persistent, non_persistent).")
        private TopicDomain topicDomain;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getTopics().getList(namespace, topicDomain));
        }
    }

    @Parameters(commandDescription = "Get the list of partitioned topics under a namespace.")
    private class PartitionedTopicListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getTopics().getPartitionedTopicList(namespace));
        }
    }

    @Parameters(commandDescription = "Grant a new permission to a client role on a single topic.")
    private class GrantPermissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Parameter(names = "--actions", description = "Actions to be granted (produce,consume,sources,sinks," +
                "functions,packages)", required = true)
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().grantPermission(topic, role, getAuthActions(actions));
        }
    }

    @Parameters(commandDescription = "Revoke permissions on a topic. "
            + "Revoke permissions to a client role on a single topic. If the permission "
            + "was not set at the topic level, but rather at the namespace level, this "
            + "operation will return an error (HTTP status code 412).")
    private class RevokePermissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().revokePermissions(topic, role);
        }
    }

    @Parameters(commandDescription = "Get the permissions on a topic. "
            + "Retrieve the effective permissions for a topic. These permissions are defined "
            + "by the permissions set at the namespace level combined (union) with any eventual "
            + "specific permission set on the topic.")
    private class Permissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getTopics().getPermissions(topic));
        }
    }

    @Parameters(commandDescription = "Lookup a topic from the current serving broker")
    private class Lookup extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getAdmin().lookups().lookupTopic(topic));
        }
    }

    @Parameters(commandDescription = "Lookup a partitioned topic from the current serving broker")
    protected class PartitionedLookup extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/partitionedTopic", required = true)
        protected java.util.List<String> params;
        @Parameter(names = { "-s",
                                "--sort-by-broker" }, description = "Sort partitioned-topic by Broker Url")
        protected boolean sortByBroker = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            if (sortByBroker) {
                print(lookupPartitionedTopicSortByBroker(topic));
            } else {
                print(getAdmin().lookups().lookupPartitionedTopic(topic));
            }
        }
    }

    private Map<String, List<String>> lookupPartitionedTopicSortByBroker(String topic) throws PulsarAdminException {
        Map<String, String> partitionLookup = getAdmin().lookups().lookupPartitionedTopic(topic);
        Map<String, List<String>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionLookup.entrySet()) {
            List<String> topics = result.getOrDefault(entry.getValue(), new ArrayList<String>());
            topics.add(entry.getKey());
            result.put(entry.getValue(), topics);
        }
        return result;
    }

    @Parameters(commandDescription = "Get Namespace bundle range of a topic")
    private class GetBundleRange extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getAdmin().lookups().getBundleRange(topic));
        }
    }

    @Parameters(commandDescription = "Create a partitioned topic. "
            + "The partitioned topic has to be created before creating a producer on it.")
    private class CreatePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            getTopics().createPartitionedTopic(topic, numPartitions);
        }
    }

    @Parameters(commandDescription = "Try to create partitions for partitioned topic. "
            + "The partitions of partition topic has to be created, can be used by repair partitions when "
            + "topic auto creation is disabled")
    private class CreateMissedPartitionsCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            getTopics().createMissedPartitions(topic);
        }
    }

    @Parameters(commandDescription = "Create a non-partitioned topic.")
    private class CreateNonPartitionedCmd extends CliCommand {

    	@Parameter(description = "persistent://tenant/namespace/topic", required = true)
    	private java.util.List<String> params;

    	@Override
    	void run() throws Exception {
    		String topic = validateTopicName(params);
    		getTopics().createNonPartitionedTopic(topic);
    	}
    }

    @Parameters(commandDescription = "Update existing non-global partitioned topic. "
            + "New updating number of partitions must be greater than existing number of partitions.")
    private class UpdatePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Parameter(names = { "-f",
                "--force" }, description = "Update forcefully without validating existing partitioned topic ", required = false)
        private boolean force;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            getTopics().updatePartitionedTopic(topic, numPartitions, false, force);
        }
    }

    @Parameters(commandDescription = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(getTopics().getPartitionedTopicMetadata(topic));
        }
    }

    @Parameters(commandDescription = "Delete a partitioned topic. "
            + "It will also delete all the partitions of the topic if it exists.")
    private class DeletePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Parameter(names = { "-d",
                "--deleteSchema" }, description = "Delete schema while deleting topic")
        private boolean deleteSchema = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            getTopics().deletePartitionedTopic(topic, force, deleteSchema);
        }
    }

    @Parameters(commandDescription = "Delete a topic. "
            + "The topic cannot be deleted if there's any active subscription or producers connected to it.")
    private class DeleteCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Parameter(names = { "-d",
                "--deleteSchema" }, description = "Delete schema while deleting topic")
        private boolean deleteSchema = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().delete(topic, force, deleteSchema);
        }
    }

    @Parameters(commandDescription = "Truncate a topic. \n"
            + "\t\tThe truncate operation will move all cursors to the end of the topic and delete all inactive ledgers. ")
    private class TruncateCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().truncate(topic);
        }
    }

    @Parameters(commandDescription = "Unload a topic.")
    private class UnloadCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().unload(topic);
        }
    }

    @Parameters(commandDescription = "Get the list of subscriptions on the topic")
    private class ListSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(getTopics().getSubscriptions(topic));
        }
    }

    @Parameters(commandDescription = "Delete a durable subscriber from a topic. "
            + "The subscription cannot be deleted if there are any active consumers attached to it")
    private class DeleteSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f",
            "--force" }, description = "Disconnect and close all consumers and delete subscription forcefully")
        private boolean force = false;

        @Parameter(names = { "-s", "--subscription" }, description = "Subscription to be deleted", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().deleteSubscription(topic, subName, force);
        }
    }

    @Parameters(commandDescription = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Parameter(names = { "-sbs",
                "--get-subscription-backlog-size" }, description = "Set true to get backlog size for each subscription"
        + ", locking required.")
        private boolean subscriptionBacklogSize = false;

        @Parameter(names = { "-etb",
                "--get-earliest-time-in-backlog" }, description = "Set true to get earliest time in backlog")
        private boolean getEarliestTimeInBacklog = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getTopics().getStats(topic, getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog));
        }
    }

    @Parameters(commandDescription = "Get the internal stats for the topic")
    private class GetInternalStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-m",
        "--metadata" }, description = "Flag to include ledger metadata")
        private boolean metadata = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getTopics().getInternalStats(topic, metadata));
        }
    }

    @Parameters(commandDescription = "Get the internal metadata info for the topic")
    private class GetInternalInfo extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            String internalInfo = getTopics().getInternalInfo(topic);
            JsonObject result = JsonParser.parseString(internalInfo).getAsJsonObject();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(result));
        }
    }

    @Parameters(commandDescription = "Get the stats for the partitioned topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetPartitionedStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--per-partition", description = "Get per partition stats")
        private boolean perPartition = false;

        @Parameter(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Parameter(names = { "-sbs",
                "--get-subscription-backlog-size" }, description = "Set true to get backlog size for each subscription"
                + ", locking required.")
        private boolean subscriptionBacklogSize = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(getTopics().getPartitionedStats(topic, perPartition, getPreciseBacklog, subscriptionBacklogSize));
        }
    }

    @Parameters(commandDescription = "Get the internal stats for the partitioned topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetPartitionedStatsInternal extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            print(getTopics().getPartitionedInternalStats(topic));
        }
    }

    @Parameters(commandDescription = "Skip all the messages for the subscription")
    private class ClearBacklog extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s", "--subscription" }, description = "Subscription to be cleared", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().skipAllMessages(topic, subName);
        }
    }

    @Parameters(commandDescription = "Skip some messages for the subscription")
    private class Skip extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Parameter(names = { "-n", "--count" }, description = "Number of messages to skip", required = true)
        private long numMessages;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().skipMessages(topic, subName, numMessages);
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) for the subscription")
    private class ExpireMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Parameter(names = { "-t", "--expireTime" }, description = "Expire messages older than time in seconds")
        private long expireTimeInSeconds = -1;

        @Parameter(names = { "--position",
                "-p" }, description = "message position to reset back to (ledgerId:entryId)", required = false)
        private String messagePosition;

        @Parameter(names = { "-e", "--exclude-reset-position" },
                description = "Exclude the reset position, start consume messages from the next position.", required = false)
        private boolean excludeResetPosition = false;

        @Override
        void run() throws PulsarAdminException {
            if (expireTimeInSeconds >= 0 && isNotBlank(messagePosition)) {
                throw new ParameterException(String.format("Can't expire message by time and " +
                        "by message position at the same time."));
            }
            String topic = validateTopicName(params);
            if (expireTimeInSeconds >= 0) {
                getTopics().expireMessages(topic, subName, expireTimeInSeconds);
            } else if (isNotBlank(messagePosition)) {
                int partitionIndex = TopicName.get(topic).getPartitionIndex();
                MessageId messageId = validateMessageIdString(messagePosition, partitionIndex);
                getTopics().expireMessages(topic, subName, messageId, excludeResetPosition);
            } else {
                throw new ParameterException(
                        "Either time (--expireTime) or message position (--position) has to be provided" +
                                " to expire messages");
            }
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) for all subscriptions")
    private class ExpireMessagesForAllSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--expireTime" }, description = "Expire messages older than time in seconds", required = true)
        private long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getTopics().expireMessagesForAllSubscriptions(topic, expireTimeInSeconds);
        }
    }

    @Parameters(commandDescription = "Create a new subscription on a topic")
    private class CreateSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subscriptionName;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId where to create the subscription. It can be either 'latest', 'earliest' or (ledgerId:entryId)", required = false)
        private String messageIdStr = "latest";

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            MessageId messageId;
            if (messageIdStr.equals("latest")) {
                messageId = MessageId.latest;
            } else if (messageIdStr.equals("earliest")) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messageIdStr);
            }

            getTopics().createSubscription(topic, subscriptionName, messageId);
        }
    }

    @Parameters(commandDescription = "Reset position for subscription to a position that is closest to timestamp or messageId.")
    private class ResetCursor extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subName;

        @Parameter(names = { "--time",
                "-t" }, description = "time in minutes to reset back to (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w)", required = false)
        private String resetTimeStr;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId to reset back to ('latest', 'earliest', or 'ledgerId:entryId')", required =
                false)
        private String resetMessageIdStr;

        @Parameter(names = { "-e", "--exclude-reset-position" },
                description = "Exclude the reset position, start consume messages from the next position.", required = false)
        private boolean excludeResetPosition = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            if (isNotBlank(resetMessageIdStr)) {
                MessageId messageId;
                if ("earliest".equals(resetMessageIdStr)) {
                    messageId = MessageId.earliest;
                } else if ("latest".equals(resetMessageIdStr)) {
                    messageId = MessageId.latest;
                } else {
                    messageId = validateMessageIdString(resetMessageIdStr);
                }
                if (excludeResetPosition) {
                    getTopics().resetCursor(persistentTopic, subName, messageId, true);
                } else {
                    getTopics().resetCursor(persistentTopic, subName, messageId);
                }
            } else if (isNotBlank(resetTimeStr)) {
                long resetTimeInMillis;
                try {
                    resetTimeInMillis = TimeUnit.SECONDS.toMillis(
                            RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));
                } catch (IllegalArgumentException exception) {
                    throw new ParameterException(exception.getMessage());
                }
                // now - go back time
                long timestamp = System.currentTimeMillis() - resetTimeInMillis;
                getTopics().resetCursor(persistentTopic, subName, timestamp);
            } else {
                throw new PulsarAdminException(
                        "Either Timestamp (--time) or messageId (--messageId) has to be provided to reset cursor");
            }
        }
    }

    @Parameters(commandDescription = "Terminate a topic and don't allow any more messages to be published")
    private class Terminate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                MessageId lastMessageId = getTopics().terminateTopicAsync(persistentTopic).get();
                System.out.println("Topic successfully terminated at " + lastMessageId);
            } catch (InterruptedException | ExecutionException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    @Parameters(commandDescription = "Peek some messages for the subscription")
    private class PeekMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to get messages from", required = true)
        private String subName;

        @Parameter(names = { "-n", "--count" }, description = "Number of messages (default 1)", required = false)
        private int numMessages = 1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            List<Message<byte[]>> messages = getTopics().peekMessages(persistentTopic, subName, numMessages);
            int position = 0;
            for (Message<byte[]> msg : messages) {
                MessageImpl message = (MessageImpl) msg;
                if (++position != 1) {
                    System.out.println("-------------------------------------------------------------------------\n");
                }
                if (message.getMessageId() instanceof BatchMessageIdImpl) {
                    BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                    System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":" + msgId.getBatchIndex());
                } else {
                    MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
                    System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
                }

                System.out.println("Publish time: " + message.getPublishTime());
                System.out.println("Event time: " + message.getEventTime());

                if (message.getDeliverAtTime() != 0) {
                    System.out.println("Deliver at time: " + message.getDeliverAtTime());
                }

                if (message.getBrokerEntryMetadata() != null) {
                    if (message.getBrokerEntryMetadata().hasBrokerTimestamp()) {
                        System.out.println("Broker entry metadata timestamp: " + message.getBrokerEntryMetadata().getBrokerTimestamp());
                    }
                    if (message.getBrokerEntryMetadata().hasIndex()) {
                        System.out.println("Broker entry metadata index: " + message.getBrokerEntryMetadata().getIndex());
                    }
                }

                if (message.getProperties().size() > 0) {
                    System.out.println("Properties:");
                    print(msg.getProperties());
                }
                ByteBuf data = Unpooled.wrappedBuffer(msg.getData());
                System.out.println(ByteBufUtil.prettyHexDump(data));
            }
        }
    }


    @Parameters(commandDescription = "Examine a specific message on a topic by position relative to the" +
            " earliest or the latest message.")
    private class ExamineMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-i", "--initialPosition" },
                description = "Relative start position to examine message." +
                        "It can be 'latest' or 'earliest', default is latest")
        private String initialPosition = "latest";

        @Parameter(names = { "-m", "--messagePosition" },
                description = "The position of messages (default 1)", required = false)
        private long messagePosition = 1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            MessageImpl message =
                    (MessageImpl) getTopics().examineMessage(persistentTopic, initialPosition, messagePosition);

            if (message.getMessageId() instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":" + msgId.getBatchIndex());
            } else {
                MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
                System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
            }

            System.out.println("Publish time: " + message.getPublishTime());
            System.out.println("Event time: " + message.getEventTime());

            if (message.getDeliverAtTime() != 0) {
                System.out.println("Deliver at time: " + message.getDeliverAtTime());
            }

            if (message.getBrokerEntryMetadata() != null) {
                if (message.getBrokerEntryMetadata().hasBrokerTimestamp()) {
                    System.out.println("Broker entry metadata timestamp: " + message.getBrokerEntryMetadata().getBrokerTimestamp());
                }
                if (message.getBrokerEntryMetadata().hasIndex()) {
                    System.out.println("Broker entry metadata index: " + message.getBrokerEntryMetadata().getIndex());
                }
            }

            if (message.getProperties().size() > 0) {
                System.out.println("Properties:");
                print(message.getProperties());
            }
            ByteBuf data = Unpooled.wrappedBuffer(message.getData());
            System.out.println(ByteBufUtil.prettyHexDump(data));
        }
    }

    @Parameters(commandDescription = "Get message by its ledgerId and entryId")
    private class GetMessageById extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-l", "--ledgerId" },
            description = "ledger id pointing to the desired ledger",
            required = true)
        private long ledgerId;

        @Parameter(names = { "-e", "--entryId" },
            description = "entry id pointing to the desired entry",
            required = true)
        private long entryId;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            MessageImpl message = (MessageImpl) getTopics().getMessageById(persistentTopic, ledgerId, entryId);
            if (message == null) {
                System.out.println("Cannot find any messages based on ledgerId:"
                        + ledgerId + " entryId:" + entryId);
            } else {
                if (message.getMessageId() instanceof BatchMessageIdImpl) {
                    BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                    System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":" + msgId.getBatchIndex());
                } else {
                    MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
                    System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
                }

                System.out.println("Publish time: " + message.getPublishTime());
                System.out.println("Event time: " + message.getEventTime());

                if (message.getDeliverAtTime() != 0) {
                    System.out.println("Deliver at time: " + message.getDeliverAtTime());
                }

                if (message.getBrokerEntryMetadata() != null) {
                    if (message.getBrokerEntryMetadata().hasBrokerTimestamp()) {
                        System.out.println("Broker entry metadata timestamp: " + message.getBrokerEntryMetadata().getBrokerTimestamp());
                    }
                    if (message.getBrokerEntryMetadata().hasIndex()) {
                        System.out.println("Broker entry metadata index: " + message.getBrokerEntryMetadata().getIndex());
                    }
                }

                if (message.getProperties().size() > 0) {
                    System.out.println("Properties:");
                    print(message.getProperties());
                }
                ByteBuf date = Unpooled.wrappedBuffer(message.getData());
                System.out.println(ByteBufUtil.prettyHexDump(date));
            }
        }
    }

    @Parameters(commandDescription = "Get message ID")
    private class GetMessageId extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-d", "--datetime" },
                description = "datetime at or before this messageId. This datetime is in format of ISO_OFFSET_DATE_TIME,"
                        + " e.g. 2021-06-28T16:53:08Z or 2021-06-28T16:53:08.123456789+08:00",
                required = true)
        private String datetime;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            long timestamp = DateFormatter.parse(datetime);
            MessageId messageId = getTopics().getMessageIdByTimestamp(persistentTopic, timestamp);
            if (messageId == null) {
                System.out.println("Cannot find any messages based on timestamp " + timestamp);
            } else {
                print(messageId);
            }
        }
    }

    @Parameters(commandDescription = "Compact a topic")
    private class Compact extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            getTopics().triggerCompaction(persistentTopic);
            System.out.println("Topic compaction requested for " + persistentTopic);
        }
    }

    @Parameters(commandDescription = "Status of compaction on a topic")
    private class CompactionStatusCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-w", "--wait-complete" },
                   description = "Wait for compaction to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                LongRunningProcessStatus status = getTopics().compactionStatus(persistentTopic);
                while (wait && status.status == LongRunningProcessStatus.Status.RUNNING) {
                    Thread.sleep(1000);
                    status = getTopics().compactionStatus(persistentTopic);
                }

                switch (status.status) {
                case NOT_RUN:
                    System.out.println("Compaction has not been run for " + persistentTopic
                                       + " since broker startup");
                    break;
                case RUNNING:
                    System.out.println("Compaction is currently running");
                    break;
                case SUCCESS:
                    System.out.println("Compaction was a success");
                    break;
                case ERROR:
                    System.out.println("Error in compaction");
                    throw new PulsarAdminException("Error compacting: " + status.lastError);
                }
            } catch (InterruptedException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    static MessageId findFirstLedgerWithinThreshold(List<PersistentTopicInternalStats.LedgerInfo> ledgers,
                                                    long sizeThreshold) {
        long suffixSize = 0L;

        ledgers = Lists.reverse(ledgers);
        long previousLedger = ledgers.get(0).ledgerId;
        for (PersistentTopicInternalStats.LedgerInfo l : ledgers) {
            suffixSize += l.size;
            if (suffixSize > sizeThreshold) {
                return new MessageIdImpl(previousLedger, 0L, -1);
            }
            previousLedger = l.ledgerId;
        }
        return null;
    }

    @Parameters(commandDescription = "Trigger offload of data from a topic to long-term storage (e.g. Amazon S3)")
    private class Offload extends CliCommand {
        @Parameter(names = { "-s", "--size-threshold" },
                   description = "Maximum amount of data to keep in BookKeeper for the specified topic (e.g. 10M, 5G).",
                   required = true)
        private String sizeThresholdStr;

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long sizeThreshold = validateSizeString(sizeThresholdStr);

            PersistentTopicInternalStats stats = getTopics().getInternalStats(persistentTopic, false);
            if (stats.ledgers.size() < 1) {
                throw new PulsarAdminException("Topic doesn't have any data");
            }

            LinkedList<PersistentTopicInternalStats.LedgerInfo> ledgers = new LinkedList(stats.ledgers);
            ledgers.get(ledgers.size()-1).size = stats.currentLedgerSize; // doesn't get filled in now it seems
            MessageId messageId = findFirstLedgerWithinThreshold(ledgers, sizeThreshold);

            if (messageId == null) {
                System.out.println("Nothing to offload");
                return;
            }

            getTopics().triggerOffload(persistentTopic, messageId);
            System.out.println("Offload triggered for " + persistentTopic + " for messages before " + messageId);
        }
    }

    @Parameters(commandDescription = "Check the status of data offloading from a topic to long-term storage")
    private class OffloadStatusCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-w", "--wait-complete" },
                   description = "Wait for offloading to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            try {
                OffloadProcessStatus status = getTopics().offloadStatus(persistentTopic);
                while (wait && status.getStatus() == LongRunningProcessStatus.Status.RUNNING) {
                    Thread.sleep(1000);
                    status = getTopics().offloadStatus(persistentTopic);
                }

                switch (status.getStatus()) {
                case NOT_RUN:
                    System.out.println("Offload has not been run for " + persistentTopic
                                       + " since broker startup");
                    break;
                case RUNNING:
                    System.out.println("Offload is currently running");
                    break;
                case SUCCESS:
                    System.out.println("Offload was a success");
                    break;
                case ERROR:
                    System.out.println("Error in offload");
                    throw new PulsarAdminException("Error offloading: " + status.getLastError());
                }
            } catch (InterruptedException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    @Parameters(commandDescription = "get the last commit message id of topic")
    private class GetLastMessageId extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getLastMessageId(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Get the backlog quota policies for a topic")
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getBacklogQuotaMap(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set a backlog quota policy for a topic")
    private class SetBacklogQuota extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-l", "--limit" }, description = "Size limit (eg: 10M, 16G)")
        private String limitStr = "-1";

        @Parameter(names = { "-lt", "--limitTime" }, description = "Time limit in second, non-positive number for disabling time limit.")
        private int limitTime = -1;

        @Parameter(names = { "-p", "--policy" }, description = "Retention policy to enforce when the limit is reached. "
                + "Valid options are: [producer_request_hold, producer_exception, consumer_backlog_eviction]", required = true)
        private String policyStr;

        @Parameter(names = {"-t", "--type"}, description = "Backlog quota type to set. Valid options are: " +
                "destination_storage and message_age. " + 
                "destination_storage limits backlog by size (in bytes). " +
                "message_age limits backlog by time, that is, message timestamp (broker or publish timestamp). " +
                "You can set size or time to control the backlog, or combine them together to control the backlog. ")
        private String backlogQuotaTypeStr = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Override
        void run() throws PulsarAdminException {
            BacklogQuota.RetentionPolicy policy;
            long limit;
            BacklogQuota.BacklogQuotaType backlogQuotaType;

            try {
                policy = BacklogQuota.RetentionPolicy.valueOf(policyStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid retention policy type '%s'. Valid options are: %s",
                        policyStr, Arrays.toString(BacklogQuota.RetentionPolicy.values())));
            }

            limit = validateSizeString(limitStr);

            try {
                backlogQuotaType = BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaTypeStr);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(String.format("Invalid backlog quota type '%s'. Valid options are: %s",
                        backlogQuotaTypeStr, Arrays.toString(BacklogQuota.BacklogQuotaType.values())));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopics().setBacklogQuota(persistentTopic,
                    BacklogQuota.builder().limitSize(limit)
                            .limitTime(limitTime)
                            .retentionPolicy(policy)
                            .build(),
                    backlogQuotaType);
        }
    }

    @Parameters(commandDescription = "Remove a backlog quota policy from a topic")
    private class RemoveBacklogQuota extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-t", "--type"}, description = "Backlog quota type to remove")
        private String backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeBacklogQuota(persistentTopic, BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaType));
        }
    }

    @Parameters(commandDescription = "Get the replication clusters for a topic")
    private class GetReplicationClusters extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getReplicationClusters(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the replication clusters for a topic")
    private class SetReplicationClusters extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            getTopics().setReplicationClusters(persistentTopic, clusters);
        }
    }

    @Parameters(commandDescription = "Remove the replication clusters for a topic")
    private class RemoveReplicationClusters extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeReplicationClusters(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the delayed delivery policy for a topic")
    private class GetDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(params);
            print(getTopics().getDelayedDeliveryPolicy(topicName, applied));
        }
    }

    @Parameters(commandDescription = "Set the delayed delivery policy on a topic")
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
            String topicName = validateTopicName(params);
            long delayedDeliveryTimeInMills;
            try {
                delayedDeliveryTimeInMills = TimeUnit.SECONDS.toMillis(
                        RelativeTimeUtil.parseRelativeTimeInSeconds(delayedDeliveryTimeStr));
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(exception.getMessage());
            }

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            getTopics().setDelayedDeliveryPolicy(topicName, DelayedDeliveryPolicies.builder()
                    .tickTime(delayedDeliveryTimeInMills)
                    .active(enable)
                    .build());
        }
    }

    @Parameters(commandDescription = "Remove the delayed delivery policy on a topic")
    private class RemoveDelayedDelivery extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(params);
            getTopics().removeDelayedDeliveryPolicy(topicName);
        }
    }

    @Parameters(commandDescription = "Get the message TTL for a topic")
    private class GetMessageTTL extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMessageTTL(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set message TTL for a topic")
    private class SetMessageTTL extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--ttl" }, description = "Message TTL for topic in second, allowed range from 1 to Integer.MAX_VALUE", required = true)
        private int messageTTLInSecond;

        @Override
        void run() throws PulsarAdminException {
            if (messageTTLInSecond < 0) {
                throw new ParameterException(String.format("Invalid retention policy type '%d'. ", messageTTLInSecond));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMessageTTL(persistentTopic, messageTTLInSecond);
        }
    }

    @Parameters(commandDescription = "Remove message TTL for a topic")
    private class RemoveMessageTTL extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMessageTTL(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get deduplication snapshot interval for a topic")
    private class GetDeduplicationSnapshotInterval extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getDeduplicationSnapshotInterval(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set deduplication snapshot interval for a topic")
    private class SetDeduplicationSnapshotInterval extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-i", "--interval" }, description =
                "Deduplication snapshot interval for topic in second, allowed range from 0 to Integer.MAX_VALUE", required = true)
        private int interval;

        @Override
        void run() throws PulsarAdminException {
            if (interval < 0) {
                throw new ParameterException(String.format("Invalid interval '%d'. ", interval));
            }

            String persistentTopic = validatePersistentTopic(params);
            getTopics().setDeduplicationSnapshotInterval(persistentTopic, interval);
        }
    }

    @Parameters(commandDescription = "Remove deduplication snapshot interval for a topic")
    private class RemoveDeduplicationSnapshotInterval extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeDeduplicationSnapshotInterval(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the retention policy for a topic")
    private class GetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getRetention(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the retention policy for a topic")
    private class SetRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
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
            String persistentTopic = validatePersistentTopic(params);
            long sizeLimit = validateSizeString(limitStr);
            long retentionTimeInSec;
            try {
                retentionTimeInSec = RelativeTimeUtil.parseRelativeTimeInSeconds(retentionTimeStr);
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(exception.getMessage());
            }

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
            getTopics().setRetention(persistentTopic, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Deprecated
    @Parameters(commandDescription = "Enable the deduplication policy for a topic")
    private class EnableDeduplication extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().enableDeduplication(persistentTopic, true);
        }
    }

    @Deprecated
    @Parameters(commandDescription = "Disable the deduplication policy for a topic")
    private class DisableDeduplication extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().enableDeduplication(persistentTopic, false);
        }
    }

    @Parameters(commandDescription = "Enable or disable deduplication for a topic")
    private class SetDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getTopics().setDeduplicationStatus(persistentTopic, enable);
        }
    }

    @Parameters(commandDescription = "Get the deduplication policy for a topic")
    private class GetDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getDeduplicationStatus(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Remove the deduplication policy for a topic")
    private class RemoveDeduplicationStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeDeduplicationStatus(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Remove the retention policy for a topic")
    private class RemoveRetention extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeRetention(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the persistence policies for a topic")
    private class GetPersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getPersistence(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Get the offload policies for a topic")
    private class GetOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getOffloadPolicies(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove the offload policies for a topic")
    private class RemoveOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeOffloadPolicies(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set the offload policies for a topic")
    private class SetOffloadPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-d", "--driver"}, description = "ManagedLedger offload driver", required = true)
        private String driver;

        @Parameter(names = {"-r", "--region"}
                , description = "ManagedLedger offload region, s3 and google-cloud-storage requires this parameter")
        private String region;

        @Parameter(names = {"-b", "--bucket"}
                , description = "ManagedLedger offload bucket, s3 and google-cloud-storage requires this parameter")
        private String bucket;

        @Parameter(names = {"-e", "--endpoint"}
                , description = "ManagedLedger offload service endpoint, only s3 requires this parameter")
        private String endpoint;

        @Parameter(names = {"-i", "--aws-id"}
                , description = "AWS Credential Id to use when using driver S3 or aws-s3")
        private String awsId;

        @Parameter(names = {"-s", "--aws-secret"}
                , description = "AWS Credential Secret to use when using driver S3 or aws-s3")
        private String awsSecret;

        @Parameter(names = {"--ro", "--s3-role"}
                , description = "S3 Role used for STSAssumeRoleSessionCredentialsProvider")
        private String s3Role;

        @Parameter(names = {"--s3-role-session-name", "-rsn"}
                , description = "S3 role session name used for STSAssumeRoleSessionCredentialsProvider")
        private String s3RoleSessionName;

        @Parameter(names = {"-m", "--maxBlockSizeInBytes"},
                description = "ManagedLedger offload max block Size in bytes,"
                + "s3 and google-cloud-storage requires this parameter")
        private int maxBlockSizeInBytes;

        @Parameter(names = {"-rb", "--readBufferSizeInBytes"},
                description = "ManagedLedger offload read buffer size in bytes,"
                + "s3 and google-cloud-storage requires this parameter")
        private int readBufferSizeInBytes;

        @Parameter(names = {"-t", "--offloadThresholdInBytes"}
                , description = "ManagedLedger offload threshold in bytes", required = true)
        private long offloadThresholdInBytes;

        @Parameter(names = {"-dl", "--offloadDeletionLagInMillis"}
                , description = "ManagedLedger offload deletion lag in bytes")
        private Long offloadDeletionLagInMillis;

        @Parameter(
                names = {"--offloadedReadPriority", "-orp"},
                description = "Read priority for offloaded messages. By default, once messages are offloaded to long-term storage, brokers read messages from long-term storage, but messages can still exist in BookKeeper for a period depends on your configuration. For messages that exist in both long-term storage and BookKeeper, you can set where to read messages from with the option `tiered-storage-first` or `bookkeeper-first`.",
                required = false
        )
        private String offloadReadPriorityStr;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

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

            OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create(driver, region, bucket, endpoint,
                    s3Role, s3RoleSessionName,
                    awsId, awsSecret,
                    maxBlockSizeInBytes,
                    readBufferSizeInBytes, offloadThresholdInBytes, offloadDeletionLagInMillis, offloadedReadPriority);

            getTopics().setOffloadPolicies(persistentTopic, offloadPolicies);
        }
    }

    @Parameters(commandDescription = "Set the persistence policies for a topic")
    private class SetPersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic", required = true)
        private int bookkeeperEnsemble;

        @Parameter(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry", required = true)
        private int bookkeeperWriteQuorum;

        @Parameter(names = { "-a",
                "--bookkeeper-ack-quorum" }, description = "Number of acks (guaranteed copies) to wait for each entry", required = true)
        private int bookkeeperAckQuorum;

        @Parameter(names = { "-r",
                "--ml-mark-delete-max-rate" }, description = "Throttling rate of mark-delete operation (0 means no throttle)", required = true)
        private double managedLedgerMaxMarkDeleteRate;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setPersistence(persistentTopic, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate));
        }
    }

    @Parameters(commandDescription = "Remove the persistence policy for a topic")
    private class RemovePersistence extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removePersistence(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get message dispatch rate for a topic")
    private class GetDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getDispatchRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set message dispatch rate for a topic")
    private class SetDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
                "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove message dispatch rate for a topic")
    private class RemoveDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeDispatchRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max unacked messages policy on consumer for a topic")
    private class GetMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxUnackedMessagesOnConsumer(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove max unacked messages policy on consumer for a topic")
    private class RemoveMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxUnackedMessagesOnConsumer(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set max unacked messages policy on consumer for a topic")
    private class SetMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-m", "--maxNum"}, description = "max unacked messages num on consumer", required = true)
        private int maxNum;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxUnackedMessagesOnConsumer(persistentTopic, maxNum);
        }
    }

    @Parameters(commandDescription = "Get max unacked messages policy on subscription for a topic")
    private class GetMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxUnackedMessagesOnSubscription(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Remove max unacked messages policy on subscription for a topic")
    private class RemoveMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxUnackedMessagesOnSubscription(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Set max unacked messages policy on subscription for a topic")
    private class SetMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-m", "--maxNum"}, description = "max unacked messages num on subscription", required = true)
        private int maxNum;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxUnackedMessagesOnSubscription(persistentTopic, maxNum);
        }
    }


    @Parameters(commandDescription = "Set subscription types enabled for a topic")
    private class SetSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--types", "-t"}, description = "Subscription types enabled list (comma separated values)."
                + " Possible values: (Exclusive, Shared, Failover, Key_Shared).", required = true)
        private List<String> subTypes;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
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
            getTopics().setSubscriptionTypesEnabled(persistentTopic, types);
        }
    }

    @Parameters(commandDescription = "Get subscription types enabled for a topic")
    private class GetSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getSubscriptionTypesEnabled(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Remove subscription types enabled for a topic")
    private class RemoveSubscriptionTypesEnabled extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeSubscriptionTypesEnabled(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get compaction threshold for a topic")
    private class GetCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getCompactionThreshold(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set compaction threshold for a topic")
    private class SetCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--threshold", "-t" },
            description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
            required = true)
        private String thresholdStr = "0";

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            long threshold = validateSizeString(thresholdStr);
            getTopics().setCompactionThreshold(persistentTopic, threshold);
        }
    }

    @Parameters(commandDescription = "Remove compaction threshold for a topic")
    private class RemoveCompactionThreshold extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeCompactionThreshold(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get publish rate for a topic")
    private class GetPublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getPublishRate(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set publish rate for a topic")
    private class SetPublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-publish-rate",
            "-m" }, description = "message-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgPublishRate = -1;

         @Parameter(names = { "--byte-publish-rate",
            "-b" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private long bytePublishRate = -1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setPublishRate(persistentTopic,
                new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

    @Parameters(commandDescription = "Remove publish rate for a topic")
    private class RemovePublishRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removePublishRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get subscription message-dispatch-rate for a topic")
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getSubscriptionDispatchRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set subscription message-dispatch-rate for a topic")
    private class SetSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setSubscriptionDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove subscription message-dispatch-rate for a topic")
    private class RemoveSubscriptionDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeSubscriptionDispatchRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get replicator message-dispatch-rate for a topic")
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validatePersistentTopic(params);
            print(getTopics().getReplicatorDispatchRate(topic, applied));
        }
    }

    @Parameters(commandDescription = "Set replicator message-dispatch-rate for a topic")
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgDispatchRate = -1;

        @Parameter(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Parameter(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not passed)", required = false)
        private int dispatchRatePeriodSec = 1;

        @Parameter(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled then broker will apply throttling value to (publish-rate + dispatch rate))", required = false)
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setReplicatorDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Parameters(commandDescription = "Remove replicator message-dispatch-rate for a topic")
    private class RemoveReplicatorDispatchRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeReplicatorDispatchRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max number of producers for a topic")
    private class GetMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxProducers(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set max number of producers for a topic")
    private class SetMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-producers", "-p"}, description = "Max producers for a topic", required = true)
        private int maxProducers;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxProducers(persistentTopic, maxProducers);
        }
    }

    @Parameters(commandDescription = "Remove max number of producers for a topic")
    private class RemoveMaxProducers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxProducers(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max number of subscriptions for a topic")
    private class GetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxSubscriptionsPerTopic(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max number of subscriptions for a topic")
    private class SetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-subscriptions-per-topic", "-m"},
                description = "Maximum subscription limit for a topic", required = true)
        private int maxSubscriptionsPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxSubscriptionsPerTopic(persistentTopic, maxSubscriptionsPerTopic);
        }
    }

    @Parameters(commandDescription = "Remove max number of subscriptions for a topic")
    private class RemoveMaxSubscriptionsPerTopic extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxSubscriptionsPerTopic(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max message size for a topic")
    private class GetMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxMessageSize(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max message size for a topic")
    private class SetMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"--max-message-size", "-m"}, description = "Max message size for a topic", required = true)
        private int maxMessageSize;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxMessageSize(persistentTopic, maxMessageSize);
        }
    }

    @Parameters(commandDescription = "Remove max message size for a topic")
    private class RemoveMaxMessageSize extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxMessageSize(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max consumers per subscription for a topic")
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxConsumersPerSubscription(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Set max consumers per subscription for a topic")
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers-per-subscription", "-c" }, description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxConsumersPerSubscription(persistentTopic, maxConsumersPerSubscription);
        }
    }

    @Parameters(commandDescription = "Remove max consumers per subscription for a topic")
    private class RemoveMaxConsumersPerSubscription extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxConsumersPerSubscription(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get the inactive topic policies on a topic")
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getInactiveTopicPolicies(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set the inactive topic policies on a topic")
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
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
            String persistentTopic = validatePersistentTopic(params);
            long maxInactiveDurationInSeconds;
            try {
                maxInactiveDurationInSeconds = TimeUnit.SECONDS.toSeconds(
                        RelativeTimeUtil.parseRelativeTimeInSeconds(deleteInactiveTopicsMaxInactiveDuration));
            } catch (IllegalArgumentException exception) {
                throw new ParameterException(exception.getMessage());
            }

            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new ParameterException("Need to specify either enable-delete-while-inactive or disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode = null;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions or delete_when_subscriptions_caught_up");
            }
            getTopics().setInactiveTopicPolicies(persistentTopic,
                    new InactiveTopicPolicies(deleteMode, (int) maxInactiveDurationInSeconds, enableDeleteWhileInactive));
        }
    }

    @Parameters(commandDescription = "Remove inactive topic policies from a topic")
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeInactiveTopicPolicies(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get max number of consumers for a topic")
    private class GetMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getMaxConsumers(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set max number of consumers for a topic")
    private class SetMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--max-consumers", "-c" }, description = "Max consumers for a topic", required = true)
        private int maxConsumers;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setMaxConsumers(persistentTopic, maxConsumers);
        }
    }

    @Parameters(commandDescription = "Remove max number of consumers for a topic")
    private class RemoveMaxConsumers extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeMaxConsumers(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Get consumer subscribe rate for a topic")
    private class GetSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getSubscribeRate(persistentTopic, applied));
        }
    }

    @Parameters(commandDescription = "Set consumer subscribe rate for a topic")
    private class SetSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)", required = false)
        private int subscribeRate = -1;

        @Parameter(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type (default 30 second will be overwrite if not passed)", required = false)
        private int subscribeRatePeriodSec = 30;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().setSubscribeRate(persistentTopic,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Parameters(commandDescription = "Remove consumer subscribe rate for a topic")
    private class RemoveSubscribeRate extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getTopics().removeSubscribeRate(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Enable or disable a replicated subscription on a topic")
    private class SetReplicatedSubscriptionStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription name to enable or disable replication", required = true)
        private String subName;

        @Parameter(names = { "--enable", "-e" }, description = "Enable replication")
        private boolean enable = false;

        @Parameter(names = { "--disable", "-d" }, description = "Disable replication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getTopics().setReplicatedSubscriptionStatus(persistentTopic, subName, enable);
        }
    }

    @Parameters(commandDescription = "Get replicated subscription status on a topic")
    private class GetReplicatedSubscriptionStatus extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-s",
                "--subscription"}, description = "Subscription name", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getTopics().getReplicatedSubscriptionStatus(persistentTopic, subName));
        }
    }

    private Topics getTopics() {
        return getAdmin().topics();
    }

    @Parameters(commandDescription = "Calculate backlog size by a message ID (in bytes).")
    private class GetBacklogSizeByMessageId extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId used to calculate backlog size. It can be (ledgerId:entryId).", required = false)
        private String messagePosition = "-1:-1";

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            MessageId messageId;
            if("-1:-1".equals(messagePosition)) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messagePosition);
            }
            print(getTopics().getBacklogSizeByMessageId(persistentTopic, messageId));

        }
    }
}
