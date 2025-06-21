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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToIntegerConverter;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToSecondsConverter;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TransactionIsolationLevel;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Getter
@Command(description = "Operations on persistent topics")
public class CmdTopics extends CmdBase {
    private final CmdTopics.PartitionedLookup partitionedLookup;
    private final CmdTopics.DeleteCmd deleteCmd;

    public CmdTopics(Supplier<PulsarAdmin> admin) {
        super("topics", admin);
        partitionedLookup = new PartitionedLookup();
        deleteCmd = new DeleteCmd();
        addCommand("list", new ListCmd());
        addCommand("list-partitioned-topics", new PartitionedTopicListCmd());
        addCommand("permissions", new Permissions());
        addCommand("grant-permission", new GrantPermissions());
        addCommand("revoke-permission", new RevokePermissions());
        addCommand("lookup", new Lookup());
        addCommand("partitioned-lookup", partitionedLookup);
        addCommand("bundle-range", new GetBundleRange());
        addCommand("delete", deleteCmd);
        addCommand("truncate", new TruncateCmd());
        addCommand("unload", new UnloadCmd());
        addCommand("subscriptions", new ListSubscriptions());
        addCommand("unsubscribe", new DeleteSubscription());
        addCommand("create-subscription", new CreateSubscription());
        addCommand("update-subscription-properties", new UpdateSubscriptionProperties());
        addCommand("get-subscription-properties", new GetSubscriptionProperties());

        addCommand("stats", new GetStats());
        addCommand("stats-internal", new GetInternalStats());
        addCommand("info-internal", new GetInternalInfo());

        addCommand("partitioned-stats", new GetPartitionedStats());
        addCommand("partitioned-stats-internal", new GetPartitionedStatsInternal());

        addCommand("skip", new Skip());
        addCommand("clear-backlog", new ClearBacklog());

        addCommand("expire-messages", new ExpireMessages());
        addCommand("expire-messages-all-subscriptions", new ExpireMessagesForAllSubscriptions());

        addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        addCommand("create-missed-partitions", new CreateMissedPartitionsCmd());
        addCommand("create", new CreateNonPartitionedCmd());
        addCommand("update-partitioned-topic", new UpdatePartitionedCmd());
        addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());
        addCommand("get-properties", new GetPropertiesCmd());
        addCommand("update-properties", new UpdateProperties());
        addCommand("remove-properties", new RemoveProperties());

        addCommand("delete-partitioned-topic", new DeletePartitionedCmd());
        addCommand("peek-messages", new PeekMessages());
        addCommand("examine-messages", new ExamineMessages());
        addCommand("get-message-by-id", new GetMessageById());
        addCommand("get-message-id", new GetMessageId());
        addCommand("reset-cursor", new ResetCursor());
        addCommand("terminate", new Terminate());
        addCommand("partitioned-terminate", new PartitionedTerminate());
        addCommand("compact", new Compact());
        addCommand("compaction-status", new CompactionStatusCmd());
        addCommand("offload", new Offload());
        addCommand("offload-status", new OffloadStatusCmd());
        addCommand("last-message-id", new GetLastMessageId());
        addCommand("get-backlog-quotas", new GetBacklogQuotaMap());
        addCommand("set-backlog-quota", new SetBacklogQuota());
        addCommand("remove-backlog-quota", new RemoveBacklogQuota());
        addCommand("get-message-ttl", new GetMessageTTL());
        addCommand("set-message-ttl", new SetMessageTTL());
        addCommand("remove-message-ttl", new RemoveMessageTTL());
        addCommand("get-retention", new GetRetention());
        addCommand("set-retention", new SetRetention());
        addCommand("remove-retention", new RemoveRetention());
        //deprecated commands
        addCommand("enable-deduplication", new EnableDeduplication());
        addCommand("disable-deduplication", new DisableDeduplication());
        addCommand("get-deduplication-enabled", new GetDeduplicationStatus());

        addCommand("set-deduplication", new SetDeduplicationStatus());
        addCommand("get-deduplication", new GetDeduplicationStatus());
        addCommand("remove-deduplication", new RemoveDeduplicationStatus());

        addCommand("get-deduplication-snapshot-interval", new GetDeduplicationSnapshotInterval());
        addCommand("set-deduplication-snapshot-interval", new SetDeduplicationSnapshotInterval());
        addCommand("remove-deduplication-snapshot-interval", new RemoveDeduplicationSnapshotInterval());

        addCommand("get-delayed-delivery", new GetDelayedDelivery());
        addCommand("set-delayed-delivery", new SetDelayedDelivery());
        addCommand("remove-delayed-delivery", new RemoveDelayedDelivery());
        addCommand("get-persistence", new GetPersistence());
        addCommand("set-persistence", new SetPersistence());
        addCommand("remove-persistence", new RemovePersistence());
        addCommand("get-offload-policies", new GetOffloadPolicies());
        addCommand("set-offload-policies", new SetOffloadPolicies());
        addCommand("remove-offload-policies", new RemoveOffloadPolicies());

        addCommand("get-dispatch-rate", new GetDispatchRate());
        addCommand("set-dispatch-rate", new SetDispatchRate());
        addCommand("remove-dispatch-rate", new RemoveDispatchRate());

        addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());
        addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        addCommand("remove-subscription-dispatch-rate", new RemoveSubscriptionDispatchRate());

        addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());
        addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        addCommand("remove-replicator-dispatch-rate", new RemoveReplicatorDispatchRate());

        addCommand("get-compaction-threshold", new GetCompactionThreshold());
        addCommand("set-compaction-threshold", new SetCompactionThreshold());
        addCommand("remove-compaction-threshold", new RemoveCompactionThreshold());

        //deprecated commands
        addCommand("get-max-unacked-messages-on-consumer", new GetMaxUnackedMessagesOnConsumer());
        addCommand("set-max-unacked-messages-on-consumer", new SetMaxUnackedMessagesOnConsumer());
        addCommand("remove-max-unacked-messages-on-consumer", new RemoveMaxUnackedMessagesOnConsumer());
        addCommand("get-max-unacked-messages-on-subscription", new GetMaxUnackedMessagesOnSubscription());
        addCommand("set-max-unacked-messages-on-subscription", new SetMaxUnackedMessagesOnSubscription());
        addCommand("remove-max-unacked-messages-on-subscription",
                new RemoveMaxUnackedMessagesOnSubscription());

        addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesOnConsumer());
        addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesOnConsumer());
        addCommand("remove-max-unacked-messages-per-consumer", new RemoveMaxUnackedMessagesOnConsumer());
        addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesOnSubscription());
        addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesOnSubscription());
        addCommand("remove-max-unacked-messages-per-subscription",
                new RemoveMaxUnackedMessagesOnSubscription());
        addCommand("get-publish-rate", new GetPublishRate());
        addCommand("set-publish-rate", new SetPublishRate());
        addCommand("remove-publish-rate", new RemovePublishRate());

        addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());

        //deprecated commands
        addCommand("get-maxProducers", new GetMaxProducers());
        addCommand("set-maxProducers", new SetMaxProducers());
        addCommand("remove-maxProducers", new RemoveMaxProducers());

        addCommand("get-max-producers", new GetMaxProducers());
        addCommand("set-max-producers", new SetMaxProducers());
        addCommand("remove-max-producers", new RemoveMaxProducers());

        addCommand("get-max-subscriptions", new GetMaxSubscriptionsPerTopic());
        addCommand("set-max-subscriptions", new SetMaxSubscriptionsPerTopic());
        addCommand("remove-max-subscriptions", new RemoveMaxSubscriptionsPerTopic());

        addCommand("get-max-message-size", new GetMaxMessageSize());
        addCommand("set-max-message-size", new SetMaxMessageSize());
        addCommand("remove-max-message-size", new RemoveMaxMessageSize());

        addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        addCommand("remove-max-consumers-per-subscription", new RemoveMaxConsumersPerSubscription());

        addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        addCommand("get-max-consumers", new GetMaxConsumers());
        addCommand("set-max-consumers", new SetMaxConsumers());
        addCommand("remove-max-consumers", new RemoveMaxConsumers());

        addCommand("get-subscribe-rate", new GetSubscribeRate());
        addCommand("set-subscribe-rate", new SetSubscribeRate());
        addCommand("remove-subscribe-rate", new RemoveSubscribeRate());

        addCommand("set-replicated-subscription-status", new SetReplicatedSubscriptionStatus());
        addCommand("get-replicated-subscription-status", new GetReplicatedSubscriptionStatus());
        addCommand("get-backlog-size", new GetBacklogSizeByMessageId());
        addCommand("analyze-backlog", new AnalyzeBacklog());

        addCommand("get-replication-clusters", new GetReplicationClusters());
        addCommand("set-replication-clusters", new SetReplicationClusters());
        addCommand("remove-replication-clusters", new RemoveReplicationClusters());

        addCommand("get-shadow-topics", new GetShadowTopics());
        addCommand("set-shadow-topics", new SetShadowTopics());
        addCommand("remove-shadow-topics", new RemoveShadowTopics());
        addCommand("create-shadow-topic", new CreateShadowTopic());
        addCommand("get-shadow-source", new GetShadowSource());

        addCommand("get-schema-validation-enforce", new GetSchemaValidationEnforced());
        addCommand("set-schema-validation-enforce", new SetSchemaValidationEnforced());

        addCommand("trim-topic", new TrimTopic());
    }

    @Command(description = "Get the list of topics under a namespace.")
    private class ListCmd extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = {"-td", "--topic-domain"},
                description = "Allowed topic domain (persistent, non_persistent).")
        private TopicDomain topicDomain;

        @Option(names = { "-b",
                "--bundle" }, description = "Namespace bundle to get list of topics")
        private String bundle;

        @Option(names = { "-ist",
                "--include-system-topic" }, description = "Include system topic")
        private boolean includeSystemTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            ListTopicsOptions options = ListTopicsOptions.builder()
                    .bundle(bundle)
                    .includeSystemTopic(includeSystemTopic)
                    .build();
            print(getTopics().getList(namespace, topicDomain, options));
        }
    }

    @Command(description = "Get the list of partitioned topics under a namespace.")
    private class PartitionedTopicListCmd extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Option(names = { "-ist",
                "--include-system-topic" }, description = "Include system topic")
        private boolean includeSystemTopic;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            ListTopicsOptions options = ListTopicsOptions.builder().includeSystemTopic(includeSystemTopic).build();
            print(getTopics().getPartitionedTopicList(namespace, options));
        }
    }

    @Command(description = "Grant a new permission to a client role on a single topic.")
    private class GrantPermissions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-r", "--role"}, description = "Client role to which grant permissions", required = true)
        private String role;

        @Option(names = {"-a", "--actions"}, description = "Actions to be granted (produce,consume,sources,sinks,"
                + "functions,packages)", required = true, split = ",")
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().grantPermission(topic, role, getAuthActions(actions));
        }
    }

    @Command(description = "Revoke permissions on a topic. "
            + "Revoke permissions to a client role on a single topic. If the permission "
            + "was not set at the topic level, but rather at the namespace level, this "
            + "operation will return an error (HTTP status code 412).")
    private class RevokePermissions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-r", "--role"}, description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().revokePermissions(topic, role);
        }
    }

    @Command(description = "Get the permissions on a topic. "
            + "Retrieve the effective permissions for a topic. These permissions are defined "
            + "by the permissions set at the namespace level combined (union) with any eventual "
            + "specific permission set on the topic.")
    private class Permissions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getTopics().getPermissions(topic));
        }
    }

    @Command(description = "Lookup a topic from the current serving broker")
    private class Lookup extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getAdmin().lookups().lookupTopic(topic));
        }
    }

    @Command(description = "Lookup a partitioned topic from the current serving broker")
    protected class PartitionedLookup extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        protected String topicName;
        @Option(names = { "-s",
                "--sort-by-broker" }, description = "Sort partitioned-topic by Broker Url")
        protected boolean sortByBroker = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            if (sortByBroker) {
                Map<String, String> partitionLookup = getAdmin().lookups().lookupPartitionedTopic(topic);
                Map<String, List<String>> result = new HashMap<>();
                for (Map.Entry<String, String> entry : partitionLookup.entrySet()) {
                    List<String> topics = result.getOrDefault(entry.getValue(), new ArrayList<String>());
                    topics.add(entry.getKey());
                    result.put(entry.getValue(), topics);
                }
                print(result);
            } else {
                print(getAdmin().lookups().lookupPartitionedTopic(topic));
            }
        }
    }

    @Command(description = "Get Namespace bundle range of a topic")
    private class GetBundleRange extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getAdmin().lookups().getBundleRange(topic));
        }
    }

    @Command(description = "Create a partitioned topic. "
            + "The partitioned topic has to be created before creating a producer on it.")
    private class CreatePartitionedCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Option(names = {"--metadata", "-m"}, description = "key value pair properties(a=a,b=b,c=c)")
        private java.util.List<String> metadata;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            Map<String, String> map = parseListKeyValueMap(metadata);
            getTopics().createPartitionedTopic(topic, numPartitions, map);
        }
    }

    @Command(description = "Try to create partitions for partitioned topic. "
            + "The partitions of partition topic has to be created, can be used by repair partitions when "
            + "topic auto creation is disabled")
    private class CreateMissedPartitionsCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getTopics().createMissedPartitions(topic);
        }
    }

    @Command(description = "Create a non-partitioned topic.")
    private class CreateNonPartitionedCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--metadata", "-m"}, description = "key value pair properties(a=a,b=b,c=c)")
        private java.util.List<String> metadata;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            Map<String, String> map = parseListKeyValueMap(metadata);
            getTopics().createNonPartitionedTopic(topic, map);
        }
    }

    @Command(description = "Update existing partitioned topic. "
            + "New updating number of partitions must be greater than existing number of partitions.")
    private class UpdatePartitionedCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Option(names = { "-ulo",
                "--update-local-only"}, description = "Update partitions number for topic in local cluster only")
        private boolean updateLocalOnly = false;

        @Option(names = { "-f",
                "--force" }, description = "Update forcefully without validating existing partitioned topic")
        private boolean force;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getTopics().updatePartitionedTopic(topic, numPartitions, updateLocalOnly, force);
        }
    }

    @Command(description = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getTopics().getPartitionedTopicMetadata(topic));
        }
    }

    @Command(description = "Get the topic properties.")
    private class GetPropertiesCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getTopics().getProperties(topic));
        }
    }

    @Command(description = "Update the properties of on a topic")
    private class UpdateProperties extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--property", "-p"}, description = "key value pair properties(-p a=b -p c=d)",
                required = false)
        private Map<String, String> properties;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            if (properties == null) {
                properties = Collections.emptyMap();
            }
            getTopics().updateProperties(topic, properties);
        }
    }

    @Command(description = "Remove the key in properties of a topic")
    private class RemoveProperties extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--key", "-k"}, description = "The key to remove in the properties of topic")
        private String key;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getTopics().removeProperties(topic, key);
        }
    }

    @Command(description = "Delete a partitioned topic. "
            + "It will also delete all the partitions of the topic if it exists."
            + "And the application is not able to connect to the topic(delete then re-create with same name) again "
            + "if the schema auto uploading is disabled. Besides, users should to use the truncate cmd to clean up "
            + "data of the topic instead of delete cmd if users continue to use this topic later.")
    private class DeletePartitionedCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Option(names = {"-d", "--deleteSchema"}, description = "Delete schema while deleting topic, "
                + "but the parameter is invalid and the schema is always deleted", hidden = true)
        private boolean deleteSchema = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getTopics().deletePartitionedTopic(topic, force);
        }
    }

    @Command(description = "Delete a topic. "
            + "The topic cannot be deleted if there's any active subscription or producers connected to it."
            + "And the application is not able to connect to the topic(delete then re-create with same name) again "
            + "if the schema auto uploading is disabled. Besides, users should to use the truncate cmd to clean up "
            + "data of the topic instead of delete cmd if users continue to use this topic later.")
    protected class DeleteCmd extends CliCommand {
        @Parameters(description = "Provide either a single topic in the format 'persistent://tenant/namespace/topic', "
                + "or a path to a file containing a list of topics, e.g., 'path://resources/topics.txt'. "
                + "This parameter is required.", arity = "1")
        protected String topic;

        @Option(names = { "-f",
                "--force" }, description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Option(names = {"-d", "--deleteSchema"}, description = "Delete schema while deleting topic, "
                + "but the parameter is invalid and the schema is always deleted", hidden = true)
        private boolean deleteSchema = false;

        @Option(names = {"-r", "regex"},
                description = "Use a regex expression to match multiple topics for deletion.")
        boolean regex = false;

        @Option(names = {"--from-file"}, description = "Read a list of topics from a file for deletion.")
        boolean readFromFile;


        @Override
        void run() throws PulsarAdminException, IOException {
            if (readFromFile && regex) {
                throw new ParameterException("Could not apply regex when read topics from file.");
            }
            if (readFromFile) {
                List<String> topicsFromFile = Files.readAllLines(Path.of(topic));
                for (String t : topicsFromFile) {
                    try {
                        getTopics().delete(t, force);
                    } catch (Exception e) {
                        print("Failed to delete topic: " + t + ". Exception: " + e);
                    }
                }
            } else {
                String topicName = validateTopicName(topic);
                if (regex) {
                    String namespace = TopicName.get(topic).getNamespace();
                    List<String> topics = getTopics().getList(namespace);
                    topics = topics.stream().filter(s -> s.matches(topicName)).toList();
                    for (String t : topics) {
                        try {
                            getTopics().delete(t, force);
                        } catch (Exception e) {
                            print("Failed to delete topic: " + t + ". Exception: " + e);
                        }
                    }
                } else {
                    try {
                        getTopics().delete(topicName, force);
                    } catch (Exception e) {
                        print("Failed to delete topic: " + topic + ". Exception: " + e);
                    }
                }
            }
        }
    }

    @Command(description = "Truncate a topic. \n"
            + "\t\tThe truncate operation will move all cursors to the end of the topic "
            + "and delete all inactive ledgers. ")
    private class TruncateCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().truncate(topic);
        }
    }

    @Command(description = "Unload a topic.")
    private class UnloadCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().unload(topic);
        }
    }

    @Command(description = "Get the list of subscriptions on the topic")
    private class ListSubscriptions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getTopics().getSubscriptions(topic));
        }
    }

    @Command(description = "Delete a durable subscriber from a topic. "
            + "The subscription cannot be deleted if there are any active consumers attached to it")
    private class DeleteSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-f",
            "--force" }, description = "Disconnect and close all consumers and delete subscription forcefully")
        private boolean force = false;

        @Option(names = {"-s", "--subscription"}, description = "Subscription to be deleted", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().deleteSubscription(topic, subName, force);
        }
    }

    @Command(name = "stats", description = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Option(names = { "-sbs",
                "--get-subscription-backlog-size" }, description = "Set true to get backlog size for each subscription"
                + ", locking required. If set to false, the attribute 'backlogSize' in the response will be -1")
        private boolean subscriptionBacklogSize = true;

        @Option(names = { "-etb",
                "--get-earliest-time-in-backlog" }, description = "Set true to get earliest time in backlog")
        private boolean getEarliestTimeInBacklog = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getTopics().getStats(topic, getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog));
        }
    }

    @Command(description = "Get the internal stats for the topic")
    private class GetInternalStats extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-m",
        "--metadata" }, description = "Flag to include ledger metadata")
        private boolean metadata = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getTopics().getInternalStats(topic, metadata));
        }
    }

    @Command(description = "Get the internal metadata info for the topic")
    private class GetInternalInfo extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            String internalInfo = getTopics().getInternalInfo(topic);
            if (internalInfo == null) {
                System.out.println("Did not find any internal metadata info");
                return;
            }
            JsonObject result = JsonParser.parseString(internalInfo).getAsJsonObject();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(result));
        }
    }

    @Command(description = "Get the stats for the partitioned topic "
            + "and its connected producers and consumers. All the rates are computed over a 1 minute window "
            + "and are relative the last completed 1 minute period.")
    private class GetPartitionedStats extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--per-partition", description = "Get per partition stats")
        private boolean perPartition = false;

        @Option(names = { "-gpb",
            "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Option(names = { "-sbs",
                "--get-subscription-backlog-size" }, description = "Set true to get backlog size for each subscription"
                + ", locking required.")
        private boolean subscriptionBacklogSize = true;

        @Option(names = { "-etb",
                "--get-earliest-time-in-backlog" }, description = "Set true to get earliest time in backlog")
        private boolean getEarliestTimeInBacklog = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getTopics().getPartitionedStats(topic, perPartition, getPreciseBacklog,
                    subscriptionBacklogSize, getEarliestTimeInBacklog));
        }
    }

    @Command(description = "Get the internal stats for the partitioned topic "
            + "and its connected producers and consumers. All the rates are computed over a 1 minute window "
            + "and are relative the last completed 1 minute period.")
    private class GetPartitionedStatsInternal extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getTopics().getPartitionedInternalStats(topic));
        }
    }

    @Command(description = "Skip all the messages for the subscription")
    private class ClearBacklog extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s", "--subscription" }, description = "Subscription to be cleared", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().skipAllMessages(topic, subName);
        }
    }

    @Command(description = "Skip some messages for the subscription")
    private class Skip extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Option(names = { "-n", "--count" }, description = "Number of messages to skip", required = true)
        private long numMessages;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().skipMessages(topic, subName, numMessages);
        }
    }

    @Command(description = "Expire messages that older than given expiry time (in seconds) "
            + "for the subscription")
    private class ExpireMessages extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Option(names = { "-t", "--expireTime" }, description = "Expire messages older than time in seconds "
                + "(or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w)",
                converter = TimeUnitToSecondsConverter.class)
        private Long expireTimeInSeconds = -1L;

        @Option(names = { "--position",
                "-p" }, description = "message position to reset back to (ledgerId:entryId)", required = false)
        private String messagePosition;

        @Option(names = { "-e", "--exclude-reset-position" },
                description = "Exclude the reset position, start consume messages from the next position.")
        private boolean excludeResetPosition = false;

        @Override
        void run() throws PulsarAdminException {
            if (expireTimeInSeconds >= 0 && isNotBlank(messagePosition)) {
                throw new ParameterException(String.format("Can't expire message by time and "
                        + "by message position at the same time."));
            }
            String topic = validateTopicName(topicName);
            if (expireTimeInSeconds >= 0) {
                getTopics().expireMessages(topic, subName, expireTimeInSeconds);
            } else if (isNotBlank(messagePosition)) {
                int partitionIndex = TopicName.get(topic).getPartitionIndex();
                MessageId messageId = validateMessageIdString(messagePosition, partitionIndex);
                getTopics().expireMessages(topic, subName, messageId, excludeResetPosition);
            } else {
                throw new ParameterException(
                        "Either time (--expireTime) or message position (--position) has to be provided"
                                + " to expire messages");
            }
        }
    }

    @Command(description = "Expire messages that older than given expiry time (in seconds) "
            + "for all subscriptions")
    private class ExpireMessagesForAllSubscriptions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-t", "--expireTime"}, description = "Expire messages older than time in seconds "
                + "(or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w)", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopics().expireMessagesForAllSubscriptions(topic, expireTimeInSeconds);
        }
    }

    @Command(description = "Create a new subscription on a topic")
    private class CreateSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Name of subscription to be created", required = true)
        private String subscriptionName;

        @Option(names = { "-m" , "--messageId" }, description = "messageId where to create the subscription. "
                + "It can be either 'latest', 'earliest' or (ledgerId:entryId)", required = false)
        private String messageIdStr = "latest";

        @Option(names = { "-r", "--replicated" }, description = "replicated subscriptions", required = false)
        private boolean replicated = false;

        @Option(names = {"--property", "-p"}, description = "key value pair properties(-p a=b -p c=d)",
                required = false)
        private Map<String, String> properties;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            MessageId messageId;
            if (messageIdStr.equals("latest")) {
                messageId = MessageId.latest;
            } else if (messageIdStr.equals("earliest")) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messageIdStr);
            }
            getTopics().createSubscription(topic, subscriptionName, messageId, replicated, properties);
        }
    }

    @Command(description = "Update the properties of a subscription on a topic")
    private class UpdateSubscriptionProperties extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to update", required = true)
        private String subscriptionName;

        @Option(names = {"--property", "-p"}, description = "key value pair properties(-p a=b -p c=d)",
                required = false)
        private Map<String, String> properties;

        @Option(names = {"--clear", "-c"}, description = "Remove all properties",
                required = false)
        private boolean clear;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            if (properties == null) {
                properties = Collections.emptyMap();
            }
            if ((properties.isEmpty()) && !clear) {
                throw new IllegalArgumentException("If you want to clear the properties you have to use --clear");
            }
            if (clear && !properties.isEmpty()) {
                throw new IllegalArgumentException("If you set --clear then you should not pass any properties");
            }
            getTopics().updateSubscriptionProperties(topic, subscriptionName, properties);
        }
    }

    @Command(description = "Get the properties of a subscription on a topic")
    private class GetSubscriptionProperties extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to describe", required = true)
        private String subscriptionName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            Map<String, String> result = getTopics().getSubscriptionProperties(topic, subscriptionName);
            // Ensure we are using JSON and not Java toString()
            System.out.println(ObjectMapperFactory.getMapper().writer().writeValueAsString(result));
        }
    }


    @Command(description = "Reset position for subscription to a position that is closest to "
            + "timestamp or messageId.")
    private class ResetCursor extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-s",
                "--subscription"}, description = "Subscription to reset position on", required = true)
        private String subName;

        @Option(names = { "--time",
                "-t" }, description = "time in minutes to reset back to "
                + "(or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w)", required = false,
                converter = TimeUnitToMillisConverter.class)
        private Long resetTimeInMillis = null;

        @Option(names = { "--messageId",
                "-m" }, description = "messageId to reset back to ('latest', 'earliest', or 'ledgerId:entryId')")
        private String resetMessageIdStr;

        @Option(names = { "-e", "--exclude-reset-position" },
                description = "Exclude the reset position, start consume messages from the next position.")
        private boolean excludeResetPosition = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
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
            } else if (Objects.nonNull(resetTimeInMillis)) {
                // now - go back time
                long timestamp = System.currentTimeMillis() - resetTimeInMillis;
                getTopics().resetCursor(persistentTopic, subName, timestamp);
            } else {
                throw new PulsarAdminException(
                        "Either Timestamp (--time) or messageId (--messageId) has to be provided to reset cursor");
            }
        }
    }

    @Command(description = "Terminate a topic and don't allow any more messages to be published")
    private class Terminate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            try {
                MessageId lastMessageId = getTopics().terminateTopicAsync(persistentTopic).get();
                System.out.println("Topic successfully terminated at " + lastMessageId);
            } catch (InterruptedException | ExecutionException e) {
                throw new PulsarAdminException(e);
            }
        }
    }

    @Command(description = "Terminate a partitioned topic and don't allow any more messages to be published")
    private class PartitionedTerminate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException, TimeoutException {
            String persistentTopic = validatePersistentTopic(topicName);
            Map<Integer, MessageId> messageIds = getTopics().terminatePartitionedTopic(persistentTopic);
            for (Map.Entry<Integer, MessageId> entry : messageIds.entrySet()) {
                String topicName = persistentTopic + "-partition-" + entry.getKey();
                System.out.println("Topic " + topicName + " successfully terminated at " + entry.getValue());
            }
        }
    }

    @Command(description = "Peek some messages for the subscription")
    private class PeekMessages extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to get messages from", required = true)
        private String subName;

        @Option(names = { "-n", "--count" }, description = "Number of messages (default 1)", required = false)
        private int numMessages = 1;

        @Option(names = { "-ssm", "--show-server-marker" },
                description = "Enables the display of internal server write markers.", required = false)
        private boolean showServerMarker = false;

        @Option(names = { "-til", "--transaction-isolation-level" },
                description = "Sets the isolation level for peeking messages within transactions. "
                   + "'READ_COMMITTED' allows peeking only committed transactional messages. "
                   + "'READ_UNCOMMITTED' allows peeking all messages, "
                        + "even transactional messages which have been aborted.",
                required = false)
        private TransactionIsolationLevel transactionIsolationLevel = TransactionIsolationLevel.READ_COMMITTED;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            List<Message<byte[]>> messages = getTopics().peekMessages(persistentTopic, subName, numMessages,
                    showServerMarker, transactionIsolationLevel);
            printMessages(messages, showServerMarker, this);
        }
    }


    @Command(description = "Examine a specific message on a topic by position relative to the"
            + " earliest or the latest message.")
    private class ExamineMessages extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-i", "--initialPosition" },
                description = "Relative start position to examine message."
                        + "It can be 'latest' or 'earliest', default is latest")
        private String initialPosition = "latest";

        @Option(names = { "-m", "--messagePosition" },
                description = "The position of messages (default 1)", required = false)
        private long messagePosition = 1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            MessageImpl message =
                    (MessageImpl) getTopics().examineMessage(persistentTopic, initialPosition, messagePosition);

            if (message.getMessageId() instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":"
                        + msgId.getBatchIndex());
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
                    System.out.println("Broker entry metadata timestamp: "
                            + message.getBrokerEntryMetadata().getBrokerTimestamp());
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

    @Command(description = "Get message by its ledgerId and entryId")
    private class GetMessageById extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-l", "--ledgerId" },
            description = "ledger id pointing to the desired ledger",
            required = true)
        private long ledgerId;

        @Option(names = { "-e", "--entryId" },
            description = "entry id pointing to the desired entry",
            required = true)
        private long entryId;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            MessageImpl message = (MessageImpl) getTopics().getMessageById(persistentTopic, ledgerId, entryId);
            if (message == null) {
                System.out.println("Cannot find any messages based on ledgerId:"
                        + ledgerId + " entryId:" + entryId);
            } else {
                if (message.getMessageId() instanceof BatchMessageIdImpl) {
                    BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                    System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":"
                            + msgId.getBatchIndex());
                } else {
                    MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
                    System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
                }

                System.out.println("Publish time: " + message.getPublishTime());
                System.out.println("Event time: " + message.getEventTime());
                System.out.println("Redelivery count: " + message.getRedeliveryCount());

                if (message.getDeliverAtTime() != 0) {
                    System.out.println("Deliver at time: " + message.getDeliverAtTime());
                }

                if (message.getBrokerEntryMetadata() != null) {
                    if (message.getBrokerEntryMetadata().hasBrokerTimestamp()) {
                        System.out.println("Broker entry metadata timestamp: "
                                + message.getBrokerEntryMetadata().getBrokerTimestamp());
                    }
                    if (message.getBrokerEntryMetadata().hasIndex()) {
                        System.out.println("Broker entry metadata index: "
                                + message.getBrokerEntryMetadata().getIndex());
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

    @Command(description = "Get message ID")
    private class GetMessageId extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-d", "--datetime" },
                description = "datetime at or before this messageId. This datetime is in format of "
                        + "ISO_OFFSET_DATE_TIME, e.g. 2021-06-28T16:53:08Z or 2021-06-28T16:53:08.123456789+08:00",
                required = true)
        private String datetime;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            long timestamp = DateFormatter.parse(datetime);
            MessageId messageId = getTopics().getMessageIdByTimestamp(persistentTopic, timestamp);
            if (messageId == null) {
                System.out.println("Cannot find any messages based on timestamp " + timestamp);
            } else {
                print(messageId);
            }
        }
    }

    @Command(description = "Compact a topic")
    private class Compact extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            getTopics().triggerCompaction(persistentTopic);
            System.out.println("Topic compaction requested for " + persistentTopic);
        }
    }

    @Command(description = "Status of compaction on a topic")
    private class CompactionStatusCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-w", "--wait-complete" },
                   description = "Wait for compaction to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

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

    public static void printMessages(List<Message<byte[]>> messages, boolean showServerMarker, CliCommand cli) {
        if (messages == null) {
            return;
        }
        int position = 0;
        for (Message<byte[]> msg : messages) {
            MessageImpl message = (MessageImpl) msg;
            if (++position != 1) {
                System.out.println("-------------------------------------------------------------------------\n");
            }
            if (message.getMessageId() instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl msgId = (BatchMessageIdImpl) message.getMessageId();
                System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":"
                        + msgId.getBatchIndex());
            } else {
                MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
                System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
            }

            System.out.println("Publish time: " + message.getPublishTime());
            System.out.println("Event time: " + message.getEventTime());

            if (message.getDeliverAtTime() != 0) {
                System.out.println("Deliver at time: " + message.getDeliverAtTime());
            }
            MessageMetadata msgMetaData = message.getMessageBuilder();
            if (showServerMarker && msgMetaData.hasMarkerType()) {
                System.out.println("Marker Type: " + MarkerType.valueOf(msgMetaData.getMarkerType()));
            }

            if (message.getBrokerEntryMetadata() != null) {
                if (message.getBrokerEntryMetadata().hasBrokerTimestamp()) {
                    System.out.println("Broker entry metadata timestamp: "
                            + message.getBrokerEntryMetadata().getBrokerTimestamp());
                }
                if (message.getBrokerEntryMetadata().hasIndex()) {
                    System.out.println("Broker entry metadata index: " + message.getBrokerEntryMetadata().getIndex());
                }
            }

            if (message.getProperties().size() > 0) {
                System.out.println("Properties:");
                cli.print(msg.getProperties());
            }
            ByteBuf data = Unpooled.wrappedBuffer(msg.getData());
            System.out.println(ByteBufUtil.prettyHexDump(data));
        }
    }

    @Command(description = "Trigger offload of data from a topic to long-term storage (e.g. Amazon S3)")
    private class Offload extends CliCommand {
        @Option(names = { "-s", "--size-threshold" },
                description = "Maximum amount of data to keep in BookKeeper for the specified topic (e.g. 10M, 5G).",
                required = true,
                converter = ByteUnitToLongConverter.class)
        private Long sizeThreshold;

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().triggerOffload(persistentTopic, sizeThreshold);
            System.out.println("Offload triggered for " + persistentTopic + " which keep "
                    + sizeThreshold + " bytes on bookkeeper");
        }
    }

    @Command(description = "Check the status of data offloading from a topic to long-term storage")
    private class OffloadStatusCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-w", "--wait-complete" },
                   description = "Wait for offloading to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

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

    @Command(description = "get the last commit message id of topic")
    private class GetLastMessageId extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getLastMessageId(persistentTopic));
        }
    }

    @Command(description = "Get the backlog quota policies for a topic", hidden = true)
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getBacklogQuotaMap(persistentTopic, applied));
        }
    }

    @Command(description = "Set a backlog quota policy for a topic", hidden = true)
    private class SetBacklogQuota extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-l", "--limit" }, description = "Size limit (eg: 10M, 16G)",
                    converter = ByteUnitToLongConverter.class)
        private Long limit = -1L;

        @Option(names = { "-lt", "--limitTime" },
                description = "Time limit in second (or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w), "
                        + "non-positive number for disabling time limit.",
                converter = TimeUnitToSecondsConverter.class)
        private Long limitTimeInSec = -1L;

        @Option(names = { "-p", "--policy" },
                description = "Retention policy to enforce when the limit is reached. Valid options are: "
                        + "[producer_request_hold, producer_exception, consumer_backlog_eviction]", required = true)
        private String policyStr;

        @Option(names = {"-t", "--type"}, description = "Backlog quota type to set. Valid options are: "
                + "destination_storage and message_age. "
                + "destination_storage limits backlog by size (in bytes). "
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

            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setBacklogQuota(persistentTopic,
                    BacklogQuota.builder().limitSize(limit)
                            .limitTime(limitTimeInSec.intValue())
                            .retentionPolicy(policy)
                            .build(),
                    backlogQuotaType);
        }
    }

    @Command(description = "Remove a backlog quota policy from a topic", hidden = true)
    private class RemoveBacklogQuota extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-t", "--type"}, description = "Backlog quota type to remove")
        private String backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeBacklogQuota(persistentTopic, BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaType));
        }
    }

    @Command(description = "Get the replication clusters for a topic")
    private class GetReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getReplicationClusters(persistentTopic, applied));
        }
    }

    @Command(description = "Set the replication clusters for a topic")
    private class SetReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            getTopics().setReplicationClusters(persistentTopic, clusters);
        }
    }

    @Command(description = "Remove the replication clusters for a topic")
    private class RemoveReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeReplicationClusters(persistentTopic);
        }
    }

    @Command(description = "Get the shadow topics for a topic")
    private class GetShadowTopics extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getShadowTopics(persistentTopic));
        }
    }

    @Command(description = "Set the shadow topics for a topic")
    private class SetShadowTopics extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--topics",
                "-t" }, description = "Shadow topic list (comma separated values)", required = true)
        private String shadowTopics;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            List<String> topics = Lists.newArrayList(shadowTopics.split(","));
            getTopics().setShadowTopics(persistentTopic, topics);
        }
    }

    @Command(description = "Remove the shadow topics for a topic")
    private class RemoveShadowTopics extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeShadowTopics(persistentTopic);
        }
    }

    @Command(description = "Create a shadow topic for an existing source topic.")
    private class CreateShadowTopic extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--source", "-s"}, description = "source topic name", required = true)
        private String sourceTopic;

        @Option(names = {"--properties", "-p"}, description = "key value pair properties(eg: a=a,b=b,c=c)", split = ",")
        private Map<String, String> properties;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getTopics().createShadowTopic(topic, TopicName.get(sourceTopic).toString(), properties);
        }
    }

    @Command(description = "Get the source topic for a shadow topic")
    private class GetShadowSource extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;


        @Override
        void run() throws PulsarAdminException {
            String shadowTopic = validatePersistentTopic(topicName);
            print(getTopics().getShadowSource(shadowTopic));
        }
    }

    @Command(description = "Get the delayed delivery policy for a topic", hidden = true)
    private class GetDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getTopics().getDelayedDeliveryPolicy(topic, applied));
        }
    }

    @Command(description = "Set the delayed delivery policy on a topic", hidden = true)
    private class SetDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable", "-e" }, description = "Enable delayed delivery messages")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable delayed delivery messages")
        private boolean disable = false;

        @Option(names = { "--time", "-t" },
                description = "The tick time for when retrying on delayed delivery messages, affecting the accuracy of "
                        + "the delivery time compared to the scheduled time. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryTimeInMills = 1_000L;

        @Option(names = {"--maxDelay", "-md"},
                description = "The max allowed delay for delayed delivery. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryMaxDelayInMillis = 0L;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            getTopics().setDelayedDeliveryPolicy(topic, DelayedDeliveryPolicies.builder()
                    .tickTime(delayedDeliveryTimeInMills)
                    .active(enable)
                    .maxDeliveryDelayInMillis(delayedDeliveryMaxDelayInMillis)
                    .build());
        }
    }

    @Command(description = "Remove the delayed delivery policy on a topic", hidden = true)
    private class RemoveDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topic;

        @Override
        void run() throws PulsarAdminException {
            String topicName = validateTopicName(topic);
            getTopics().removeDelayedDeliveryPolicy(topicName);
        }
    }

    @Command(description = "Get the message TTL for a topic", hidden = true)
    private class GetMessageTTL extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMessageTTL(persistentTopic, applied));
        }
    }

    @Command(description = "Set message TTL for a topic", hidden = true)
    private class SetMessageTTL extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-t", "--ttl" }, description = "Message TTL for topic in second "
                + "(or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w), "
                + "allowed range from 1 to Integer.MAX_VALUE", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long messageTTLInSecond;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMessageTTL(persistentTopic, messageTTLInSecond.intValue());
        }
    }

    @Command(description = "Remove message TTL for a topic", hidden = true)
    private class RemoveMessageTTL extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMessageTTL(persistentTopic);
        }
    }

    @Command(description = "Get deduplication snapshot interval for a topic", hidden = true)
    private class GetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getDeduplicationSnapshotInterval(persistentTopic));
        }
    }

    @Command(description = "Set deduplication snapshot interval for a topic", hidden = true)
    private class SetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-i", "--interval" }, description = "Deduplication snapshot interval for topic in second, "
                + "allowed range from 0 to Integer.MAX_VALUE", required = true)
        private int interval;

        @Override
        void run() throws PulsarAdminException {
            if (interval < 0) {
                throw new IllegalArgumentException(String.format("Invalid interval '%d'. ", interval));
            }

            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setDeduplicationSnapshotInterval(persistentTopic, interval);
        }
    }

    @Command(description = "Remove deduplication snapshot interval for a topic", hidden = true)
    private class RemoveDeduplicationSnapshotInterval extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeDeduplicationSnapshotInterval(persistentTopic);
        }
    }

    @Command(description = "Get the retention policy for a topic")
    private class GetRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getRetention(persistentTopic, applied));
        }
    }

    @Command(description = "Set the retention policy for a topic", hidden = true)
    private class SetRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--time",
                "-t" }, description = "Retention time with optional time unit suffix. "
                + "For example, 100m, 3h, 2d, 5w. "
                + "If the time unit is not specified, the default unit is seconds. For example, "
                + "-t 120 will set retention to 2 minutes. "
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
            String persistentTopic = validatePersistentTopic(topicName);
            final int retentionTimeInMin = retentionTimeInSec != -1
                    ? (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec)
                    : retentionTimeInSec.intValue();
            final long retentionSizeInMB = sizeLimit != -1
                    ? (sizeLimit / (1024 * 1024))
                    : sizeLimit;
            getTopics().setRetention(persistentTopic, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Deprecated
    @Command(description = "Enable the deduplication policy for a topic", hidden = true)
    private class EnableDeduplication extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().enableDeduplication(persistentTopic, true);
        }
    }

    @Deprecated
    @Command(description = "Disable the deduplication policy for a topic", hidden = true)
    private class DisableDeduplication extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().enableDeduplication(persistentTopic, false);
        }
    }

    @Command(description = "Enable or disable deduplication for a topic", hidden = true)
    private class SetDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            if (enable == disable) {
                throw new IllegalArgumentException("Need to specify either --enable or --disable");
            }
            getTopics().setDeduplicationStatus(persistentTopic, enable);
        }
    }

    @Command(description = "Get the deduplication policy for a topic", hidden = true)
    private class GetDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getDeduplicationStatus(persistentTopic));
        }
    }

    @Command(description = "Remove the deduplication policy for a topic", hidden = true)
    private class RemoveDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeDeduplicationStatus(persistentTopic);
        }
    }

    @Command(description = "Remove the retention policy for a topic", hidden = true)
    private class RemoveRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeRetention(persistentTopic);
        }
    }

    @Command(description = "Get the persistence policies for a topic", hidden = true)
    private class GetPersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getPersistence(persistentTopic));
        }
    }

    @Command(description = "Get the offload policies for a topic", hidden = true)
    private class GetOffloadPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getOffloadPolicies(persistentTopic, applied));
        }
    }

    @Command(description = "Remove the offload policies for a topic", hidden = true)
    private class RemoveOffloadPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeOffloadPolicies(persistentTopic);
        }
    }

    @Command(description = "Set the offload policies for a topic", hidden = true)
    private class SetOffloadPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-d", "--driver"}, description = "ManagedLedger offload driver", required = true)
        private String driver;

        @Option(names = {"-r", "--region"}
                , description = "ManagedLedger offload region, s3 and google-cloud-storage requires this parameter")
        private String region;

        @Option(names = {"-b", "--bucket"}
                , description = "ManagedLedger offload bucket, s3 and google-cloud-storage requires this parameter")
        private String bucket;

        @Option(names = {"-e", "--endpoint"}
                , description = "ManagedLedger offload service endpoint, only s3 requires this parameter")
        private String endpoint;

        @Option(names = {"-i", "--aws-id"}
                , description = "AWS Credential Id to use when using driver S3 or aws-s3")
        private String awsId;

        @Option(names = {"-s", "--aws-secret"}
                , description = "AWS Credential Secret to use when using driver S3 or aws-s3")
        private String awsSecret;

        @Option(names = {"--ro", "--s3-role"}
                , description = "S3 Role used for STSAssumeRoleSessionCredentialsProvider")
        private String s3Role;

        @Option(names = {"--s3-role-session-name", "-rsn"}
                , description = "S3 role session name used for STSAssumeRoleSessionCredentialsProvider")
        private String s3RoleSessionName;

        @Option(
                names = {"-m", "--maxBlockSizeInBytes", "--maxBlockSize", "-mbs"},
                description = "Max block size (eg: 32M, 64M), default is 64MB"
                + "s3 and google-cloud-storage requires this parameter",
                required = false,
                converter = ByteUnitToIntegerConverter.class)
        private Integer maxBlockSizeInBytes = OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

        @Option(
                names = {"-rb", "--readBufferSizeInBytes", "--readBufferSize", "-rbs"},
                description = "Read buffer size (eg: 1M, 5M), default is 1MB"
                + "s3 and google-cloud-storage requires this parameter",
                required = false,
                converter = ByteUnitToIntegerConverter.class)
        private Integer readBufferSizeInBytes = OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES;

        @Option(names = {"-t", "--offloadThresholdInBytes", "--offloadAfterThreshold", "-oat"}
                , description = "Offload after threshold size (eg: 1M, 5M)", required = false,
                converter = ByteUnitToLongConverter.class)
        private Long offloadAfterThresholdInBytes = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;

        @Option(names = {"-ts", "--offloadThresholdInSeconds", "--offloadAfterThresholdInSeconds", "-oats"},
                  description = "Offload after threshold seconds (or minutes,hours,days,weeks eg: 100m, 3h, 2d, 5w).",
                    converter = TimeUnitToSecondsConverter.class)
        private Long offloadThresholdInSeconds = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS;

        @Option(names = {"-dl", "--offloadDeletionLagInMillis", "--offloadAfterElapsed", "-oae"}
                , description = "Delay time in Millis for deleting the bookkeeper ledger after offload "
              + "(or seconds,minutes,hours,days,weeks eg: 10s, 100m, 3h, 2d, 5w).",
                converter = TimeUnitToMillisConverter.class)
        private Long offloadAfterElapsedInMillis = OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

        @Option(names = {"--offloadedReadPriority", "-orp"},
                description = "Read priority for offloaded messages. "
                        + "By default, once messages are offloaded to long-term storage, "
                        + "brokers read messages from long-term storage, but messages can still exist in BookKeeper "
                        + "for a period depends on your configuration. For messages that exist in both "
                        + "long-term storage and BookKeeper, you can set where to read messages from with the option "
                        + "`tiered-storage-first` or `bookkeeper-first`."
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
            String persistentTopic = validatePersistentTopic(topicName);

            if (!driverSupported(driver)) {
                throw new ParameterException(
                  "The driver " + driver + " is not supported, "
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

            getTopics().setOffloadPolicies(persistentTopic, offloadPolicies);
        }
    }

    @Command(description = "Set the persistence policies for a topic", hidden = true)
    private class SetPersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic")
        private int bookkeeperEnsemble = 2;

        @Option(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry")
        private int bookkeeperWriteQuorum = 2;

        @Option(names = { "-a",
                "--bookkeeper-ack-quorum" }, description = "Number of acks (guaranteed copies) to wait for each entry")
        private int bookkeeperAckQuorum = 2;

        @Option(names = { "-r",
                "--ml-mark-delete-max-rate" }, description = "Throttling rate of mark-delete operation "
                + "(0 means no throttle)")
        private double managedLedgerMaxMarkDeleteRate = 0;

        @Option(names = { "-c",
                "--ml-storage-class" },
                description = "Managed ledger storage class name")
        private String managedLedgerStorageClassName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (bookkeeperEnsemble <= 0 || bookkeeperWriteQuorum <= 0 || bookkeeperAckQuorum <= 0) {
                throw new ParameterException("[--bookkeeper-ensemble], [--bookkeeper-write-quorum] "
                        + "and [--bookkeeper-ack-quorum] must greater than 0.");
            }
            if (managedLedgerMaxMarkDeleteRate < 0) {
                throw new ParameterException("[--ml-mark-delete-max-rate] cannot less than 0.");
            }
            getTopics().setPersistence(persistentTopic, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate,
                    managedLedgerStorageClassName));
        }
    }

    @Command(description = "Remove the persistence policy for a topic", hidden = true)
    private class RemovePersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removePersistence(persistentTopic);
        }
    }

    @Command(description = "Get message dispatch rate for a topic", hidden = true)
    private class GetDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getDispatchRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set message dispatch rate for a topic", hidden = true)
    private class SetDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)")
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
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove message dispatch rate for a topic", hidden = true)
    private class RemoveDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeDispatchRate(persistentTopic);
        }
    }

    @Command(description = "Get max unacked messages policy on consumer for a topic", hidden = true)
    private class GetMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxUnackedMessagesOnConsumer(persistentTopic, applied));
        }
    }

    @Command(description = "Remove max unacked messages policy on consumer for a topic", hidden = true)
    private class RemoveMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxUnackedMessagesOnConsumer(persistentTopic);
        }
    }

    @Command(description = "Set max unacked messages policy on consumer for a topic", hidden = true)
    private class SetMaxUnackedMessagesOnConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-m", "--maxNum"}, description = "max unacked messages num on consumer", required = true)
        private int maxNum;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxUnackedMessagesOnConsumer(persistentTopic, maxNum);
        }
    }

    @Command(description = "Get max unacked messages policy on subscription for a topic", hidden = true)
    private class GetMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxUnackedMessagesOnSubscription(persistentTopic, applied));
        }
    }

    @Command(description = "Remove max unacked messages policy on subscription for a topic", hidden = true)
    private class RemoveMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxUnackedMessagesOnSubscription(persistentTopic);
        }
    }

    @Command(description = "Set max unacked messages policy on subscription for a topic", hidden = true)
    private class SetMaxUnackedMessagesOnSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-m", "--maxNum"},
                description = "max unacked messages num on subscription", required = true)
        private int maxNum;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxUnackedMessagesOnSubscription(persistentTopic, maxNum);
        }
    }

    @Command(description = "Set subscription types enabled for a topic", hidden = true)
    private class SetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--types", "-t"}, description = "Subscription types enabled list (comma separated values)."
                + " Possible values: (Exclusive, Shared, Failover, Key_Shared).", required = true, split = ",")
        private List<String> subTypes;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
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

    @Command(description = "Get subscription types enabled for a topic", hidden = true)
    private class GetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getSubscriptionTypesEnabled(persistentTopic));
        }
    }

    @Command(description = "Remove subscription types enabled for a topic", hidden = true)
    private class RemoveSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeSubscriptionTypesEnabled(persistentTopic);
        }
    }

    @Command(description = "Get compaction threshold for a topic", hidden = true)
    private class GetCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getCompactionThreshold(persistentTopic, applied));
        }
    }

    @Command(description = "Set compaction threshold for a topic", hidden = true)
    private class SetCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--threshold", "-t" },
            description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
            required = true,
            converter = ByteUnitToLongConverter.class)
        private Long threshold = 0L;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setCompactionThreshold(persistentTopic, threshold);
        }
    }

    @Command(description = "Remove compaction threshold for a topic", hidden = true)
    private class RemoveCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeCompactionThreshold(persistentTopic);
        }
    }

    @Command(description = "Get publish rate for a topic", hidden = true)
    private class GetPublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getPublishRate(persistentTopic));
        }
    }

    @Command(description = "Set publish rate for a topic", hidden = true)
    private class SetPublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--msg-publish-rate",
            "-m" }, description = "message-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private int msgPublishRate = -1;

         @Option(names = { "--byte-publish-rate",
            "-b" }, description = "byte-publish-rate (default -1 will be overwrite if not passed)", required = false)
        private long bytePublishRate = -1;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setPublishRate(persistentTopic,
                new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

    @Command(description = "Remove publish rate for a topic", hidden = true)
    private class RemovePublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removePublishRate(persistentTopic);
        }
    }

    @Command(description = "Get subscription message-dispatch-rate for a topic", hidden = true)
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getSubscriptionDispatchRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set subscription message-dispatch-rate for a topic", hidden = true)
    private class SetSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Option(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type"
                + " (default 1 second will be overwrite if not passed)")
        private int dispatchRatePeriodSec = 1;

        @Option(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setSubscriptionDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove subscription message-dispatch-rate for a topic", hidden = true)
    private class RemoveSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeSubscriptionDispatchRate(persistentTopic);
        }
    }

    @Command(description = "Get replicator message-dispatch-rate for a topic", hidden = true)
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validatePersistentTopic(topicName);
            print(getTopics().getReplicatorDispatchRate(topic, applied));
        }
    }

    @Command(description = "Set replicator message-dispatch-rate for a topic", hidden = true)
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--msg-dispatch-rate",
            "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
            "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)", required = false)
        private long byteDispatchRate = -1;

        @Option(names = { "--dispatch-rate-period",
            "-dt" }, description = "dispatch-rate-period in second type "
            + "(default 1 second will be overwrite if not passed)")
        private int dispatchRatePeriodSec = 1;

        @Option(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setReplicatorDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove replicator message-dispatch-rate for a topic", hidden = true)
    private class RemoveReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeReplicatorDispatchRate(persistentTopic);
        }
    }

    @Command(description = "Get max number of producers for a topic", hidden = true)
    private class GetMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxProducers(persistentTopic, applied));
        }
    }

    @Command(description = "Set max number of producers for a topic", hidden = true)
    private class SetMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-producers", "-p"}, description = "Max producers for a topic", required = true)
        private int maxProducers;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxProducers(persistentTopic, maxProducers);
        }
    }

    @Command(description = "Remove max number of producers for a topic", hidden = true)
    private class RemoveMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxProducers(persistentTopic);
        }
    }

    @Command(description = "Get max number of subscriptions for a topic", hidden = true)
    private class GetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxSubscriptionsPerTopic(persistentTopic));
        }
    }

    @Command(description = "Set max number of subscriptions for a topic", hidden = true)
    private class SetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-subscriptions-per-topic", "-m"},
                description = "Maximum subscription limit for a topic", required = true)
        private int maxSubscriptionsPerTopic;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxSubscriptionsPerTopic(persistentTopic, maxSubscriptionsPerTopic);
        }
    }

    @Command(description = "Remove max number of subscriptions for a topic", hidden = true)
    private class RemoveMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxSubscriptionsPerTopic(persistentTopic);
        }
    }

    @Command(description = "Get max message size for a topic", hidden = true)
    private class GetMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxMessageSize(persistentTopic));
        }
    }

    @Command(description = "Set max message size for a topic", hidden = true)
    private class SetMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-message-size", "-m"}, description = "Max message size for a topic", required = true)
        private int maxMessageSize;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxMessageSize(persistentTopic, maxMessageSize);
        }
    }

    @Command(description = "Remove max message size for a topic", hidden = true)
    private class RemoveMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxMessageSize(persistentTopic);
        }
    }

    @Command(description = "Get max consumers per subscription for a topic", hidden = true)
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxConsumersPerSubscription(persistentTopic));
        }
    }

    @Command(description = "Set max consumers per subscription for a topic", hidden = true)
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--max-consumers-per-subscription", "-c" },
                description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxConsumersPerSubscription(persistentTopic, maxConsumersPerSubscription);
        }
    }

    @Command(description = "Remove max consumers per subscription for a topic", hidden = true)
    private class RemoveMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxConsumersPerSubscription(persistentTopic);
        }
    }

    @Command(description = "Get the inactive topic policies on a topic", hidden = true)
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getInactiveTopicPolicies(persistentTopic, applied));
        }
    }

    @Command(description = "Set the inactive topic policies on a topic", hidden = true)
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable-delete-while-inactive", "-e" }, description = "Enable delete while inactive")
        private boolean enableDeleteWhileInactive = false;

        @Option(names = { "--disable-delete-while-inactive", "-d" }, description = "Disable delete while inactive")
        private boolean disableDeleteWhileInactive = false;

        @Option(names = {"--max-inactive-duration", "-t"}, description = "Max duration of topic inactivity "
                + "in seconds, topics that are inactive for longer than this value will be deleted "
                + "(eg: 1s, 10s, 1m, 5h, 3d)", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long maxInactiveDurationInSeconds;

        @Option(names = { "--delete-mode", "-m" }, description = "Mode of delete inactive topic, Valid options are: "
                + "[delete_when_no_subscriptions, delete_when_subscriptions_caught_up]", required = true)
        private String inactiveTopicDeleteMode;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new IllegalArgumentException("Need to specify either enable-delete-while-inactive "
                        + "or disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode = null;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions "
                        + "or delete_when_subscriptions_caught_up");
            }
            getTopics().setInactiveTopicPolicies(persistentTopic, new InactiveTopicPolicies(deleteMode,
                    maxInactiveDurationInSeconds.intValue(), enableDeleteWhileInactive));
        }
    }

    @Command(description = "Remove inactive topic policies from a topic", hidden = true)
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeInactiveTopicPolicies(persistentTopic);
        }
    }

    @Command(description = "Get max number of consumers for a topic", hidden = true)
    private class GetMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getMaxConsumers(persistentTopic, applied));
        }
    }

    @Command(description = "Set max number of consumers for a topic", hidden = true)
    private class SetMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--max-consumers", "-c" }, description = "Max consumers for a topic", required = true)
        private int maxConsumers;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setMaxConsumers(persistentTopic, maxConsumers);
        }
    }

    @Command(description = "Remove max number of consumers for a topic", hidden = true)
    private class RemoveMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeMaxConsumers(persistentTopic);
        }
    }

    @Command(description = "Get consumer subscribe rate for a topic", hidden = true)
    private class GetSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getSubscribeRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set consumer subscribe rate for a topic", hidden = true)
    private class SetSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)", required = false)
        private int subscribeRate = -1;

        @Option(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type "
                + "(default 30 second will be overwrite if not passed)")
        private int subscribeRatePeriodSec = 30;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().setSubscribeRate(persistentTopic,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Command(description = "Remove consumer subscribe rate for a topic", hidden = true)
    private class RemoveSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopics().removeSubscribeRate(persistentTopic);
        }
    }

    @Command(description = "Enable or disable a replicated subscription on a topic")
    private class SetReplicatedSubscriptionStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription name to enable or disable replication", required = true)
        private String subName;

        @Option(names = { "--enable", "-e" }, description = "Enable replication")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable replication")
        private boolean disable = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (enable == disable) {
                throw new IllegalArgumentException("Need to specify either --enable or --disable");
            }
            getTopics().setReplicatedSubscriptionStatus(persistentTopic, subName, enable);
        }
    }

    @Command(description = "Get replicated subscription status on a topic")
    private class GetReplicatedSubscriptionStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-s",
                "--subscription"}, description = "Subscription name", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopics().getReplicatedSubscriptionStatus(persistentTopic, subName));
        }
    }

    private Topics getTopics() {
        return getAdmin().topics();
    }

    @Command(description = "Calculate backlog size by a message ID (in bytes).")
    private class GetBacklogSizeByMessageId extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--messageId",
                "-m" }, description = "messageId used to calculate backlog size. It can be (ledgerId:entryId).")
        private String messagePosition = "-1:-1";

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            MessageId messageId;
            if ("-1:-1".equals(messagePosition)) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messagePosition);
            }
            print(getTopics().getBacklogSizeByMessageId(persistentTopic, messageId));

        }
    }


    @Command(description = "Analyze the backlog of a subscription.")
    private class AnalyzeBacklog extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s", "--subscription" }, description = "Subscription to be analyzed", required = true)
        private String subName;

        @Option(names = { "--position",
                "-p" }, description = "message position to start the scan from (ledgerId:entryId)", required = false)
        private String messagePosition;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            Optional<MessageId> startPosition = Optional.empty();
            if (isNotBlank(messagePosition)) {
                int partitionIndex = TopicName.get(persistentTopic).getPartitionIndex();
                MessageId messageId = validateMessageIdString(messagePosition, partitionIndex);
                startPosition = Optional.of(messageId);
            }
            print(getTopics().analyzeSubscriptionBacklog(persistentTopic, subName, startPosition));

        }
    }

    @Command(description = "Get the schema validation enforced")
    private class GetSchemaValidationEnforced extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            System.out.println(getAdmin().topics().getSchemaValidationEnforced(topic, applied));
        }
    }

    @Command(description = "Set the schema whether open schema validation enforced")
    private class SetSchemaValidationEnforced extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable", "-e" }, description = "Enable schema validation enforced")
        private boolean enable = false;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getAdmin().topics().setSchemaValidationEnforced(topic, enable);
        }
    }

    @Command(description = "Trim a topic")
    private class TrimTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getAdmin().topics().trimTopic(topic);
        }
    }
}
