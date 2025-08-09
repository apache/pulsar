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

import static org.apache.pulsar.admin.cli.utils.CmdUtils.maxValueCheck;
import static org.apache.pulsar.admin.cli.utils.CmdUtils.positiveCheck;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToSecondsConverter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations on persistent topics")
public class CmdTopicPolicies extends CmdBase {

    public CmdTopicPolicies(Supplier<PulsarAdmin> admin) {
        super("topicPolicies", admin);
        addCommand("delete", new DeletePolicies());
        addCommand("get-message-ttl", new GetMessageTTL());
        addCommand("set-message-ttl", new SetMessageTTL());
        addCommand("remove-message-ttl", new RemoveMessageTTL());

        addCommand("get-max-unacked-messages-per-consumer", new GetMaxUnackedMessagesPerConsumer());
        addCommand("set-max-unacked-messages-per-consumer", new SetMaxUnackedMessagesPerConsumer());
        addCommand("remove-max-unacked-messages-per-consumer", new RemoveMaxUnackedMessagesPerConsumer());

        addCommand("get-max-consumers-per-subscription", new GetMaxConsumersPerSubscription());
        addCommand("set-max-consumers-per-subscription", new SetMaxConsumersPerSubscription());
        addCommand("remove-max-consumers-per-subscription", new RemoveMaxConsumersPerSubscription());
        addCommand("set-subscription-types-enabled", new SetSubscriptionTypesEnabled());
        addCommand("get-subscription-types-enabled", new GetSubscriptionTypesEnabled());
        addCommand("remove-subscription-types-enabled", new RemoveSubscriptionTypesEnabled());
        addCommand("get-retention", new GetRetention());
        addCommand("set-retention", new SetRetention());
        addCommand("remove-retention", new RemoveRetention());
        addCommand("get-backlog-quota", new GetBacklogQuotaMap());
        addCommand("set-backlog-quota", new SetBacklogQuota());
        addCommand("remove-backlog-quota", new RemoveBacklogQuota());

        addCommand("get-max-producers", new GetMaxProducers());
        addCommand("set-max-producers", new SetMaxProducers());
        addCommand("remove-max-producers", new RemoveMaxProducers());

        addCommand("get-max-message-size", new GetMaxMessageSize());
        addCommand("set-max-message-size", new SetMaxMessageSize());
        addCommand("remove-max-message-size", new RemoveMaxMessageSize());

        addCommand("set-deduplication", new SetDeduplicationStatus());
        addCommand("get-deduplication", new GetDeduplicationStatus());
        addCommand("remove-deduplication", new RemoveDeduplicationStatus());

        addCommand("get-deduplication-snapshot-interval", new GetDeduplicationSnapshotInterval());
        addCommand("set-deduplication-snapshot-interval", new SetDeduplicationSnapshotInterval());
        addCommand("remove-deduplication-snapshot-interval", new RemoveDeduplicationSnapshotInterval());

        addCommand("get-persistence", new GetPersistence());
        addCommand("set-persistence", new SetPersistence());
        addCommand("remove-persistence", new RemovePersistence());

        addCommand("get-subscription-dispatch-rate", new GetSubscriptionDispatchRate());
        addCommand("set-subscription-dispatch-rate", new SetSubscriptionDispatchRate());
        addCommand("remove-subscription-dispatch-rate", new RemoveSubscriptionDispatchRate());

        addCommand("get-replicator-dispatch-rate", new GetReplicatorDispatchRate());
        addCommand("set-replicator-dispatch-rate", new SetReplicatorDispatchRate());
        addCommand("remove-replicator-dispatch-rate", new RemoveReplicatorDispatchRate());

        addCommand("get-publish-rate", new GetPublishRate());
        addCommand("set-publish-rate", new SetPublishRate());
        addCommand("remove-publish-rate", new RemovePublishRate());

        addCommand("get-compaction-threshold", new GetCompactionThreshold());
        addCommand("set-compaction-threshold", new SetCompactionThreshold());
        addCommand("remove-compaction-threshold", new RemoveCompactionThreshold());

        addCommand("get-subscribe-rate", new GetSubscribeRate());
        addCommand("set-subscribe-rate", new SetSubscribeRate());
        addCommand("remove-subscribe-rate", new RemoveSubscribeRate());

        addCommand("get-max-consumers", new GetMaxConsumers());
        addCommand("set-max-consumers", new SetMaxConsumers());
        addCommand("remove-max-consumers", new RemoveMaxConsumers());

        addCommand("get-delayed-delivery", new GetDelayedDelivery());
        addCommand("set-delayed-delivery", new SetDelayedDelivery());
        addCommand("remove-delayed-delivery", new RemoveDelayedDelivery());

        addCommand("get-dispatch-rate", new GetDispatchRate());
        addCommand("set-dispatch-rate", new SetDispatchRate());
        addCommand("remove-dispatch-rate", new RemoveDispatchRate());

        addCommand("get-offload-policies", new GetOffloadPolicies());
        addCommand("set-offload-policies", new SetOffloadPolicies());
        addCommand("remove-offload-policies", new RemoveOffloadPolicies());

        addCommand("get-max-unacked-messages-per-subscription", new GetMaxUnackedMessagesPerSubscription());
        addCommand("set-max-unacked-messages-per-subscription", new SetMaxUnackedMessagesPerSubscription());
        addCommand("remove-max-unacked-messages-per-subscription",
                new RemoveMaxUnackedMessagesPerSubscription());

        addCommand("get-inactive-topic-policies", new GetInactiveTopicPolicies());
        addCommand("set-inactive-topic-policies", new SetInactiveTopicPolicies());
        addCommand("remove-inactive-topic-policies", new RemoveInactiveTopicPolicies());

        addCommand("get-max-subscriptions-per-topic", new GetMaxSubscriptionsPerTopic());
        addCommand("set-max-subscriptions-per-topic", new SetMaxSubscriptionsPerTopic());
        addCommand("remove-max-subscriptions-per-topic", new RemoveMaxSubscriptionsPerTopic());

        addCommand("remove-schema-compatibility-strategy", new RemoveSchemaCompatibilityStrategy());
        addCommand("set-schema-compatibility-strategy", new SetSchemaCompatibilityStrategy());
        addCommand("get-schema-compatibility-strategy", new GetSchemaCompatibilityStrategy());

        addCommand("get-entry-filters-per-topic", new GetEntryFiltersPerTopic());
        addCommand("set-entry-filters-per-topic", new SetEntryFiltersPerTopic());
        addCommand("remove-entry-filters-per-topic", new RemoveEntryFiltersPerTopic());

        addCommand("set-auto-subscription-creation", new SetAutoSubscriptionCreation());
        addCommand("get-auto-subscription-creation", new GetAutoSubscriptionCreation());
        addCommand("remove-auto-subscription-creation", new RemoveAutoSubscriptionCreation());

        addCommand("set-dispatcher-pause-on-ack-state-persistent",
                new SetDispatcherPauseOnAckStatePersistent());
        addCommand("get-dispatcher-pause-on-ack-state-persistent",
                new GetDispatcherPauseOnAckStatePersistent());
        addCommand("remove-dispatcher-pause-on-ack-state-persistent",
                new RemoveDispatcherPauseOnAckStatePersistent());

        addCommand("get-replication-clusters", new GetReplicationClusters());
        addCommand("set-replication-clusters", new SetReplicationClusters());
        addCommand("remove-replication-clusters", new RemoveReplicationClusters());
    }

    @Command(description = "Get entry filters for a topic")
    private class GetEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getEntryFiltersPerTopic(persistentTopic, applied));
        }
    }

    @Command(description = "Set entry filters for a topic")
    private class SetEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;


        @Option(names = { "--entry-filters-name", "-efn" },
                description = "The class name for the entry filter.", required = true)
        private String  entryFiltersName = "";

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setEntryFiltersPerTopic(persistentTopic, new EntryFilters(entryFiltersName));
        }
    }

    @Command(description = "Remove entry filters for a topic")
    private class RemoveEntryFiltersPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeEntryFiltersPerTopic(persistentTopic);
        }
    }

    @Command(description = "Get max consumers per subscription for a topic")
    private class GetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxConsumersPerSubscription(persistentTopic));
        }
    }

    @Command(description = "Set max consumers per subscription for a topic")
    private class SetMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--max-consumers-per-subscription", "-c" },
                description = "maxConsumersPerSubscription for a namespace", required = true)
        private int maxConsumersPerSubscription;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxConsumersPerSubscription(persistentTopic, maxConsumersPerSubscription);
        }
    }

    @Command(description = "Remove max consumers per subscription for a topic")
    private class RemoveMaxConsumersPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxConsumersPerSubscription(persistentTopic);
        }
    }

    @Command(description = "Get max unacked messages policy per consumer for a topic")
    private class GetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxUnackedMessagesOnConsumer(persistentTopic, applied));
        }
    }

    @Command(description = "Remove max unacked messages policy per consumer for a topic")
    private class RemoveMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxUnackedMessagesOnConsumer(persistentTopic);
        }
    }

    @Command(description = "Set max unacked messages policy per consumer for a topic")
    private class SetMaxUnackedMessagesPerConsumer extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-m", "--maxNum"}, description = "max unacked messages num on consumer", required = true)
        private int maxNum;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxUnackedMessagesOnConsumer(persistentTopic, maxNum);
        }
    }

    @Command(description = "Get the message TTL for a topic")
    private class GetMessageTTL extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMessageTTL(persistentTopic, applied));
        }
    }

    @Command(description = "Set message TTL for a topic")
    private class SetMessageTTL extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-t", "--ttl" },
                description = "Message TTL for topic in seconds (or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w), "
                        + "allowed range from 1 to Integer.MAX_VALUE", required = true,
                    converter = TimeUnitToSecondsConverter.class)
        private Long messageTTLInSecond;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMessageTTL(persistentTopic, messageTTLInSecond.intValue());
        }
    }

    @Command(description = "Remove message TTL for a topic")
    private class RemoveMessageTTL extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMessageTTL(persistentTopic);
        }
    }

    @Command(description = "Set subscription types enabled for a topic")
    private class SetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--types", "-t"}, description = "Subscription types enabled list (comma separated values)."
                + " Possible values: (Exclusive, Shared, Failover, Key_Shared).", required = true, split = ",")
        private List<String> subTypes;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

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
            getTopicPolicies(isGlobal).setSubscriptionTypesEnabled(persistentTopic, types);
        }
    }

    @Command(description = "Get subscription types enabled for a topic")
    private class GetSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getSubscriptionTypesEnabled(persistentTopic));
        }
    }

    @Command(description = "Remove subscription types enabled for a topic")
    private class RemoveSubscriptionTypesEnabled extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeSubscriptionTypesEnabled(persistentTopic);
        }
    }

    @Command(description = "Get max number of consumers for a topic")
    private class GetMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxConsumers(persistentTopic, applied));
        }
    }

    @Command(description = "Set max number of consumers for a topic")
    private class SetMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--max-consumers", "-c" }, description = "Max consumers for a topic", required = true)
        private int maxConsumers;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxConsumers(persistentTopic, maxConsumers);
        }
    }

    @Command(description = "Remove max number of consumers for a topic")
    private class RemoveMaxConsumers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxConsumers(persistentTopic);
        }
    }

    @Command(description = "Get the retention policy for a topic")
    private class GetRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the broker returns global topic policies"
                + "If set to false or not set, the broker returns local topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getRetention(persistentTopic, applied));
        }
    }

    @Command(description = "Set the retention policy for a topic")
    private class SetRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

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

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy is replicated to other clusters asynchronously, "
                + "If set to false or not set, the topic retention policy is replicated to local clusters.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            final int retentionTimeInMin = retentionTimeInSec != -1
                    ? (int) TimeUnit.SECONDS.toMinutes(retentionTimeInSec)
                    : retentionTimeInSec.intValue();
            final long retentionSizeInMB = sizeLimit != -1
                    ? (sizeLimit / (1024 * 1024))
                    : sizeLimit;
            getTopicPolicies(isGlobal).setRetention(persistentTopic,
                    new RetentionPolicies(retentionTimeInMin, retentionSizeInMB));
        }
    }

    @Command(description = "Remove the retention policy for a topic")
    private class RemoveRetention extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation is replicated to other clusters asynchronously"
                + "If set to false or not set, the topic retention policy is replicated to local clusters.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeRetention(persistentTopic);
        }
    }

    @Command(description = "Get max unacked messages policy per subscription for a topic")
    private class GetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxUnackedMessagesOnSubscription(persistentTopic, applied));
        }
    }

    @Command(description = "Remove max unacked messages policy per subscription for a topic")
    private class RemoveMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxUnackedMessagesOnSubscription(persistentTopic);
        }
    }

    @Command(description = "Set max unacked messages policy on subscription for a topic")
    private class SetMaxUnackedMessagesPerSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-m", "--maxNum"},
                description = "max unacked messages num on subscription", required = true)
        private int maxNum;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxUnackedMessagesOnSubscription(persistentTopic, maxNum);
        }
    }

    @Command(description = "Get max number of producers for a topic")
    private class GetMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxProducers(persistentTopic, applied));
        }
    }

    @Command(description = "Set max number of producers for a topic")
    private class SetMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-producers", "-p"}, description = "Max producers for a topic", required = true)
        private int maxProducers;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxProducers(persistentTopic, maxProducers);
        }
    }

    @Command(description = "Get the delayed delivery policy for a topic")
    private class GetDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            print(getTopicPolicies(isGlobal).getDelayedDeliveryPolicy(topic, applied));
        }
    }

    @Command(description = "Set the delayed delivery policy on a topic")
    private class SetDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable", "-e" }, description = "Enable delayed delivery messages")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable delayed delivery messages")
        private boolean disable = false;

        @Option(names = { "--time", "-t" }, description = "The tick time for when retrying on "
                + "delayed delivery messages, affecting the accuracy of the delivery time compared to "
                + "the scheduled time. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryTimeInMills = 1000L;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Option(names = { "--maxDelay", "-md" },
                description = "The max allowed delay for delayed delivery. (eg: 1s, 10s, 1m, 5h, 3d)",
                converter = TimeUnitToMillisConverter.class)
        private Long delayedDeliveryMaxDelayInMillis = 0L;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }

            getTopicPolicies(isGlobal).setDelayedDeliveryPolicy(topic, DelayedDeliveryPolicies.builder()
                    .tickTime(delayedDeliveryTimeInMills)
                    .active(enable)
                    .maxDeliveryDelayInMillis(delayedDeliveryMaxDelayInMillis)
                    .build());
        }
    }

    @Command(description = "Remove the delayed delivery policy on a topic")
    private class RemoveDelayedDelivery extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getTopicPolicies(isGlobal).removeDelayedDeliveryPolicy(topic);
        }
    }

    @Command(description = "Remove max number of producers for a topic")
    private class RemoveMaxProducers extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxProducers(persistentTopic);
        }
    }

    @Command(description = "Get max message size for a topic")
    private class GetMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxMessageSize(persistentTopic));
        }
    }

    @Command(description = "Set max message size for a topic")
    private class SetMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-message-size", "-m"}, description = "Max message size for a topic", required = true)
        private int maxMessageSize;

        @Option(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxMessageSize(persistentTopic, maxMessageSize);
        }
    }

    @Command(description = "Remove max message size for a topic")
    private class RemoveMaxMessageSize extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxMessageSize(persistentTopic);
        }
    }

    @Command(description = "Enable or disable status for a topic")
    private class SetDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable", "-e" }, description = "Enable deduplication")
        private boolean enable = false;

        @Option(names = { "--disable", "-d" }, description = "Disable deduplication")
        private boolean disable = false;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            if (enable == disable) {
                throw new ParameterException("Need to specify either --enable or --disable");
            }
            getTopicPolicies(isGlobal).setDeduplicationStatus(persistentTopic, enable);
        }
    }

    @Command(description = "Get the deduplication status for a topic")
    private class GetDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getDeduplicationStatus(persistentTopic));
        }
    }

    @Command(description = "Remove the deduplication status for a topic")
    private class RemoveDeduplicationStatus extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeDeduplicationStatus(persistentTopic);
        }
    }

    @Command(description = "Get deduplication snapshot interval for a topic")
    private class GetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getDeduplicationSnapshotInterval(persistentTopic));
        }
    }

    @Command(description = "Set deduplication snapshot interval for a topic")
    private class SetDeduplicationSnapshotInterval extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-i", "--interval"}, description =
                "Deduplication snapshot interval for topic in second, allowed range from 0 to Integer.MAX_VALUE",
                required = true)
        private int interval;

        @Option(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            if (interval < 0) {
                throw new ParameterException(String.format("Invalid interval '%d'. ", interval));
            }

            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setDeduplicationSnapshotInterval(persistentTopic, interval);
        }
    }

    @Command(description = "Remove deduplication snapshot interval for a topic")
    private class RemoveDeduplicationSnapshotInterval extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeDeduplicationSnapshotInterval(persistentTopic);
        }
    }

    @Command(description = "Get the backlog quota policies for a topic")
    private class GetBacklogQuotaMap extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getBacklogQuotaMap(persistentTopic, applied));
        }
    }

    @Command(description = "Set a backlog quota policy for a topic")
    private class SetBacklogQuota extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

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

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

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
            getTopicPolicies(isGlobal).setBacklogQuota(persistentTopic,
                    builder.build(),
                    backlogQuotaType);
        }
    }

    @Command(description = "Remove a backlog quota policy from a topic")
    private class RemoveBacklogQuota extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-t", "--type"}, description = "Backlog quota type to remove")
        private String backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage.name();

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal)
                    .removeBacklogQuota(persistentTopic, BacklogQuota.BacklogQuotaType.valueOf(backlogQuotaType));
        }
    }

    @Command(description = "Get publish rate for a topic")
    private class GetPublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getPublishRate(persistentTopic));
        }
    }

    @Command(description = "Set publish rate for a topic")
    private class SetPublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--msg-publish-rate", "-m"}, description = "message-publish-rate (default -1 will be "
                + "overwrite if not passed)", required = false)
        private int msgPublishRate = -1;

        @Option(names = {"--byte-publish-rate", "-b"}, description = "byte-publish-rate "
                + "(default -1 will be overwrite if not passed)", required = false)
        private long bytePublishRate = -1;

        @Option(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setPublishRate(persistentTopic,
                    new PublishRate(msgPublishRate, bytePublishRate));
        }
    }

    @Command(description = "Remove publish rate for a topic")
    private class RemovePublishRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removePublishRate(persistentTopic);
        }
    }

    @Command(description = "Get consumer subscribe rate for a topic")
    private class GetSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returns global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getSubscribeRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set consumer subscribe rate for a topic")
    private class SetSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--subscribe-rate",
                "-sr" }, description = "subscribe-rate (default -1 will be overwrite if not passed)", required = false)
        private int subscribeRate = -1;

        @Option(names = { "--subscribe-rate-period",
                "-st" }, description = "subscribe-rate-period in second type "
                + "(default 30 second will be overwrite if not passed)", required = false)
        private int subscribeRatePeriodSec = 30;

        @Option(names = {"--global", "-g"}, description = "Whether to set this policy globally.")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setSubscribeRate(persistentTopic,
                    new SubscribeRate(subscribeRate, subscribeRatePeriodSec));
        }
    }

    @Command(description = "Remove consumer subscribe rate for a topic")
    private class RemoveSubscribeRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. ")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeSubscribeRate(persistentTopic);
        }
    }

    @Command(description = "Get the persistence policies for a topic")
    private class GetPersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getPersistence(persistentTopic));
        }
    }

    @Command(description = "Set the persistence policies for a topic")
    private class SetPersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-e",
                "--bookkeeper-ensemble" }, description = "Number of bookies to use for a topic")
        private int bookkeeperEnsemble = 2;

        @Option(names = { "-w",
                "--bookkeeper-write-quorum" }, description = "How many writes to make of each entry")
        private int bookkeeperWriteQuorum = 2;

        @Option(names = { "-a", "--bookkeeper-ack-quorum" },
                description = "Number of acks (guaranteed copies) to wait for each entry")
        private int bookkeeperAckQuorum = 2;

        @Option(names = { "-r", "--ml-mark-delete-max-rate" },
                description = "Throttling rate of mark-delete operation (0 means no throttle)")
        private double managedLedgerMaxMarkDeleteRate = 0;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously", arity = "0")
        private boolean isGlobal = false;

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
            getTopicPolicies(isGlobal).setPersistence(persistentTopic, new PersistencePolicies(bookkeeperEnsemble,
                    bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate,
                    managedLedgerStorageClassName));
        }
    }

    @Command(description = "Remove the persistence policy for a topic")
    private class RemovePersistence extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously"
                , arity = "0")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removePersistence(persistentTopic);
        }
    }

    @Command(description = "Get compaction threshold for a topic")
    private class GetCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;
        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getCompactionThreshold(persistentTopic, applied));
        }
    }

    @Command(description = "Set compaction threshold for a topic")
    private class SetCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--threshold", "-t" },
                description = "Maximum number of bytes in a topic backlog before compaction is triggered "
                        + "(eg: 10M, 16G, 3T). 0 disables automatic compaction",
                required = true,
                    converter = ByteUnitToLongConverter.class)
        private Long threshold = 0L;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setCompactionThreshold(persistentTopic, threshold);
        }
    }

    @Command(description = "Remove compaction threshold for a topic")
    private class RemoveCompactionThreshold extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;
        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;
        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeCompactionThreshold(persistentTopic);
        }
    }

    @Command(description = "Get message dispatch rate for a topic")
    private class GetDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getDispatchRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set message dispatch rate for a topic")
    private class SetDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

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

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove message dispatch rate for a topic")
    private class RemoveDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeDispatchRate(persistentTopic);
        }
    }

    @Command(description = "Get the inactive topic policies on a topic")
    private class GetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getInactiveTopicPolicies(persistentTopic, applied));
        }
    }

    @Command(description = "Set the inactive topic policies on a topic")
    private class SetInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--enable-delete-while-inactive", "-e" }, description = "Enable delete while inactive")
        private boolean enableDeleteWhileInactive = false;

        @Option(names = { "--disable-delete-while-inactive", "-d" }, description = "Disable delete while inactive")
        private boolean disableDeleteWhileInactive = false;

        @Option(names = {"--max-inactive-duration", "-t"},
                description = "Max duration of topic inactivity in seconds, topics that are inactive for longer than "
                        + "this value will be deleted (eg: 1s, 10s, 1m, 5h, 3d)", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long maxInactiveDurationInSeconds;

        @Option(names = { "--delete-mode", "-m" }, description = "Mode of delete inactive topic, Valid options are: "
                + "[delete_when_no_subscriptions, delete_when_subscriptions_caught_up]", required = true)
        private String inactiveTopicDeleteMode;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (enableDeleteWhileInactive == disableDeleteWhileInactive) {
                throw new ParameterException("Need to specify either enable-delete-while-inactive or "
                        + "disable-delete-while-inactive");
            }
            InactiveTopicDeleteMode deleteMode;
            try {
                deleteMode = InactiveTopicDeleteMode.valueOf(inactiveTopicDeleteMode);
            } catch (IllegalArgumentException e) {
                throw new ParameterException("delete mode can only be set to delete_when_no_subscriptions or "
                        + "delete_when_subscriptions_caught_up");
            }
            getTopicPolicies(isGlobal).setInactiveTopicPolicies(persistentTopic, new InactiveTopicPolicies(deleteMode,
                    maxInactiveDurationInSeconds.intValue(), enableDeleteWhileInactive));
        }
    }

    @Command(description = "Remove inactive topic policies from a topic")
    private class RemoveInactiveTopicPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;
        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;
        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeInactiveTopicPolicies(persistentTopic);
        }
    }

    @Command(description = "Get replicator message-dispatch-rate for a topic")
    private class GetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getReplicatorDispatchRate(persistentTopic, applied));
        }
    }

    @Command(description = "Set replicator message-dispatch-rate for a topic")
    private class SetReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--msg-dispatch-rate",
                "-md" }, description = "message-dispatch-rate (default -1 will be overwrite if not passed)")
        private int msgDispatchRate = -1;

        @Option(names = { "--byte-dispatch-rate",
                "-bd" }, description = "byte-dispatch-rate (default -1 will be overwrite if not passed)")
        private long byteDispatchRate = -1;

        @Option(names = {"--dispatch-rate-period",
                "-dt"}, description = "dispatch-rate-period in second type (default 1 second will be overwrite if not"
                + " passed)")
        private int dispatchRatePeriodSec = 1;

        @Option(names = {"--relative-to-publish-rate",
                "-rp"}, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setReplicatorDispatchRate(persistentTopic,
                    DispatchRate.builder()
                            .dispatchThrottlingRateInMsg(msgDispatchRate)
                            .dispatchThrottlingRateInByte(byteDispatchRate)
                            .ratePeriodInSecond(dispatchRatePeriodSec)
                            .relativeToPublishRate(relativeToPublishRate)
                            .build());
        }
    }

    @Command(description = "Remove replicator message-dispatch-rate for a topic")
    private class RemoveReplicatorDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeReplicatorDispatchRate(persistentTopic);
        }

    }

    @Command(description = "Get subscription message-dispatch-rate for a topic")
    private class GetSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Option(names = {"--subscription", "-s"},
                description = "Get message-dispatch-rate of a specific subscription")
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (StringUtils.isBlank(subName)) {
                print(getTopicPolicies(isGlobal).getSubscriptionDispatchRate(persistentTopic, applied));
            } else {
                print(getTopicPolicies(isGlobal).getSubscriptionDispatchRate(persistentTopic, subName, applied));
            }
        }
    }

    @Command(description = "Set subscription message-dispatch-rate for a topic")
    private class SetSubscriptionDispatchRate extends CliCommand {
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
                + "(default 1 second will be overwrite if not passed)")
        private int dispatchRatePeriodSec = 1;

        @Option(names = { "--relative-to-publish-rate",
                "-rp" }, description = "dispatch rate relative to publish-rate (if publish-relative flag is enabled "
                + "then broker will apply throttling value to (publish-rate + dispatch rate))")
        private boolean relativeToPublishRate = false;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Option(names = {"--subscription", "-s"},
                description = "Set message-dispatch-rate for a specific subscription")
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            DispatchRate rate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(msgDispatchRate)
                    .dispatchThrottlingRateInByte(byteDispatchRate)
                    .ratePeriodInSecond(dispatchRatePeriodSec)
                    .relativeToPublishRate(relativeToPublishRate)
                    .build();
            if (StringUtils.isBlank(subName)) {
                getTopicPolicies(isGlobal).setSubscriptionDispatchRate(persistentTopic, rate);
            } else {
                getTopicPolicies(isGlobal).setSubscriptionDispatchRate(persistentTopic, subName, rate);
            }
        }
    }

    @Command(description = "Remove subscription message-dispatch-rate for a topic")
    private class RemoveSubscriptionDispatchRate extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Option(names = {"--subscription", "-s"},
                description = "Remove message-dispatch-rate for a specific subscription")
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (StringUtils.isBlank(subName)) {
                getTopicPolicies(isGlobal).removeSubscriptionDispatchRate(persistentTopic);
            } else {
                getTopicPolicies(isGlobal).removeSubscriptionDispatchRate(persistentTopic, subName);
            }
        }

    }

    @Command(description = "Get max subscriptions for a topic")
    private class GetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getMaxSubscriptionsPerTopic(persistentTopic));
        }
    }

    @Command(description = "Set max subscriptions for a topic")
    private class SetMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--max-subscriptions-per-topic",
                "-s"}, description = "max subscriptions for a topic (default -1 will be overwrite if not passed)",
                required = true)
        private int maxSubscriptionPerTopic;

        @Option(names = {"--global", "-g"}, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setMaxSubscriptionsPerTopic(persistentTopic, maxSubscriptionPerTopic);
        }
    }

    @Command(description = "Remove max subscriptions for a topic")
    private class RemoveMaxSubscriptionsPerTopic extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeMaxSubscriptionsPerTopic(persistentTopic);
        }
    }


    @Command(description = "Get the offload policies for a topic")
    private class GetOffloadPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getOffloadPolicies(persistentTopic, applied));
        }
    }

    @Command(description = "Remove the offload policies for a topic")
    private class RemoveOffloadPolicies extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to remove this policy globally. "
                + "If set to true, the removing operation will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeOffloadPolicies(persistentTopic);
        }
    }

    @Command(description = "Set the offload policies for a topic")
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

        @Option(names = {"-m", "--maxBlockSizeInBytes"},
                description = "ManagedLedger offload max block Size in bytes,"
                        + "s3 and google-cloud-storage requires this parameter")
        private int maxBlockSizeInBytes = OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

        @Option(names = {"-rb", "--readBufferSizeInBytes"},
                description = "ManagedLedger offload read buffer size in bytes,"
                        + "s3 and google-cloud-storage requires this parameter")
        private int readBufferSizeInBytes = OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES;

        @Option(names = {"-t", "--offloadThresholdInBytes"}
                , description = "ManagedLedger offload threshold in bytes")
        private Long offloadThresholdInBytes = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;

        @Option(names = {"-ts", "--offloadThresholdInSeconds"}
                , description = "ManagedLedger offload threshold in seconds")
        private Long offloadThresholdInSeconds = OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS;

        @Option(names = {"-dl", "--offloadDeletionLagInMillis"}
                , description = "ManagedLedger offload deletion lag in bytes")
        private Long offloadDeletionLagInMillis = OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

        @Option(
                names = {"--offloadedReadPriority", "-orp"},
                description = "Read priority for offloaded messages. By default, once messages are offloaded to"
                        + " long-term storage, brokers read messages from long-term storage, but messages can still"
                        + " exist in BookKeeper for a period depends on your configuration. For messages that exist"
                        + " in both long-term storage and BookKeeper, you can set where to read messages from with"
                        + " the option `tiered-storage-first` or `bookkeeper-first`.",
                required = false
        )
        private String offloadReadPriorityStr;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

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
                throw new ParameterException("The driver " + driver + " is not supported, "
                        + "(Possible values: " + String.join(",", driverNames) + ").");
            }

            if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
                throw new ParameterException(
                        "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                                + " if s3 offload enabled");
            }
            positiveCheck("maxBlockSizeInBytes", maxBlockSizeInBytes);
            maxValueCheck("maxBlockSizeInBytes", maxBlockSizeInBytes, Integer.MAX_VALUE);
            positiveCheck("readBufferSizeInBytes", readBufferSizeInBytes);
            maxValueCheck("readBufferSizeInBytes", readBufferSizeInBytes, Integer.MAX_VALUE);

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
                    readBufferSizeInBytes, offloadThresholdInBytes, offloadThresholdInSeconds,
                    offloadDeletionLagInMillis, offloadedReadPriority);

            getTopicPolicies(isGlobal).setOffloadPolicies(persistentTopic, offloadPolicies);
        }
    }


    @Command(description = "Remove schema compatibility strategy on a topic")
    private class RemoveSchemaCompatibilityStrategy extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getAdmin().topicPolicies().removeSchemaCompatibilityStrategy(persistentTopic);
        }
    }

    @Command(description = "Set schema compatibility strategy on a topic")
    private class SetSchemaCompatibilityStrategy extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--strategy", "-s"}, description = "Schema compatibility strategy: [UNDEFINED, "
                + "ALWAYS_INCOMPATIBLE, ALWAYS_COMPATIBLE, BACKWARD, FORWARD, FULL, BACKWARD_TRANSITIVE, "
                + "FORWARD_TRANSITIVE, FULL_TRANSITIVE]", required = true)
        private SchemaCompatibilityStrategy strategy;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getAdmin().topicPolicies().setSchemaCompatibilityStrategy(persistentTopic, strategy);
        }
    }

    @Command(description = "Get schema compatibility strategy on a topic")
    private class GetSchemaCompatibilityStrategy extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-ap", "--applied"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            SchemaCompatibilityStrategy strategy =
                    getAdmin().topicPolicies().getSchemaCompatibilityStrategy(persistentTopic, applied);
            print(strategy == null ? "null" : strategy.name());
        }
    }

    @Command(description = "Enable autoSubscriptionCreation for a topic")
    private class SetAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--enable", "-e"}, description = "Enable allowAutoSubscriptionCreation on topic")
        private boolean enable = false;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setAutoSubscriptionCreation(persistentTopic,
                    AutoSubscriptionCreationOverride.builder()
                            .allowAutoSubscriptionCreation(enable)
                            .build());
        }
    }

    @Command(description = "Get the autoSubscriptionCreation for a topic")
    private class GetAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--applied", "-a"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getAutoSubscriptionCreation(persistentTopic, applied));
        }
    }

    @Command(description = "Remove override of autoSubscriptionCreation for a topic")
    private class RemoveAutoSubscriptionCreation extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeAutoSubscriptionCreation(persistentTopic);
        }
    }

    @Command(description = "Enable dispatcherPauseOnAckStatePersistent for a topic")
    private class SetDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).setDispatcherPauseOnAckStatePersistent(persistentTopic);
        }
    }

    @Command(description = "Get the dispatcherPauseOnAckStatePersistent for a topic")
    private class GetDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--applied", "-a"}, description = "Get the applied policy of the topic")
        private boolean applied = false;

        @Option(names = {"--global", "-g"}, description = "Whether to get this policy globally. "
                + "If set to true, broker returned global topic policies")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getDispatcherPauseOnAckStatePersistent(persistentTopic, applied));
        }
    }

    @Command(description = "Remove dispatcherPauseOnAckStatePersistent for a topic")
    private class RemoveDispatcherPauseOnAckStatePersistent extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"--global", "-g"}, description = "Whether to remove this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeDispatcherPauseOnAckStatePersistent(persistentTopic);
        }
    }

    @Command(description = "Set the replication clusters for a topic, global policy will be copied to the remote"
            + " cluster if you enabled namespace level replication.")
    private class SetReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--clusters",
                "-c" }, description = "Replication Cluster Ids list (comma separated values)", required = true)
        private String clusterIds;

        @Option(names = { "--global", "-g" }, description = "Whether to set this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            List<String> clusters = Lists.newArrayList(clusterIds.split(","));
            getTopicPolicies(isGlobal).setReplicationClusters(persistentTopic, clusters);
        }
    }

    @Command(description = "Get the replication clusters for a topic")
    private class GetReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-ap", "--applied" }, description = "Get the applied policy of the topic. If set to true,"
                + " the param \"--global\" will be ignored. ")
        private boolean applied = false;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set \"--applied\" to true, the current param will be ignored. ")
        private boolean isGlobal = false;


        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getTopicPolicies(isGlobal).getReplicationClusters(persistentTopic, applied));
        }
    }

    @Command(description = "Remove the replication clusters for a topic, it will not remove the policy from the remote"
            + "cluster")
    private class RemoveReplicationClusters extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "--global", "-g" }, description = "Whether to get this policy globally. "
                + "If set to true, the policy will be replicate to other clusters asynchronously")
        private boolean isGlobal = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(isGlobal).removeReplicationClusters(persistentTopic);
        }
    }

    @Command(description = "Remove the all policies for a topic, it will not remove policies from the remote"
            + "cluster")
    private class DeletePolicies extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getTopicPolicies(false).deleteTopicPolicies(persistentTopic);
        }
    }

    private TopicPolicies getTopicPolicies(boolean isGlobal) {
        return getAdmin().topicPolicies(isGlobal);
    }

}
