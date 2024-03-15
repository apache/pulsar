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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToSecondsConverter;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations on persistent topics. The persistent-topics "
        + "has been deprecated in favor of topics", hidden = true)
public class CmdPersistentTopics extends CmdBase {
    private Topics persistentTopics;

    public CmdPersistentTopics(Supplier<PulsarAdmin> admin) {
        super("persistent", admin);

        addCommand("list", new ListCmd());
        addCommand("list-partitioned-topics", new PartitionedTopicListCmd());
        addCommand("permissions", new Permissions());
        addCommand("grant-permission", new GrantPermissions());
        addCommand("revoke-permission", new RevokePermissions());
        addCommand("lookup", new Lookup());
        addCommand("bundle-range", new GetBundleRange());
        addCommand("delete", new DeleteCmd());
        addCommand("unload", new UnloadCmd());
        addCommand("truncate", new TruncateCmd());
        addCommand("subscriptions", new ListSubscriptions());
        addCommand("unsubscribe", new DeleteSubscription());
        addCommand("create-subscription", new CreateSubscription());
        addCommand("stats", new GetStats());
        addCommand("stats-internal", new GetInternalStats());
        addCommand("info-internal", new GetInternalInfo());
        addCommand("partitioned-stats", new GetPartitionedStats());
        addCommand("partitioned-stats-internal", new GetPartitionedStatsInternal());
        addCommand("skip", new Skip());
        addCommand("skip-all", new SkipAll());
        addCommand("expire-messages", new ExpireMessages());
        addCommand("expire-messages-all-subscriptions", new ExpireMessagesForAllSubscriptions());
        addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        addCommand("update-partitioned-topic", new UpdatePartitionedCmd());
        addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());
        addCommand("delete-partitioned-topic", new DeletePartitionedCmd());
        addCommand("peek-messages", new PeekMessages());
        addCommand("get-message-by-id", new GetMessageById());
        addCommand("last-message-id", new GetLastMessageId());
        addCommand("reset-cursor", new ResetCursor());
        addCommand("terminate", new Terminate());
        addCommand("compact", new Compact());
        addCommand("compaction-status", new CompactionStatusCmd());
    }

    private Topics getPersistentTopics() {
        if (persistentTopics == null) {
            persistentTopics = getAdmin().topics();
        }
        return persistentTopics;
    }

    @Command(description = "Get the list of topics under a namespace.")
    private class ListCmd extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getPersistentTopics().getList(namespace));
        }
    }

    @Command(description = "Get the list of partitioned topics under a namespace.")
    private class PartitionedTopicListCmd extends CliCommand {
        @Parameters(description = "tenant/namespace", arity = "1")
        private String namespaceName;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(namespaceName);
            print(getPersistentTopics().getPartitionedTopicList(namespace));
        }
    }

    @Command(description = "Grant a new permission to a client role on a single topic.")
    private class GrantPermissions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Option(names = "--actions", description = "Actions to be granted (produce,consume,sources,sinks,"
                + "functions,packages)", required = true)
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getPersistentTopics().grantPermission(topic, role, getAuthActions(actions));
        }
    }

    @Command(description = "Revoke permissions on a topic. "
            + "Revoke permissions to a client role on a single topic. If the permission "
            + "was not set at the topic level, but rather at the namespace level, this "
            + "operation will return an error (HTTP status code 412).")
    private class RevokePermissions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--role", description = "Client role to which revoke permissions", required = true)
        private String role;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getPersistentTopics().revokePermissions(topic, role);
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
            print(getPersistentTopics().getPermissions(topic));
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

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().createPartitionedTopic(persistentTopic, numPartitions);
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

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().updatePartitionedTopic(persistentTopic, numPartitions);
        }
    }

    @Command(description = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getPartitionedTopicMetadata(persistentTopic));
        }
    }

    @Command(description = "Delete a partitioned topic. "
            + "It will also delete all the partitions of the topic if it exists.")
    private class DeletePartitionedCmd extends CliCommand {

        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--force",
                description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().deletePartitionedTopic(persistentTopic, force);
        }
    }

    @Command(description = "Delete a topic. "
            + "The topic cannot be deleted if there's any active subscription or producers connected to it.")
    private class DeleteCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--force",
                description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().delete(persistentTopic, force);
        }
    }

    @Command(description = "Unload a topic.")
    private class UnloadCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().unload(persistentTopic);
        }
    }

    @Command(description = "Truncate a topic. \n\t\tThe truncate operation will move all cursors to the end "
            + "of the topic and delete all inactive ledgers. ")
    private class TruncateCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(topicName);
            getPersistentTopics().truncate(topic);
        }
    }

    @Command(description = "Get the list of subscriptions on the topic")
    private class ListSubscriptions extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getSubscriptions(persistentTopic));
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

        @Option(names = { "-s", "--subscription" }, description = "Subscription to be deleted", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().deleteSubscription(persistentTopic, subName, force);
        }
    }

    @Command(description = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-gpb", "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getStats(persistentTopic, getPreciseBacklog));
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
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getInternalStats(persistentTopic, metadata));
        }
    }

    @Command(description = "Get the internal metadata info for the topic")
    private class GetInternalInfo extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            String result = getPersistentTopics().getInternalInfo(persistentTopic);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(result));
        }
    }

    @Command(description = "Get the stats for the partitioned topic and "
            + "its connected producers and consumers. All the rates are computed over a 1 minute window and "
            + "are relative the last completed 1 minute period.")
    private class GetPartitionedStats extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = "--per-partition", description = "Get per partition stats")
        private boolean perPartition = false;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getPartitionedStats(persistentTopic, perPartition));
        }
    }

    @Command(description = "Get the stats-internal for the partitioned topic and "
            + "its connected producers and consumers. All the rates are computed over a 1 minute window and "
            + "are relative the last completed 1 minute period.")
    private class GetPartitionedStatsInternal extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(topicName);
            print(getPersistentTopics().getPartitionedInternalStats(persistentTopic));
        }
    }

    @Command(description = "Skip all the messages for the subscription")
    private class SkipAll extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s", "--subscription" }, description = "Subscription to be cleared", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().skipAllMessages(persistentTopic, subName);
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
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().skipMessages(persistentTopic, subName, numMessages);
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
                + "(or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w)", required = true,
                converter = TimeUnitToSecondsConverter.class)
        private Long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().expireMessages(persistentTopic, subName, expireTimeInSeconds);
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
            String persistentTopic = validatePersistentTopic(topicName);
            getPersistentTopics().expireMessagesForAllSubscriptions(persistentTopic, expireTimeInSeconds);
        }
    }

    @Command(description = "Create a new subscription on a topic")
    private class CreateSubscription extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription name", required = true)
        private String subscriptionName;

        @Option(names = { "--messageId",
                "-m" }, description = "messageId where to create the subscription. "
                + "It can be either 'latest', 'earliest' or (ledgerId:entryId)", required = false)
        private String messageIdStr = "latest";

        @Option(names = {"--property", "-p"}, description = "key value pair properties(-p a=b -p c=d)",
                required = false)
        private Map<String, String> properties;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            MessageId messageId;
            if (messageIdStr.equals("latest")) {
                messageId = MessageId.latest;
            } else if (messageIdStr.equals("earliest")) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messageIdStr);
            }
            getPersistentTopics().createSubscription(persistentTopic, subscriptionName, messageId, false, properties);
        }
    }

    @Command(description = "Reset position for subscription to position closest to timestamp or messageId")
    private class ResetCursor extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subName;

        @Option(names = { "--time",
                "-t" }, description = "time in minutes to reset back to "
                + "(or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w)", required = false,
                converter = TimeUnitToMillisConverter.class)
        private Long resetTimeInMillis = null;

        @Option(names = { "--messageId",
                "-m" }, description = "messageId to reset back to (ledgerId:entryId)", required = false)
        private String resetMessageIdStr;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            if (isNotBlank(resetMessageIdStr)) {
                MessageId messageId = validateMessageIdString(resetMessageIdStr);
                getPersistentTopics().resetCursor(persistentTopic, subName, messageId);
            } else if (Objects.nonNull(resetTimeInMillis)) {
                // now - go back time
                long timestamp = System.currentTimeMillis() - resetTimeInMillis;
                getPersistentTopics().resetCursor(persistentTopic, subName, timestamp);
            } else {
                throw new PulsarAdminException(
                        "Either Timestamp (--time) or Position (--position) has to be provided to reset cursor");
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
                MessageId lastMessageId = getPersistentTopics().terminateTopicAsync(persistentTopic).get();
                System.out.println("Topic successfully terminated at " + lastMessageId);
            } catch (InterruptedException | ExecutionException e) {
                throw new PulsarAdminException(e);
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

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            List<Message<byte[]>> messages = getPersistentTopics().peekMessages(persistentTopic, subName, numMessages);
            int position = 0;
            for (Message<byte[]> msg : messages) {
                if (++position != 1) {
                    System.out.println("-------------------------------------------------------------------------\n");
                }
                if (msg.getMessageId() instanceof BatchMessageIdImpl) {
                    BatchMessageIdImpl msgId = (BatchMessageIdImpl) msg.getMessageId();
                    System.out.println("Batch Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId() + ":"
                            + msgId.getBatchIndex());
                } else {
                    MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
                    System.out.println("Message ID: " + msgId.getLedgerId() + ":" + msgId.getEntryId());
                }
                if (msg.getProperties().size() > 0) {
                    System.out.println("Properties:");
                    print(msg.getProperties());
                }
                ByteBuf data = Unpooled.wrappedBuffer(msg.getData());
                System.out.println(ByteBufUtil.prettyHexDump(data));
            }
        }
    }

    @Command(description = "Get message by its ledgerId and entryId")
    private class GetMessageById extends CliCommand {
        @Parameters(description = "persistent://property/cluster/namespace/topic", arity = "1")
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

            Message<byte[]> message = getPersistentTopics().getMessageById(persistentTopic, ledgerId, entryId);

            ByteBuf date = Unpooled.wrappedBuffer(message.getData());
            System.out.println(ByteBufUtil.prettyHexDump(date));
        }
    }


    @Command(description = "Get last message Id of the topic")
    private class GetLastMessageId extends CliCommand {
        @Parameters(description = "persistent://property/cluster/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);
            MessageId messageId = getPersistentTopics().getLastMessageId(persistentTopic);
            print(messageId);
        }
    }

    @Command(description = "Compact a topic")
    private class Compact extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            getPersistentTopics().triggerCompaction(persistentTopic);
            System.out.println("Topic compaction requested for " + persistentTopic);
        }
    }

    @Command(description = "Status of compaction on a topic")
    private class CompactionStatusCmd extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-w", "--wait-complete"},
                description = "Wait for compaction to complete", required = false)
        private boolean wait = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(topicName);

            try {
                LongRunningProcessStatus status = getPersistentTopics().compactionStatus(persistentTopic);
                while (wait && status.status == LongRunningProcessStatus.Status.RUNNING) {
                    Thread.sleep(1000);
                    status = getPersistentTopics().compactionStatus(persistentTopic);
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
}
