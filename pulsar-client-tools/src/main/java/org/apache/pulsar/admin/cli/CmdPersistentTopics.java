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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations on persistent topics. The persistent-topics "
        + "has been deprecated in favor of topics", hidden = true)
public class CmdPersistentTopics extends CmdBase {
    private Topics persistentTopics;

    public CmdPersistentTopics(Supplier<PulsarAdmin> admin) {
        super("persistent", admin);

        jcommander.addCommand("list", new ListCmd());
        jcommander.addCommand("list-partitioned-topics", new PartitionedTopicListCmd());
        jcommander.addCommand("permissions", new Permissions());
        jcommander.addCommand("grant-permission", new GrantPermissions());
        jcommander.addCommand("revoke-permission", new RevokePermissions());
        jcommander.addCommand("lookup", new Lookup());
        jcommander.addCommand("bundle-range", new GetBundleRange());
        jcommander.addCommand("delete", new DeleteCmd());
        jcommander.addCommand("unload", new UnloadCmd());
        jcommander.addCommand("truncate", new TruncateCmd());
        jcommander.addCommand("subscriptions", new ListSubscriptions());
        jcommander.addCommand("unsubscribe", new DeleteSubscription());
        jcommander.addCommand("create-subscription", new CreateSubscription());
        jcommander.addCommand("stats", new GetStats());
        jcommander.addCommand("stats-internal", new GetInternalStats());
        jcommander.addCommand("info-internal", new GetInternalInfo());
        jcommander.addCommand("partitioned-stats", new GetPartitionedStats());
        jcommander.addCommand("partitioned-stats-internal", new GetPartitionedStatsInternal());
        jcommander.addCommand("skip", new Skip());
        jcommander.addCommand("skip-all", new SkipAll());
        jcommander.addCommand("expire-messages", new ExpireMessages());
        jcommander.addCommand("expire-messages-all-subscriptions", new ExpireMessagesForAllSubscriptions());
        jcommander.addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        jcommander.addCommand("update-partitioned-topic", new UpdatePartitionedCmd());
        jcommander.addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());
        jcommander.addCommand("delete-partitioned-topic", new DeletePartitionedCmd());
        jcommander.addCommand("peek-messages", new PeekMessages());
        jcommander.addCommand("get-message-by-id", new GetMessageById());
        jcommander.addCommand("last-message-id", new GetLastMessageId());
        jcommander.addCommand("reset-cursor", new ResetCursor());
        jcommander.addCommand("terminate", new Terminate());
        jcommander.addCommand("compact", new Compact());
        jcommander.addCommand("compaction-status", new CompactionStatusCmd());
    }

    private Topics getPersistentTopics() {
        if (persistentTopics == null) {
            persistentTopics = getAdmin().topics();
        }
        return persistentTopics;
    }

    @Parameters(commandDescription = "Get the list of topics under a namespace.")
    private class ListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getPersistentTopics().getList(namespace));
        }
    }

    @Parameters(commandDescription = "Get the list of partitioned topics under a namespace.")
    private class PartitionedTopicListCmd extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getPersistentTopics().getPartitionedTopicList(namespace));
        }
    }

    @Parameters(commandDescription = "Grant a new permission to a client role on a single topic.")
    private class GrantPermissions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--role", description = "Client role to which grant permissions", required = true)
        private String role;

        @Parameter(names = "--actions", description = "Actions to be granted (produce,consume,sources,sinks,"
                + "functions,packages)", required = true)
        private List<String> actions;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getPersistentTopics().grantPermission(topic, role, getAuthActions(actions));
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
            getPersistentTopics().revokePermissions(topic, role);
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
            print(getPersistentTopics().getPermissions(topic));
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
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().createPartitionedTopic(persistentTopic, numPartitions);
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

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().updatePartitionedTopic(persistentTopic, numPartitions);
        }
    }

    @Parameters(commandDescription = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getPartitionedTopicMetadata(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Delete a partitioned topic. "
            + "It will also delete all the partitions of the topic if it exists.")
    private class DeletePartitionedCmd extends CliCommand {

        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--force",
                description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().deletePartitionedTopic(persistentTopic, force);
        }
    }

    @Parameters(commandDescription = "Delete a topic. "
            + "The topic cannot be deleted if there's any active subscription or producers connected to it.")
    private class DeleteCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--force",
                description = "Close all producer/consumer/replicator and delete topic forcefully")
        private boolean force = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().delete(persistentTopic, force);
        }
    }

    @Parameters(commandDescription = "Unload a topic.")
    private class UnloadCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().unload(persistentTopic);
        }
    }

    @Parameters(commandDescription = "Truncate a topic. \n\t\tThe truncate operation will move all cursors to the end "
            + "of the topic and delete all inactive ledgers. ")
    private class TruncateCmd extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            getPersistentTopics().truncate(topic);
        }
    }

    @Parameters(commandDescription = "Get the list of subscriptions on the topic")
    private class ListSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getSubscriptions(persistentTopic));
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
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().deleteSubscription(persistentTopic, subName, force);
        }
    }

    @Parameters(commandDescription = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-gpb", "--get-precise-backlog" }, description = "Set true to get precise backlog")
        private boolean getPreciseBacklog = false;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getStats(persistentTopic, getPreciseBacklog));
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
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getInternalStats(persistentTopic, metadata));
        }
    }

    @Parameters(commandDescription = "Get the internal metadata info for the topic")
    private class GetInternalInfo extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            String result = getPersistentTopics().getInternalInfo(persistentTopic);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(result));
        }
    }

    @Parameters(commandDescription = "Get the stats for the partitioned topic and "
            + "its connected producers and consumers. All the rates are computed over a 1 minute window and "
            + "are relative the last completed 1 minute period.")
    private class GetPartitionedStats extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--per-partition", description = "Get per partition stats")
        private boolean perPartition = false;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getPartitionedStats(persistentTopic, perPartition));
        }
    }

    @Parameters(commandDescription = "Get the stats-internal for the partitioned topic and "
            + "its connected producers and consumers. All the rates are computed over a 1 minute window and "
            + "are relative the last completed 1 minute period.")
    private class GetPartitionedStatsInternal extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String persistentTopic = validatePersistentTopic(params);
            print(getPersistentTopics().getPartitionedInternalStats(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Skip all the messages for the subscription")
    private class SkipAll extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s", "--subscription" }, description = "Subscription to be cleared", required = true)
        private String subName;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().skipAllMessages(persistentTopic, subName);
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
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().skipMessages(persistentTopic, subName, numMessages);
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) "
            + "for the subscription")
    private class ExpireMessages extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to be skip messages on", required = true)
        private String subName;

        @Parameter(names = { "-t", "--expireTime" },
                description = "Expire messages older than time in seconds", required = true)
        private long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().expireMessages(persistentTopic, subName, expireTimeInSeconds);
        }
    }

    @Parameters(commandDescription = "Expire messages that older than given expiry time (in seconds) "
            + "for all subscriptions")
    private class ExpireMessagesForAllSubscriptions extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-t", "--expireTime" },
                description = "Expire messages older than time in seconds", required = true)
        private long expireTimeInSeconds;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            getPersistentTopics().expireMessagesForAllSubscriptions(persistentTopic, expireTimeInSeconds);
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
                "-m" }, description = "messageId where to create the subscription. "
                + "It can be either 'latest', 'earliest' or (ledgerId:entryId)", required = false)
        private String messageIdStr = "latest";

        @Parameter(names = {"--property", "-p"}, description = "key value pair properties(-p a=b -p c=d)",
                required = false)
        private java.util.List<String> properties;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            MessageId messageId;
            if (messageIdStr.equals("latest")) {
                messageId = MessageId.latest;
            } else if (messageIdStr.equals("earliest")) {
                messageId = MessageId.earliest;
            } else {
                messageId = validateMessageIdString(messageIdStr);
            }
            Map<String, String> map = parseListKeyValueMap(properties);
            getPersistentTopics().createSubscription(persistentTopic, subscriptionName, messageId, false, map);
        }
    }

    @Parameters(commandDescription = "Reset position for subscription to position closest to timestamp or messageId")
    private class ResetCursor extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-s",
                "--subscription" }, description = "Subscription to reset position on", required = true)
        private String subName;

        @Parameter(names = { "--time",
                "-t" }, description = "time in minutes to reset back to "
                + "(or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w)", required = false)
        private String resetTimeStr;

        @Parameter(names = { "--messageId",
                "-m" }, description = "messageId to reset back to (ledgerId:entryId)", required = false)
        private String resetMessageIdStr;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            if (isNotBlank(resetMessageIdStr)) {
                MessageId messageId = validateMessageIdString(resetMessageIdStr);
                getPersistentTopics().resetCursor(persistentTopic, subName, messageId);
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
                getPersistentTopics().resetCursor(persistentTopic, subName, timestamp);
            } else {
                throw new PulsarAdminException(
                        "Either Timestamp (--time) or Position (--position) has to be provided to reset cursor");
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
                MessageId lastMessageId = getPersistentTopics().terminateTopicAsync(persistentTopic).get();
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

    @Parameters(commandDescription = "Get message by its ledgerId and entryId")
    private class GetMessageById extends CliCommand {
        @Parameter(description = "persistent://property/cluster/namespace/topic", required = true)
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

            Message<byte[]> message = getPersistentTopics().getMessageById(persistentTopic, ledgerId, entryId);

            ByteBuf date = Unpooled.wrappedBuffer(message.getData());
            System.out.println(ByteBufUtil.prettyHexDump(date));
        }
    }


    @Parameters(commandDescription = "Get last message Id of the topic")
    private class GetLastMessageId extends CliCommand {
        @Parameter(description = "persistent://property/cluster/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);
            MessageId messageId = getPersistentTopics().getLastMessageId(persistentTopic);
            print(messageId);
        }
    }

    @Parameters(commandDescription = "Compact a topic")
    private class Compact extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validatePersistentTopic(params);

            getPersistentTopics().triggerCompaction(persistentTopic);
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
