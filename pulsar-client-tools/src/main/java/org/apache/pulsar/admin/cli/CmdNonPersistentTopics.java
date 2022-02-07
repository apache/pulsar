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
import com.beust.jcommander.Parameters;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@SuppressWarnings("deprecation")
@Parameters(commandDescription = "Operations on non-persistent topics", hidden = true)
public class CmdNonPersistentTopics extends CmdBase {
    private NonPersistentTopics nonPersistentTopics;

    public CmdNonPersistentTopics(Supplier<PulsarAdmin> admin) {
        super("non-persistent", admin);

        jcommander.addCommand("create-partitioned-topic", new CreatePartitionedCmd());
        jcommander.addCommand("lookup", new Lookup());
        jcommander.addCommand("stats", new GetStats());
        jcommander.addCommand("stats-internal", new GetInternalStats());
        jcommander.addCommand("get-partitioned-topic-metadata", new GetPartitionedTopicMetadataCmd());
        jcommander.addCommand("list", new GetList());
        jcommander.addCommand("list-in-bundle", new GetListInBundle());
    }

    private NonPersistentTopics getNonPersistentTopics() {
        if (nonPersistentTopics == null) {
            nonPersistentTopics = getAdmin().nonPersistentTopics();
        }
        return nonPersistentTopics;
    }

    @Parameters(commandDescription = "Lookup a topic from the current serving broker")
    private class Lookup extends CliCommand {
        @Parameter(description = "non-persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String topic = validateTopicName(params);
            print(getAdmin().lookups().lookupTopic(topic));
        }
    }

    @Parameters(commandDescription = "Get the stats for the topic and its connected producers and consumers. "
            + "All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.")
    private class GetStats extends CliCommand {
        @Parameter(description = "non-persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validateNonPersistentTopic(params);
            print(getNonPersistentTopics().getStats(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Get the internal stats for the topic")
    private class GetInternalStats extends CliCommand {
        @Parameter(description = "non-persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String persistentTopic = validateNonPersistentTopic(params);
            print(getNonPersistentTopics().getInternalStats(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Create a partitioned topic. "
            + "The partitioned topic has to be created before creating a producer on it.")
    private class CreatePartitionedCmd extends CliCommand {

        @Parameter(description = "non-persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-p",
                "--partitions" }, description = "Number of partitions for the topic", required = true)
        private int numPartitions;

        @Override
        void run() throws Exception {
            String persistentTopic = validateNonPersistentTopic(params);
            getNonPersistentTopics().createPartitionedTopic(persistentTopic, numPartitions);
        }
    }

    @Parameters(commandDescription = "Get the partitioned topic metadata. "
            + "If the topic is not created or is a non-partitioned topic, it returns empty topic with 0 partitions")
    private class GetPartitionedTopicMetadataCmd extends CliCommand {

        @Parameter(description = "non-persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String persistentTopic = validateNonPersistentTopic(params);
            print(getNonPersistentTopics().getPartitionedTopicMetadata(persistentTopic));
        }
    }

    @Parameters(commandDescription = "Get list of non-persistent topics present under a namespace")
    private class GetList extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getNonPersistentTopics().getList(namespace));
        }
    }

    @Parameters(commandDescription = "Get list of non-persistent topics present under a namespace bundle")
    private class GetListInBundle extends CliCommand {
        @Parameter(description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-b",
                "--bundle" }, description = "bundle range", required = true)
        private String bundleRange;

        @Override
        void run() throws PulsarAdminException {
            String namespace = validateNamespace(params);
            print(getNonPersistentTopics().getListInBundle(namespace, bundleRange));
        }
    }
}
