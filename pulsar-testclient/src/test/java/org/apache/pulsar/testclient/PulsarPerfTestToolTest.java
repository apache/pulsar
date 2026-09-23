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
package org.apache.pulsar.testclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * Subcommand registration of {@code pulsar-perf}, and how {@code produce}, {@code consume}, {@code read}
 * and {@code transaction} pick the v4 or the V5 client and group their options by client.
 */
public class PulsarPerfTestToolTest {

    /**
     * Every subcommand must carry its own {@code @Command(name = ..., description = ...)}. The name
     * assertion catches a copy-pasted one (picocli keeps a declared name and would then print the
     * wrong command in the usage message); the description assertion catches a missing annotation,
     * whose only symptom is an empty description in the usage message and in {@code gen-doc}.
     */
    @Test
    public void testAllSubcommandsAreRegisteredUnderTheirOwnName() throws Exception {
        CommandLine commander = initCommander();

        Map<String, CommandLine> subcommands = commander.getSubcommands();
        assertThat(subcommands.keySet()).containsExactlyInAnyOrder(
                "produce", "consume", "read", "transaction",
                "monitor-brokers", "websocket-producer", "managed-ledger", "gen-doc");

        subcommands.forEach((name, cmd) -> {
            assertThat(cmd.getCommandSpec().name())
                    .as("spec name of subcommand '%s'", name)
                    .isEqualTo(name);
            assertThat(cmd.getCommandSpec().usageMessage().description())
                    .as("description of subcommand '%s'", name)
                    .isNotEmpty();
        });
    }

    /**
     * The merged commands keep every option the V5 command and the former {@code -v4} command had, so
     * no command line that worked with either of them is rejected as unknown.
     */
    @Test
    public void testMergedCommandsKeepEveryOptionOfBothClients() throws Exception {
        CommandLine commander = initCommander();

        assertThat(optionNames(commander, "produce")).contains(ClientApi.OPTION_NAME).containsAll(List.of(
                "--access-mode", "--admin-url", "--batch-max-bytes", "--batch-max-messages", "--batch-time-window",
                "--chunking", "--compression", "--delay", "--delay-range", "--disable-batching",
                "--encryption-key-name", "--encryption-key-value-file", "--exit-on-failure", "--format-class",
                "--format-payload", "--histogram-file", "--isolated-clients", "--max-outstanding",
                "--max-outstanding-across-partitions", "--message-key-generation-mode", "--num-messages",
                "--num-producers", "--num-test-threads", "--numMessage-perTransaction", "--partitions",
                "--payload-delimiter", "--payload-file", "--producer-name", "--rate", "--send-timeout",
                "--separator", "--set-event-time", "--size", "--test-duration", "--txn-enable", "--txn-timeout",
                "--warmup-time", "-abort", "-am", "-au", "-b", "-bb", "-bm", "-ch", "-d", "-db", "-dr", "-e",
                "-ef", "-f", "-fc", "-fp", "-k", "-m", "-mk", "-n", "-nmt", "-np", "-o", "-p", "-pn", "-r",
                "-s", "-set", "-threads", "-time", "-tto", "-txn", "-v", "-z"));
        assertThat(optionNames(commander, "consume")).contains(ClientApi.OPTION_NAME).containsAll(List.of(
                "--acks-delay-millis", "--auto-scaled-receiver-queue-size", "--auto_ack_chunk_q_full",
                "--batch-index-ack", "--encryption-key-value-file", "--expire_time_incomplete_chunked_messages",
                "--histogram-file", "--isolated-clients", "--max_chunked_msg", "--num-consumers",
                "--num-messages", "--num-subscriptions", "--numMessage-perTransaction", "--pool-messages",
                "--rate", "--receiver-queue-size", "--receiver-queue-size-across-partitions", "--replicated",
                "--scalable-consumer-type", "--subscriber-name", "--subscription-position",
                "--subscription-type", "--subscriptions", "--test-duration", "--txn-enable", "--txn-timeout",
                "-abort", "-ac", "-aq", "-e", "-m", "-mc", "-n", "-nmt", "-ns", "-ntxn", "-p", "-pm", "-q",
                "-r", "-rs", "-s", "-sct", "-sp", "-ss", "-st", "-time", "-tto", "-txn", "-v"));
        assertThat(optionNames(commander, "read")).contains(ClientApi.OPTION_NAME).containsAll(List.of(
                "--num-messages", "--rate", "--receiver-queue-size", "--start-message-id", "--test-duration",
                "--use-tls", "-m", "-n", "-q", "-r", "-time"));
        assertThat(optionNames(commander, "transaction")).contains(ClientApi.OPTION_NAME).containsAll(List.of(
                "--admin-url", "--num-subscriptions", "--num-test-threads", "--numMessage-perTransaction-consume",
                "--numMessage-perTransaction-produce", "--number-txn", "--partitions", "--receiver-queue-size",
                "--replicated", "--scalable", "--scalable-segments", "--subscription-position",
                "--subscription-type", "--subscriptions", "--test-duration", "--topics-c", "--topics-p",
                "--txn-disable", "--txn-timeout", "-abort", "-au", "-nmc", "-nmp", "-np", "-ns", "-ntxn", "-q",
                "-rs", "-sp", "-ss", "-st", "-threads", "-time", "-tto", "-txnRate"));
    }

    /** {@code --help} lists the common options first, then one section per client. */
    @Test
    public void testHelpGroupsOptionsByClient() throws Exception {
        CommandLine commander = initCommander();

        assertSections(usage(commander, "produce"), List.of("--rate", "--compression", "--client-api"),
                List.of("--max-outstanding-across-partitions", "--isolated-clients"), List.of());
        assertSections(usage(commander, "consume"), List.of("--subscription-type", "--receiver-queue-size="),
                List.of("--auto-scaled-receiver-queue-size", "--batch-index-ack", "--isolated-clients",
                        "--pool-messages", "--replicated"),
                List.of("--scalable-consumer-type"));
        assertSections(usage(commander, "read"), List.of("--start-message-id"),
                List.of("--receiver-queue-size", "--use-tls"), List.of());
        assertSections(usage(commander, "transaction"), List.of("--topics-c", "--subscription-type"),
                List.of("--replicated"), List.of("--scalable ", "--scalable-segments"));

        assertThat(usage(commander, "produce"))
                .contains("topic:// (scalable) domain are produced to with the V5 client");
    }

    /** The merged commands resolve conf-file defaults and case-insensitive enums, in both spellings. */
    @Test
    public void testConfFileDefaultsAndEnumSpellings() throws Exception {
        PerformanceProducer v4Spelling = parse("brokerServiceUrl=pulsar://from-conf:6650\n",
                "produce", "-am", "exclusiveWithFencing", "my-topic");
        assertThat(v4Spelling.serviceURL).isEqualTo("pulsar://from-conf:6650");
        assertThat(v4Spelling.producerAccessMode).isEqualTo(ProducerAccessMode.ExclusiveWithFencing);

        PerformanceProducer v5Spelling = parse("", "produce", "-am", "EXCLUSIVE_WITH_FENCING",
                "topic://public/default/t");
        assertThat(v5Spelling.producerAccessMode).isEqualTo(ProducerAccessMode.ExclusiveWithFencing);

        PerformanceConsumer consumer = parse("", "consume", "-sp", "EARLIEST", "my-topic");
        assertThat(consumer.subscriptionInitialPosition)
                .isEqualTo(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest);
    }

    @Test
    public void testClientIsPickedFromTheTopicDomain() throws Exception {
        assertThat(this.<PerformanceProducer>parse("", "produce", "my-topic").resolvedClientApi)
                .isEqualTo(ClientApi.V4);
        assertThat(this.<PerformanceProducer>parse("", "produce", "persistent://public/default/t")
                .resolvedClientApi).isEqualTo(ClientApi.V4);
        assertThat(this.<PerformanceProducer>parse("", "produce", "non-persistent://public/default/t")
                .resolvedClientApi).isEqualTo(ClientApi.V4);
        assertThat(this.<PerformanceProducer>parse("", "produce", "topic://public/default/t")
                .resolvedClientApi).isEqualTo(ClientApi.V5);
        assertThat(this.<PerformanceProducer>parse("", "produce", "--client-api", "v5",
                "persistent://public/default/t").resolvedClientApi).isEqualTo(ClientApi.V5);
        assertThat(this.<PerformanceConsumer>parse("", "consume", "-t", "3", "topic://public/default/t")
                .resolvedClientApi).isEqualTo(ClientApi.V5);
        assertThat(this.<PerformanceReader>parse("", "read", "my-topic").resolvedClientApi)
                .isEqualTo(ClientApi.V4);
        assertThat(this.<PerformanceTransaction>parse("", "transaction", "--topics-c", "test-consume",
                "--topics-p", "test-produce").resolvedClientApi).isEqualTo(ClientApi.V4);
        assertThat(this.<PerformanceTransaction>parse("", "transaction", "--topics-c", "topic://public/default/c",
                "--topics-p", "topic://public/default/p").resolvedClientApi).isEqualTo(ClientApi.V5);

        assertThatThrownBy(() -> parse("", "produce", "topic://public/default/a", "persistent://public/default/b",
                "-t", "2")).isInstanceOf(CommandLine.ParameterException.class).hasMessageContaining("Cannot mix");
        assertThatThrownBy(() -> parse("", "transaction", "--topics-c", "topic://public/default/c",
                "--topics-p", "test-produce"))
                .isInstanceOf(CommandLine.ParameterException.class).hasMessageContaining("Cannot mix");
        assertThatThrownBy(() -> parse("", "produce", "--client-api", "v4", "topic://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class);
    }

    /** An option of the other client's section is a usage error; the sections follow the resolved client. */
    @Test
    public void testOptionsOfTheOtherClientAreRejected() throws Exception {
        assertThatThrownBy(() -> parse("", "produce", "--isolated-clients", "2", "topic://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--isolated-clients applies only to the v4 client");
        assertThatThrownBy(() -> parse("", "consume", "--client-api", "v5", "-aq", "persistent://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--auto-scaled-receiver-queue-size applies only to the v4 client");
        assertThatThrownBy(() -> parse("", "consume", "-sct", "Stream", "persistent://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--scalable-consumer-type applies only to the V5 client");
        assertThatThrownBy(() -> parse("", "consume", "-rs", "topic://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--replicated applies only to the v4 client");
        assertThatThrownBy(() -> parse("", "transaction", "--client-api", "v5", "-rs", "--topics-c", "c",
                "--topics-p", "p"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--replicated applies only to the v4 client");
        assertThatThrownBy(() -> parse("", "read", "--use-tls", "topic://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--use-tls applies only to the v4 client");
        assertThatThrownBy(() -> parse("", "transaction", "--scalable", "--topics-c", "c", "--topics-p", "p"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--scalable applies only to the V5 client");

        PerformanceConsumer stream = parse("", "consume", "--client-api", "v5", "-sct", "Stream",
                "persistent://public/default/t");
        assertThat(stream.v5.scalableConsumerType).isEqualTo(PerformanceConsumer.ScalableConsumerType.Stream);
        PerformanceConsumer v4Only = parse("", "consume", "-aq", "-pm", "false", "persistent://public/default/t");
        assertThat(v4Only.v4.autoScaledReceiverQueueSize).isTrue();
        assertThat(v4Only.v4.poolMessages).isFalse();
        PerformanceTransaction scalable = parse("", "transaction", "--client-api", "v5", "--scalable",
                "--topics-c", "c", "--topics-p", "p");
        assertThat(scalable.v5.scalable).isTrue();

        // A conf-file value is not an option typed on the command line, so it never trips the check.
        PerformanceReader confTls = parse("useTls=true\n", "read", "topic://public/default/t");
        assertThat(confTls.v4.useTls).isTrue();
    }

    /** A rejected command line exits with picocli's usage-error code rather than a stack trace. */
    @Test
    public void testRejectionIsAUsageError() throws Exception {
        File confFile = writeConfFile("");
        PulsarPerfTestTool tool = new PulsarPerfTestTool();
        String[] rest = tool.initCommander(new String[]{confFile.getAbsolutePath(), "produce",
                "--isolated-clients", "2", "topic://public/default/t"});
        StringWriter err = new StringWriter();
        tool.commander.setErr(new PrintWriter(err));
        assertThat(tool.commander.execute(rest)).isEqualTo(CommandLine.ExitCode.USAGE);
        assertThat(err.toString()).contains("--isolated-clients applies only to the v4 client")
                .contains("Usage: pulsar-perf produce");
    }

    @Test
    public void testValueBasedRules() throws Exception {
        assertThat(this.<PerformanceReader>parse("", "read", "-m", "1:2", "my-topic").startMessageId)
                .isEqualTo("1:2");
        assertThatThrownBy(() -> parse("", "read", "-m", "1:2", "topic://public/default/t"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--client-api V4");
        assertThatThrownBy(() -> parse("", "read", "-m", "nope", "my-topic"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("invalid start message ID");
        assertThatThrownBy(() -> parse("", "transaction", "--client-api", "v5", "--scalable", "-np", "2",
                "--topics-c", "c", "--topics-p", "p"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--scalable cannot be combined with --partitions");
        assertThatThrownBy(() -> parse("", "produce", "--isolated-clients", "3", "-threads", "2", "my-topic"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("cannot be combined");
    }

    @Test
    public void testIsolatedClientsSpreadProducersOverWorkers() throws Exception {
        PerformanceProducer arguments = parse("", "produce", "--isolated-clients", "2", "--num-producers", "5",
                "my-topic");
        PerformanceProducerV4 distributed = new PerformanceProducerV4(arguments);
        assertThat(distributed.workerCount()).isEqualTo(2);
        assertThat(distributed.producersForWorker(0)).isEqualTo(3);
        assertThat(distributed.producersForWorker(1)).isEqualTo(2);

        PerformanceConsumer consumer = parse("", "consume", "--isolated-clients", "3", "my-topic");
        assertThat(new PerformanceConsumerV4(consumer).isolatedClientCount()).isEqualTo(3);
    }

    /**
     * The reports are emitted by the per-client runners, but must keep naming the command:
     * {@code tests/integration/.../cli/PerfToolTest} matches stdout on
     * {@code "PerformanceProducer - Aggregated throughput stats"} and friends, which is the
     * {@code %logger{36}} rendering of the logger the report line came from.
     */
    @Test
    public void testReportsAreLoggedUnderTheCommandClassName() {
        PerformanceProducer producer = new PerformanceProducer();
        assertThat(new PerformanceProducerV5(producer).log.name()).isEqualTo(PerformanceProducer.class.getName());
        assertThat(new PerformanceProducerV4(producer).log.name()).isEqualTo(PerformanceProducer.class.getName());
        PerformanceConsumer consumer = new PerformanceConsumer();
        assertThat(new PerformanceConsumerV5(consumer).log.name()).isEqualTo(PerformanceConsumer.class.getName());
        assertThat(new PerformanceConsumerV4(consumer).log.name()).isEqualTo(PerformanceConsumer.class.getName());
        PerformanceReader reader = new PerformanceReader();
        assertThat(new PerformanceReaderV5(reader).log.name()).isEqualTo(PerformanceReader.class.getName());
        assertThat(new PerformanceReaderV4(reader).log.name()).isEqualTo(PerformanceReader.class.getName());
        PerformanceTransaction transaction = new PerformanceTransaction();
        assertThat(new PerformanceTransactionV5(transaction).log.name())
                .isEqualTo(PerformanceTransaction.class.getName());
        assertThat(new PerformanceTransactionV4(transaction).log.name())
                .isEqualTo(PerformanceTransaction.class.getName());
    }

    /**
     * The v4 runners map the shared {@code -st} flag onto the v4 client enum by name, so the two sets of
     * constants must stay identical.
     */
    @Test
    public void testSubscriptionTypeNamesMapOntoTheV4ClientEnum() {
        assertThat(names(PerformanceConsumer.SubscriptionType.values()))
                .isEqualTo(names(SubscriptionType.values()));
    }

    private static Set<String> names(Enum<?>[] values) {
        Set<String> names = new TreeSet<>();
        for (Enum<?> value : values) {
            names.add(value.name());
        }
        return names;
    }

    /**
     * {@code --delay-range} accepts any range, so a drawn delay of {@code 0} is a delay the user
     * asked for and must be distinguishable from "no delay flag given" — which rules out carrying
     * the decision in a numeric sentinel.
     */
    @Test
    public void testDeliveryDelayDistinguishesAZeroDelayFromNoDelay() throws Exception {
        assertThat(new PerformanceProducerV5(parse("", "produce", "my-topic")).nextDeliverAfterSeconds())
                .isNull();
        assertThat(new PerformanceProducerV5(parse("", "produce", "-dr", "0,1", "my-topic"))
                .nextDeliverAfterSeconds()).isEqualTo(0L);
        assertThat(new PerformanceProducerV4(parse("", "produce", "-dr", "-5,-4", "my-topic"))
                .nextDeliverAfterSeconds()).isEqualTo(-5L);
        assertThat(new PerformanceProducerV4(parse("", "produce", "-d", "7", "my-topic"))
                .nextDeliverAfterSeconds()).isEqualTo(7L);
    }

    /**
     * Parses a command line the way {@code pulsar-perf} does — through {@link PulsarPerfTestTool} with a
     * conf file — and runs the command's validation, which is where the client is picked.
     */
    @SuppressWarnings("unchecked")
    private <T extends CmdBase> T parse(String conf, String... args) throws Exception {
        File confFile = writeConfFile(conf);
        PulsarPerfTestTool tool = new PulsarPerfTestTool();
        String[] withConf = new String[args.length + 1];
        withConf[0] = confFile.getAbsolutePath();
        System.arraycopy(args, 0, withConf, 1, args.length);
        String[] rest = tool.initCommander(withConf);
        tool.commander.parseArgs(rest);
        T command = (T) tool.commander.getSubcommands().get(args[0]).getCommand();
        command.validate();
        return command;
    }

    private static void assertSections(String help, List<String> common, List<String> v4, List<String> v5) {
        int commonAt = help.indexOf("Common options:");
        int v4At = help.indexOf("v4 client options");
        int v5At = help.indexOf("V5 client options");
        assertThat(commonAt).as("Common section in%n%s", help).isPositive();
        int commonEnd = v4At > 0 ? v4At : v5At > 0 ? v5At : help.length();
        for (String option : common) {
            assertThat(help.indexOf(option, commonAt)).as("%s in the common section of%n%s", option, help)
                    .isBetween(commonAt, commonEnd);
        }
        if (v4.isEmpty()) {
            assertThat(v4At).as("no v4 section in%n%s", help).isNegative();
        } else {
            assertThat(v4At).isGreaterThan(commonAt);
            int v4End = v5At > 0 ? v5At : help.length();
            for (String option : v4) {
                assertThat(help.indexOf(option, v4At)).as("%s in the v4 section of%n%s", option, help)
                        .isBetween(v4At, v4End);
            }
        }
        if (v5.isEmpty()) {
            assertThat(v5At).as("no V5 section in%n%s", help).isNegative();
        } else {
            assertThat(v5At).isGreaterThan(commonAt);
            for (String option : v5) {
                assertThat(help.indexOf(option, v5At)).as("%s in the V5 section of%n%s", option, help)
                        .isGreaterThan(v5At);
            }
        }
    }

    private static String usage(CommandLine commander, String subcommand) {
        StringWriter out = new StringWriter();
        commander.getSubcommands().get(subcommand).usage(new PrintWriter(out));
        return out.toString();
    }

    private static CommandLine initCommander() throws Exception {
        File confFile = writeConfFile("");
        PulsarPerfTestTool tool = new PulsarPerfTestTool();
        tool.initCommander(new String[]{confFile.getAbsolutePath(), "produce", "--help"});
        return tool.commander;
    }

    private static File writeConfFile(String contents) throws Exception {
        File confFile = Files.createTempFile("pulsar-perf-test", ".conf").toFile();
        confFile.deleteOnExit();
        Files.writeString(confFile.toPath(), contents);
        return confFile;
    }

    private static Set<String> optionNames(CommandLine commander, String subcommand) {
        Set<String> names = new TreeSet<>();
        commander.getSubcommands().get(subcommand).getCommandSpec().options()
                .forEach(option -> names.addAll(Arrays.asList(option.names())));
        return names;
    }
}
