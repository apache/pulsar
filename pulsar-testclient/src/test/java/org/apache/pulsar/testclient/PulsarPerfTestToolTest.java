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
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * Subcommand registration of {@code pulsar-perf}, including the v4 variants.
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
        assertThat(subcommands.keySet()).contains(
                "produce", "consume", "read", "transaction",
                "produce-v4", "consume-v4", "read-v4", "transaction-v4",
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

    /** Each v4 command must offer the same flags as its V5 counterpart, so runs stay comparable. */
    @Test
    public void testV4CommandsOfferTheSameFlagsAsTheirV5Counterparts() throws Exception {
        CommandLine commander = initCommander();

        assertThat(optionNames(commander, "produce-v4"))
                .isEqualTo(optionNames(commander, "produce"));
        assertThat(optionNames(commander, "read-v4"))
                .isEqualTo(optionNames(commander, "read"));
        // consume and transaction have V5-only flags that the v4 commands must not advertise.
        assertThat(optionNames(commander, "consume"))
                .containsAll(optionNames(commander, "consume-v4"))
                .contains("--scalable-consumer-type");
        assertThat(optionNames(commander, "consume-v4")).doesNotContain("--scalable-consumer-type");
        assertThat(optionNames(commander, "transaction"))
                .containsAll(optionNames(commander, "transaction-v4"))
                .contains("--scalable");
        assertThat(optionNames(commander, "transaction-v4")).doesNotContain("--scalable");
    }

    /** The v4 commands must resolve conf-file defaults and case-insensitive enums like the others. */
    @Test
    public void testV4CommandsGetConfFileDefaultsAndCaseInsensitiveEnums() throws Exception {
        File confFile = writeConfFile("brokerServiceUrl=pulsar://from-conf:6650\n");
        PulsarPerfTestTool tool = new PulsarPerfTestTool();
        String[] rest = tool.initCommander(new String[]{confFile.getAbsolutePath(),
                "produce-v4", "-am", "exclusive", "my-topic"});

        CommandLine commander = tool.commander;
        commander.parseArgs(rest);
        PerformanceProducerV4 cmd = commander.getSubcommands().get("produce-v4").getCommand();

        assertThat(cmd.serviceURL).isEqualTo("pulsar://from-conf:6650");
        assertThat(cmd.producerAccessMode).isEqualTo(ProducerAccessMode.Exclusive);
    }

    /**
     * The reports are emitted from the shared base classes, but must keep naming the concrete
     * subcommand: {@code tests/integration/.../cli/PerfToolTest} matches stdout on
     * {@code "PerformanceProducer - Aggregated throughput stats"} and friends, which is the
     * {@code %logger{36}} rendering of the logger the report line came from.
     */
    @Test
    public void testReportsAreLoggedUnderTheConcreteCommandClassName() {
        assertThat(new PerformanceProducer().log.name()).isEqualTo(PerformanceProducer.class.getName());
        assertThat(new PerformanceConsumer().log.name()).isEqualTo(PerformanceConsumer.class.getName());
        assertThat(new PerformanceReader().log.name()).isEqualTo(PerformanceReader.class.getName());
        assertThat(new PerformanceTransaction().log.name()).isEqualTo(PerformanceTransaction.class.getName());
        assertThat(new PerformanceProducerV4().log.name()).isEqualTo(PerformanceProducerV4.class.getName());
        assertThat(new PerformanceConsumerV4().log.name()).isEqualTo(PerformanceConsumerV4.class.getName());
        assertThat(new PerformanceReaderV4().log.name()).isEqualTo(PerformanceReaderV4.class.getName());
        assertThat(new PerformanceTransactionV4().log.name())
                .isEqualTo(PerformanceTransactionV4.class.getName());
    }

    /**
     * {@code consume-v4} and {@code transaction-v4} map the shared {@code -st} flag onto the v4
     * client enum by name, so the two sets of constants must stay identical.
     */
    @Test
    public void testSubscriptionTypeNamesMapOntoTheV4ClientEnum() {
        assertThat(names(PerformanceConsumerBase.SubscriptionType.values()))
                .isEqualTo(names(SubscriptionType.values()));
        assertThat(names(PerformanceTransactionBase.SubscriptionType.values()))
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
    public void testDeliveryDelayDistinguishesAZeroDelayFromNoDelay() {
        PerformanceProducer noDelay = new PerformanceProducer();
        assertThat(noDelay.nextDeliverAfterSeconds()).isNull();

        PerformanceProducer zeroFromRange = new PerformanceProducer();
        new CommandLine(zeroFromRange).parseArgs("-dr", "0,1", "my-topic");
        assertThat(zeroFromRange.nextDeliverAfterSeconds()).isEqualTo(0L);

        PerformanceProducerV4 negativeFromRange = new PerformanceProducerV4();
        new CommandLine(negativeFromRange).parseArgs("-dr", "-5,-4", "my-topic");
        assertThat(negativeFromRange.nextDeliverAfterSeconds()).isEqualTo(-5L);

        PerformanceProducerV4 fixedDelay = new PerformanceProducerV4();
        new CommandLine(fixedDelay).parseArgs("-d", "7", "my-topic");
        assertThat(fixedDelay.nextDeliverAfterSeconds()).isEqualTo(7L);
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
