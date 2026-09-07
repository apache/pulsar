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
package org.apache.pulsar.client.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * The {@code produce-v4} / {@code consume-v4} / {@code read-v4} subcommands and the v4-only
 * capabilities they restore.
 */
public class CmdV4CommandsTest {

    @Test
    public void testAllSubcommandsAreRegisteredUnderTheirOwnName() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();

        Map<String, CommandLine> subcommands = commander.getSubcommands();
        assertThat(subcommands.keySet()).contains(
                "produce", "consume", "read", "produce-v4", "consume-v4", "read-v4",
                "generate_documentation");

        subcommands.forEach((name, cmd) -> {
            assertThat(cmd.getCommandSpec().name())
                    .as("spec name of subcommand '%s'", name)
                    .isEqualTo(name);
            assertThat(cmd.getCommandSpec().usageMessage().description())
                    .as("description of subcommand '%s'", name)
                    .isNotEmpty();
        });
    }

    /** The v4 commands must offer at least the flags of their V5 counterpart. */
    @Test
    public void testV4CommandsOfferTheirV5CounterpartsFlags() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();

        assertThat(optionNames(commander, "produce-v4"))
                .containsAll(optionNames(commander, "produce"));
        assertThat(optionNames(commander, "consume-v4"))
                .containsAll(optionNames(commander, "consume"))
                // v4-only: the V5 QueueConsumer cannot seek to a timestamp.
                .contains("--start-timestamp");
        assertThat(optionNames(commander, "read-v4"))
                .containsAll(optionNames(commander, "read"));
    }

    /** Case-insensitive enum parsing must reach the v4 subcommands too. */
    @Test
    public void testCaseInsensitiveEnumsOnV4Subcommands() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();
        commander.parseArgs("consume-v4", "-s", "sub", "-t", "key_shared", "-m", "nondurable",
                "-p", "earliest", "my-topic");

        CmdConsumeV4 cmd = commander.getSubcommands().get("consume-v4").getCommand();
        assertThat(cmd.subscriptionType).isEqualTo(AbstractCmdConsumeCommand.SubscriptionType.Key_Shared);
        assertThat(cmd.subscriptionMode).isEqualTo(AbstractCmdConsumeCommand.SubscriptionMode.NonDurable);
    }

    /** KeyValue schemas are the headline capability {@code produce} lost and {@code produce-v4} keeps. */
    @Test
    public void testProduceV4BuildsKeyValueSchemas() {
        Schema<?> separated = CmdProduceV4.buildSchema("string", "bytes", "separated");
        assertThat(separated.getSchemaInfo().getType()).isEqualTo(SchemaType.KEY_VALUE);
        assertThat(KeyValueEncodingType.valueOf(
                separated.getSchemaInfo().getProperties().get("kv.encoding.type")))
                .isEqualTo(KeyValueEncodingType.SEPARATED);

        Schema<?> inline = CmdProduceV4.buildSchema("string", "bytes", "inline");
        assertThat(inline.getSchemaInfo().getType()).isEqualTo(SchemaType.KEY_VALUE);
        assertThat(KeyValueEncodingType.valueOf(
                inline.getSchemaInfo().getProperties().get("kv.encoding.type")))
                .isEqualTo(KeyValueEncodingType.INLINE);

        // No encoding type: a plain value schema, as before.
        assertThat(CmdProduceV4.buildSchema("string", "bytes", "").getSchemaInfo().getType())
                .isEqualTo(SchemaType.BYTES);
    }

    /**
     * The v4 client builder must be built only when a {@code *-v4} command actually runs. It is
     * resolved from {@code preRun()}, which runs for every invocation, and building it validates
     * the service URL and parses the whole {@code client.conf} — so an eager build would make
     * {@code --help}, {@code generate_documentation} and the V5 commands fail on configurations
     * they never needed a v4 client for.
     */
    @Test
    public void testCommandsThatNeedNoV4ClientRunWithoutAServiceUrl() {
        // No brokerServiceUrl / webServiceUrl / serviceUrl anywhere: rootParams.serviceURL stays null.
        PulsarClientTool tool = new PulsarClientTool(new Properties());
        assertThat(tool.run(new String[]{"--help"})).isZero();
        assertThat(tool.run(new String[]{"generate_documentation", "-n", "produce"})).isZero();
    }

    /**
     * Only the v4 client parses these {@code client.conf} keys, so they must not reach any other
     * command: {@code sslFactoryPlugin} is rejected outright by the v4 configuration loader
     * (PIP-478 removed it) and {@code tlsProtocols} is a typed {@code Set} field that a plain
     * string fails to deserialize into.
     */
    @Test
    public void testV4OnlyConfKeysDoNotBreakTheOtherCommands() {
        Properties properties = new Properties();
        properties.setProperty("brokerServiceUrl", "pulsar://localhost:6650");
        properties.setProperty("sslFactoryPlugin", "com.acme.MyCustomSslFactory");
        properties.setProperty("tlsProtocols", "TLSv1.3");

        PulsarClientTool tool = new PulsarClientTool(properties);
        assertThat(tool.run(new String[]{"--help"})).isZero();
        assertThat(tool.run(new String[]{"generate_documentation", "-n", "produce"})).isZero();
    }

    @Test
    public void testProduceRejectsKeyValueEncodingTypeAndProduceV4AcceptsIt() {
        CmdProduce v5 = new CmdProduce();
        new CommandLine(v5).parseArgs("-kvet", "separated", "-m", "a", "my-topic");
        assertThatThrownBy(v5::validateSchemaOptions)
                .hasMessageContaining("KeyValue schemas");

        CmdProduceV4 v4 = new CmdProduceV4();
        new CommandLine(v4).parseArgs("-kvet", "separated", "-m", "a", "my-topic");
        assertThatCode(v4::validateSchemaOptions).doesNotThrowAnyException();

        CmdProduceV4 invalid = new CmdProduceV4();
        new CommandLine(invalid).parseArgs("-kvet", "nonsense", "-m", "a", "my-topic");
        assertThatThrownBy(invalid::validateSchemaOptions)
                .hasMessageContaining("only 'separated' or 'inline'");

        // An explicitly-empty value is a supplied value, not an absent flag: the pre-migration v4
        // command rejected it, and producing a plain BYTES message instead would silently give a
        // KeyValue-schema consumer the wrong message shape.
        CmdProduceV4 empty = new CmdProduceV4();
        new CommandLine(empty).parseArgs("-kvet", "", "-m", "a", "my-topic");
        assertThatThrownBy(empty::validateSchemaOptions)
                .hasMessageContaining("only 'separated' or 'inline'");

        // ... while an absent flag is fine on both.
        CmdProduceV4 absent = new CmdProduceV4();
        new CommandLine(absent).parseArgs("-m", "a", "my-topic");
        assertThatCode(absent::validateSchemaOptions).doesNotThrowAnyException();
        CmdProduce absentV5 = new CmdProduce();
        new CommandLine(absentV5).parseArgs("-m", "a", "my-topic");
        assertThatCode(absentV5::validateSchemaOptions).doesNotThrowAnyException();
    }

    @Test
    public void testReadV4ParsesASpecificMessageId() {
        assertThat(CmdReadV4.parseMessageId("latest")).isEqualTo(MessageId.latest);
        assertThat(CmdReadV4.parseMessageId("earliest")).isEqualTo(MessageId.earliest);

        MessageIdImpl parsed = (MessageIdImpl) CmdReadV4.parseMessageId("12:34");
        assertThat(parsed.getLedgerId()).isEqualTo(12L);
        assertThat(parsed.getEntryId()).isEqualTo(34L);

        assertThatThrownBy(() -> CmdReadV4.parseMessageId("nope"))
                .hasMessageContaining("'<ledgerId>:<entryId>'");
    }

    @Test
    public void testReadRejectsASpecificMessageIdAndReadV4AcceptsIt() {
        CmdRead v5 = new CmdRead();
        new CommandLine(v5).parseArgs("-m", "12:34", "my-topic");
        assertThatThrownBy(v5::validateArguments)
                .hasMessageContaining("<ledgerId>:<entryId>");

        CmdReadV4 v4 = new CmdReadV4();
        new CommandLine(v4).parseArgs("-m", "12:34", "my-topic");
        assertThatCode(v4::validateArguments).doesNotThrowAnyException();
    }

    /** Over WebSocket the v4 reader encodes a specific message id; the V5 one only has the names. */
    @Test
    public void testReadV4WebSocketUriCarriesTheEncodedMessageId() {
        CmdReadV4 v4 = new CmdReadV4();
        new CommandLine(v4).parseArgs("-m", "12:34", "persistent://public/default/t");
        v4.updateConfig(null, null, "ws://localhost:8080/");

        String expectedId = Base64.getEncoder()
                .encodeToString(new MessageIdImpl(12, 34, -1).toByteArray());
        assertThat(v4.getWebSocketReadUri("persistent://public/default/t"))
                .isEqualTo("ws://localhost:8080/ws/v2/reader/persistent/public/default/t"
                        + "?messageId=" + expectedId);

        CmdReadV4 earliest = new CmdReadV4();
        new CommandLine(earliest).parseArgs("-m", "earliest", "persistent://public/default/t");
        earliest.updateConfig(null, null, "ws://localhost:8080/");
        assertThat(earliest.getWebSocketReadUri("persistent://public/default/t"))
                .endsWith("?messageId=earliest");
    }

    @Test
    public void testConsumeV4RejectsAnInconsistentTimestampRange() {
        CmdConsumeV4 cmd = new CmdConsumeV4();
        new CommandLine(cmd).parseArgs("-s", "sub", "-stp", "100", "-etp", "50", "my-topic");
        assertThatThrownBy(cmd::validateArguments)
                .hasMessageContaining("end timestamp should be greater than start timestamp");

        CmdConsumeV4 negative = new CmdConsumeV4();
        new CommandLine(negative).parseArgs("-s", "sub", "-stp", "-1", "my-topic");
        assertThatThrownBy(negative::validateArguments)
                .hasMessageContaining("start timestamp should be positive");
    }

    private static java.util.Set<String> optionNames(CommandLine commander, String subcommand) {
        java.util.Set<String> names = new java.util.TreeSet<>();
        commander.getSubcommands().get(subcommand).getCommandSpec().options()
                .forEach(option -> names.addAll(java.util.Arrays.asList(option.names())));
        return names;
    }
}
