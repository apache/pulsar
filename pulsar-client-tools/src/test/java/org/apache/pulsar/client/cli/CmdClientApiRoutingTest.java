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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * The {@code produce} / {@code consume} / {@code read} commands pick the v4 or the V5 client from the
 * topic domain or {@code --client-api}, and group their client-specific options.
 */
public class CmdClientApiRoutingTest {

    private static final Set<String> ROOT_OPTIONS = Set.of("--url", "--proxy-url", "--proxy-protocol",
            "--auth-plugin", "--listener-name", "--auth-params", "--tlsTrustCertsFilePath", "-ml",
            "--memory-limit", "-h", "--help", "-v", "--version");

    @Test
    public void testSubcommandsAreRegisteredUnderTheirOwnName() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();

        Map<String, CommandLine> subcommands = commander.getSubcommands();
        assertThat(subcommands.keySet())
                .contains("produce", "consume", "read", "generate_documentation")
                .doesNotContain("produce-v4", "consume-v4", "read-v4");

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
     * The merged commands keep every option of the former command pairs (produce + produce-v4,
     * consume + consume-v4, read + read-v4), and add only {@code --client-api}.
     */
    @Test
    public void testMergedCommandsKeepEveryOptionOfTheFormerCommandPairs() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();

        assertThat(commandOptionNames(commander, "produce")).containsExactlyInAnyOrder(
                "--client-api",
                "-m", "--messages", "-f", "--files", "-n", "--num-produce", "-r", "--rate",
                "-db", "--disable-batching", "-c", "--chunking", "-s", "--separator", "-p", "--properties",
                "-k", "--key", "-kvk", "--key-value-key", "-kvkf", "--key-value-key-file",
                "-vs", "--value-schema", "-ks", "--key-schema", "-kvet", "--key-value-encoding-type",
                "-ekn", "--encryption-key-name", "-ekv", "--encryption-key-value",
                "-dr", "--disable-replication");
        assertThat(commandOptionNames(commander, "consume")).containsExactlyInAnyOrder(
                "--client-api",
                "-t", "--subscription-type", "-m", "--subscription-mode", "-s", "--subscription-name",
                "-p", "--subscription-position", "-n", "--num-messages", "--hex", "--hide-content",
                "-r", "--rate", "--regex", "-q", "--queue-size", "-ekv", "--encryption-key-value",
                "-ca", "--crypto-failure-action", "-st", "--schema-type", "-rs", "--replicated",
                "-mp", "--print-metadata", "-etp", "--end-timestamp",
                "-stp", "--start-timestamp", "-mc", "--max_chunked_msg", "-ac", "--auto_ack_chunk_q_full",
                "-pm", "--pool-messages");
        assertThat(commandOptionNames(commander, "read")).containsExactlyInAnyOrder(
                "--client-api",
                "-m", "--start-message-id", "-n", "--num-messages", "--hex", "--hide-content",
                "-r", "--rate", "-ekv", "--encryption-key-value", "-ca", "--crypto-failure-action",
                "-st", "--schema-type", "-mp", "--print-metadata",
                "-i", "--start-message-id-inclusive", "-q", "--queue-size", "-mc", "--max_chunked_msg",
                "-ac", "--auto_ack_chunk_q_full", "-pm", "--pool-messages");
    }

    @DataProvider(name = "sections")
    public Object[][] sections() {
        return new Object[][] {
            {"produce", List.of("--client-api", "--messages", "--encryption-key-value"),
                    List.of("--key-value-key", "--key-value-key-file", "--key-schema",
                            "--key-value-encoding-type", "--disable-replication")},
            {"consume", List.of("--client-api", "--subscription-type", "--subscription-position",
                    "--crypto-failure-action", "--end-timestamp"),
                    List.of("--start-timestamp", "--max_chunked_msg", "--auto_ack_chunk_q_full",
                            "--pool-messages")},
            {"read", List.of("--client-api", "--start-message-id", "--crypto-failure-action"),
                    List.of("--start-message-id-inclusive", "--queue-size", "--max_chunked_msg",
                            "--auto_ack_chunk_q_full", "--pool-messages")},
        };
    }

    /** {@code --help} lists the common options first and then the v4-client options in their own section. */
    @Test(dataProvider = "sections")
    public void testHelpGroupsTheOptionsByClient(String command, List<String> common, List<String> v4Only) {
        String help = usage(command);
        int commonHeading = help.indexOf("Common options:");
        int v4Heading = help.indexOf("v4 client options (default for persistent://");
        assertThat(commonHeading).isPositive();
        assertThat(v4Heading).isGreaterThan(commonHeading);
        assertThat(help).contains("topic:// (scalable) topics use the V5 client");
        // Search after the synopsis, which lists every option too.
        for (String option : common) {
            assertThat(help.indexOf(option, commonHeading)).as(option).isBetween(commonHeading, v4Heading);
        }
        for (String option : v4Only) {
            assertThat(help.indexOf(option, commonHeading)).as(option).isGreaterThan(v4Heading);
        }
    }

    @DataProvider(name = "rejected")
    public Object[][] rejected() {
        return new Object[][] {
            // A v4-only option with a topic:// topic.
            {new String[]{"produce", "-m", "a", "-kvet", "separated", "topic://public/default/t"},
                    "--key-value-encoding-type applies only to the v4 client"},
            {new String[]{"produce", "-m", "a", "--disable-replication", "topic://public/default/t"},
                    "--disable-replication applies only to the v4 client"},
            {new String[]{"consume", "-s", "sub", "--start-timestamp", "5", "topic://public/default/t"},
                    "--start-timestamp applies only to the v4 client"},
            {new String[]{"read", "-q", "5", "topic://public/default/t"},
                    "--queue-size applies only to the v4 client"},
            // The groups follow the resolved client, not the topic domain.
            {new String[]{"consume", "--client-api", "V5", "-s", "sub", "-pm", "false",
                    "persistent://public/default/t"}, "--pool-messages applies only to the v4 client"},
            // --client-api conflicts.
            {new String[]{"produce", "--client-api", "V4", "-m", "a", "topic://public/default/t"},
                    "the v4 client does not support topic://"},
            {new String[]{"consume", "--client-api", "v5", "-s", "sub", "non-persistent://public/default/t"},
                    "the V5 client does not support non-persistent topics"},
            // Value rules that only the v4 client satisfies.
            {new String[]{"read", "-m", "12:34", "topic://public/default/t"},
                    "'latest' or 'earliest' with the V5 client"},
            {new String[]{"read", "--client-api", "V5", "-ekv", "data:application/x-pem-file;base64,AAAA",
                    "persistent://public/default/t"}, "the V5 client supports only file: key URIs"},
        };
    }

    @Test(dataProvider = "rejected")
    public void testRejectsOptionsTheResolvedClientCannotHonor(String[] args, String message) {
        PulsarClientTool tool = new PulsarClientTool(new Properties());
        StringWriter err = new StringWriter();
        tool.getCommander().setErr(new PrintWriter(err));
        assertThat(tool.run(args)).isEqualTo(CommandLine.ExitCode.USAGE);
        assertThat(err.toString()).contains(message);
    }

    @Test
    public void testTopicPicksTheClient() {
        assertThat(resolve("produce", "-m", "a", "my-topic")).isEqualTo(ClientApi.V4);
        assertThat(resolve("produce", "-m", "a", "-kvet", "inline", "persistent://public/default/t"))
                .isEqualTo(ClientApi.V4);
        assertThat(resolve("consume", "-s", "sub", "-stp", "5", "non-persistent://public/default/t"))
                .isEqualTo(ClientApi.V4);
        assertThat(resolve("read", "topic://public/default/t")).isEqualTo(ClientApi.V5);
        assertThat(resolve("consume", "--client-api", "v5", "-s", "sub", "persistent://public/default/t"))
                .isEqualTo(ClientApi.V5);
        assertThat(resolve("read", "--client-api", "V4", "-m", "1:2", "persistent://public/default/t"))
                .isEqualTo(ClientApi.V4);
    }

    @Test
    public void testWebSocketRejectsScalableTopics() {
        CmdProduce cmd = new CmdProduce();
        new CommandLine(cmd).parseArgs("-m", "a", "topic://public/default/t");
        cmd.updateConfig(null, null, "ws://localhost:8080/");
        assertThatThrownBy(cmd::run)
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("WebSocket proxy");
    }

    /** Case-insensitive enum parsing must reach the merged subcommands, with v4 and V5 spellings. */
    @Test
    public void testCaseInsensitiveEnums() {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();
        commander.parseArgs("consume", "-s", "sub", "-t", "key_shared", "-m", "nondurable",
                "-p", "EARLIEST", "--client-api", "v4", "my-topic");

        CmdConsume cmd = commander.getSubcommands().get("consume").getCommand();
        assertThat(cmd.subscriptionType).isEqualTo(CmdConsume.SubscriptionType.Key_Shared);
        assertThat(cmd.subscriptionMode).isEqualTo(CmdConsume.SubscriptionMode.NonDurable);
        assertThat(cmd.clientApi).isEqualTo(ClientApi.V4);
    }

    /** KeyValue schemas are a capability only the v4 client has. */
    @Test
    public void testProduceBuildsKeyValueSchemasForTheV4Client() {
        Schema<?> separated = ProduceV4.buildSchema("string", "bytes", "separated");
        assertThat(separated.getSchemaInfo().getType()).isEqualTo(SchemaType.KEY_VALUE);
        assertThat(KeyValueEncodingType.valueOf(
                separated.getSchemaInfo().getProperties().get("kv.encoding.type")))
                .isEqualTo(KeyValueEncodingType.SEPARATED);

        Schema<?> inline = ProduceV4.buildSchema("string", "bytes", "inline");
        assertThat(inline.getSchemaInfo().getType()).isEqualTo(SchemaType.KEY_VALUE);
        assertThat(KeyValueEncodingType.valueOf(
                inline.getSchemaInfo().getProperties().get("kv.encoding.type")))
                .isEqualTo(KeyValueEncodingType.INLINE);

        // No encoding type: a plain value schema, as before.
        assertThat(ProduceV4.buildSchema("string", "bytes", "").getSchemaInfo().getType())
                .isEqualTo(SchemaType.BYTES);
    }

    @Test
    public void testKeyValueEncodingTypeValidation() {
        assertThatCode(() -> produceCommand("-kvet", "separated", "-m", "a", "my-topic")
                .validateKeyValueEncodingType()).doesNotThrowAnyException();
        assertThatThrownBy(() -> produceCommand("-kvet", "nonsense", "-m", "a", "my-topic")
                .validateKeyValueEncodingType()).hasMessageContaining("only 'separated' or 'inline'");
        // An explicitly-empty value is a supplied value, not an absent flag: producing a plain
        // BYTES message instead would silently give a KeyValue-schema consumer the wrong message shape.
        assertThatThrownBy(() -> produceCommand("-kvet", "", "-m", "a", "my-topic")
                .validateKeyValueEncodingType()).hasMessageContaining("only 'separated' or 'inline'");
        assertThatCode(() -> produceCommand("-m", "a", "my-topic").validateKeyValueEncodingType())
                .doesNotThrowAnyException();
    }

    /**
     * The v4 client builder must be built only when the v4 client is actually used. It is resolved
     * from {@code preRun()}, which runs for every invocation, and building it validates the service
     * URL and parses the whole {@code client.conf} — so an eager build would make {@code --help} and
     * {@code generate_documentation} fail on configurations they never needed a v4 client for.
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
    public void testGeneratedDocumentationUsesTheHelpSections() {
        PulsarClientTool tool = new PulsarClientTool(new Properties());
        String doc = new CmdGenerateDocumentation()
                .generateDocument("consume", tool.getCommander().getSubcommands().get("consume"));
        int common = doc.indexOf("### Common options");
        int v4 = doc.indexOf("### v4 client options");
        assertThat(common).isPositive();
        assertThat(v4).isGreaterThan(common);
        assertThat(doc.indexOf("--subscription-name")).isBetween(common, v4);
        assertThat(doc.indexOf("--start-timestamp")).isGreaterThan(v4);
        assertThat(doc).contains("topic:// (scalable) topics use the V5 client");
    }

    @Test
    public void testReadParsesASpecificMessageIdForTheV4Client() {
        assertThat(ReadV4.parseMessageId("latest")).isEqualTo(MessageId.latest);
        assertThat(ReadV4.parseMessageId("earliest")).isEqualTo(MessageId.earliest);

        MessageIdImpl parsed = (MessageIdImpl) ReadV4.parseMessageId("12:34");
        assertThat(parsed.getLedgerId()).isEqualTo(12L);
        assertThat(parsed.getEntryId()).isEqualTo(34L);

        assertThatThrownBy(() -> ReadV4.parseMessageId("nope"))
                .hasMessageContaining("'<ledgerId>:<entryId>'");
    }

    @Test
    public void testReadAcceptsASpecificMessageIdOnlyWithTheV4Client() {
        CmdRead cmd = new CmdRead();
        new CommandLine(cmd).parseArgs("-m", "12:34", "my-topic");
        assertThatThrownBy(() -> cmd.validateStartMessageId(ClientApi.V5))
                .hasMessageContaining("<ledgerId>:<entryId>");
        assertThatCode(() -> cmd.validateStartMessageId(ClientApi.V4)).doesNotThrowAnyException();

        CmdRead malformed = new CmdRead();
        new CommandLine(malformed).parseArgs("-m", "nope", "my-topic");
        assertThatThrownBy(() -> malformed.validateStartMessageId(ClientApi.V4))
                .isInstanceOf(CommandLine.ParameterException.class);
    }

    /** Over WebSocket a specific message id is encoded the way the v4 reader does. */
    @Test
    public void testReadWebSocketUriCarriesTheEncodedMessageId() {
        CmdRead cmd = new CmdRead();
        new CommandLine(cmd).parseArgs("-m", "12:34", "persistent://public/default/t");
        cmd.updateConfig(null, null, "ws://localhost:8080/");

        String expectedId = Base64.getEncoder()
                .encodeToString(new MessageIdImpl(12, 34, -1).toByteArray());
        assertThat(cmd.getWebSocketReadUri("persistent://public/default/t"))
                .isEqualTo("ws://localhost:8080/ws/v2/reader/persistent/public/default/t"
                        + "?messageId=" + expectedId);

        CmdRead earliest = new CmdRead();
        new CommandLine(earliest).parseArgs("-m", "earliest", "persistent://public/default/t");
        earliest.updateConfig(null, null, "ws://localhost:8080/");
        assertThat(earliest.getWebSocketReadUri("persistent://public/default/t"))
                .endsWith("?messageId=earliest");
    }

    @Test
    public void testConsumeRejectsAnInconsistentTimestampRange() {
        CmdConsume cmd = new CmdConsume();
        new CommandLine(cmd).parseArgs("-s", "sub", "-stp", "100", "-etp", "50", "my-topic");
        assertThatThrownBy(cmd::validateTimestampRange)
                .hasMessageContaining("end timestamp should be greater than start timestamp");

        CmdConsume negative = new CmdConsume();
        new CommandLine(negative).parseArgs("-s", "sub", "-stp", "-1", "my-topic");
        assertThatThrownBy(negative::validateTimestampRange)
                .hasMessageContaining("start timestamp should be positive");
    }

    private static ClientApi resolve(String... args) {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();
        commander.parseArgs(args);
        CommandLine sub = commander.getSubcommands().get(args[0]);
        Object cmd = sub.getCommand();
        if (cmd instanceof CmdProduce produce) {
            return AbstractCmd.resolveClientApi(produce.commandSpec, produce.clientApi, produce.topic, null);
        } else if (cmd instanceof CmdConsume consume) {
            return AbstractCmd.resolveClientApi(consume.commandSpec, consume.clientApi, consume.topic, null);
        } else {
            CmdRead read = (CmdRead) cmd;
            return AbstractCmd.resolveClientApi(read.commandSpec, read.clientApi, read.topic, null);
        }
    }

    private static CmdProduce produceCommand(String... args) {
        CmdProduce cmd = new CmdProduce();
        new CommandLine(cmd).parseArgs(args);
        return cmd;
    }

    private static String usage(String command) {
        CommandLine commander = new PulsarClientTool(new Properties()).getCommander();
        StringWriter out = new StringWriter();
        commander.getSubcommands().get(command).usage(new PrintWriter(out));
        return out.toString();
    }

    /** The command's own option names, without the inherited root options. */
    private static Set<String> commandOptionNames(CommandLine commander, String subcommand) {
        Set<String> names = new TreeSet<>();
        commander.getSubcommands().get(subcommand).getCommandSpec().options()
                .forEach(option -> names.addAll(Arrays.asList(option.names())));
        names.removeAll(ROOT_OPTIONS);
        return names;
    }
}
