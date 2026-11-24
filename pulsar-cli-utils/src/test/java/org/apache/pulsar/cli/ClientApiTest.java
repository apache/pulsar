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
package org.apache.pulsar.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
import org.testng.annotations.Test;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.PropertiesDefaultProvider;
import picocli.CommandLine.Spec;

public class ClientApiTest {

    private static final CommandLine CMD = new CommandLine(new SampleCommand());

    private static ClientApi resolve(ClientApi requested, String... topics) {
        return ClientApi.resolve(requested, Arrays.asList(topics), CMD);
    }

    @Test
    public void testResolveFromTopicDomain() {
        assertThat(resolve(null, "my-topic")).isEqualTo(ClientApi.V4);
        assertThat(resolve(null, "persistent://public/default/t")).isEqualTo(ClientApi.V4);
        assertThat(resolve(null, "non-persistent://public/default/t")).isEqualTo(ClientApi.V4);
        assertThat(resolve(null, "topic://public/default/t")).isEqualTo(ClientApi.V5);
        assertThat(resolve(null, "topic://public/default/a", "topic://public/default/b")).isEqualTo(ClientApi.V5);
        assertThat(resolve(null, "a", null, "persistent://public/default/b")).isEqualTo(ClientApi.V4);
    }

    @Test
    public void testResolveWithOverride() {
        assertThat(resolve(ClientApi.V5, "persistent://public/default/t")).isEqualTo(ClientApi.V5);
        assertThat(resolve(ClientApi.V5, "my-topic")).isEqualTo(ClientApi.V5);
        assertThat(resolve(ClientApi.V4, "persistent://public/default/t")).isEqualTo(ClientApi.V4);
        assertThat(resolve(ClientApi.V5, "topic://public/default/t")).isEqualTo(ClientApi.V5);
    }

    @Test
    public void testResolveRejectsInvalidCombinations() {
        assertThatThrownBy(() -> resolve(null, "topic://public/default/a", "persistent://public/default/b"))
                .isInstanceOf(ParameterException.class).hasMessageContaining("Cannot mix");
        assertThatThrownBy(() -> resolve(ClientApi.V4, "topic://public/default/a"))
                .isInstanceOf(ParameterException.class).hasMessageContaining("--client-api V4");
        assertThatThrownBy(() -> resolve(ClientApi.V5, "non-persistent://public/default/a"))
                .isInstanceOf(ParameterException.class).hasMessageContaining("non-persistent");
        assertThatThrownBy(() -> resolve(null, "segment://public/default/a/0"))
                .isInstanceOf(ParameterException.class).hasMessageContaining("segment");
    }

    @Test
    public void testWrongGroupOptionIsRejected() {
        SampleCommand v4Only = parse("-o", "5", "persistent://public/default/t");
        assertThat(v4Only.clientApi).isEqualTo(ClientApi.V4);

        assertThatThrownBy(() -> parse("-o", "5", "topic://public/default/t"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("--max-outstanding applies only to the v4 client");
        // The option groups follow the resolved client, not the topic domain.
        assertThatThrownBy(() -> parse("--client-api", "v5", "-o", "5", "persistent://public/default/t"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("--max-outstanding applies only to the v4 client");
        assertThatThrownBy(() -> parse("-sct", "Stream", "persistent://public/default/t"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("--scalable-consumer-type applies only to the V5 client");

        SampleCommand v5 = parse("--client-api", "v5", "-sct", "Stream", "persistent://public/default/t");
        assertThat(v5.clientApi).isEqualTo(ClientApi.V5);
        assertThat(v5.v5.scalableConsumerType).isEqualTo("Stream");
        // Common options are accepted with either client.
        assertThat(parse("-r", "7", "topic://public/default/t").common.rate).isEqualTo(7);
    }

    @Test
    public void testDefaultsFromProviderDoNotCountAsGiven() {
        Properties properties = new Properties();
        properties.setProperty("maxOutstanding", "42");
        SampleCommand command = new SampleCommand();
        CommandLine commandLine = new CommandLine(command);
        commandLine.setDefaultValueProvider(new PropertiesDefaultProvider(properties));
        commandLine.parseArgs("topic://public/default/t");
        command.validate();
        assertThat(command.clientApi).isEqualTo(ClientApi.V5);
        // Default values reach an @ArgGroup that was not matched on the command line.
        assertThat(command.v4.maxOutstanding).isEqualTo(42);
        assertThat(command.common.rate).isEqualTo(100);
    }

    @Test
    public void testHelpShowsClientSections() {
        StringWriter out = new StringWriter();
        new CommandLine(new SampleCommand()).usage(new PrintWriter(out));
        String help = out.toString();
        int common = help.indexOf("Common options:");
        int v4 = help.indexOf("v4 client options");
        int v5 = help.indexOf("V5 client options");
        assertThat(common).isPositive();
        assertThat(v4).isGreaterThan(common);
        assertThat(v5).isGreaterThan(v4);
        assertThat(help.indexOf("--max-outstanding", common)).isBetween(v4, v5);
        assertThat(help.indexOf("--scalable-consumer-type", common)).isGreaterThan(v5);
        assertThat(help.indexOf("--client-api", common)).isBetween(common, v4);
    }

    @Test
    public void testSectionHeading() {
        CommandSpec spec = new CommandLine(new SampleCommand()).getCommandSpec();
        assertThat(ClientApiOptionGroups.sectionHeading(spec.findOption("-o")))
                .startsWith("v4 client options");
        assertThat(ClientApiOptionGroups.sectionHeading(spec.findOption("-r"))).isEqualTo("Common options:");
        assertThat(ClientApiOptionGroups.clientApiOf(spec.findOption("-r"))).isNull();
    }

    enum AccessMode { Shared, ExclusiveWithFencing }

    enum OtherAccessMode { SHARED, EXCLUSIVE_WITH_FENCING }

    static class AccessModeConverter extends EnumNameConverter<AccessMode> {
        AccessModeConverter() {
            super(AccessMode.class);
        }
    }

    @Test
    public void testEnumNameConverter() {
        AccessModeConverter converter = new AccessModeConverter();
        assertThat(converter.convert("ExclusiveWithFencing")).isEqualTo(AccessMode.ExclusiveWithFencing);
        assertThat(converter.convert("EXCLUSIVE_WITH_FENCING")).isEqualTo(AccessMode.ExclusiveWithFencing);
        assertThat(converter.convert("shared")).isEqualTo(AccessMode.Shared);
        assertThatThrownBy(() -> converter.convert("nope")).isInstanceOf(CommandLine.TypeConversionException.class);
        assertThat(EnumNameConverter.mapByName(AccessMode.ExclusiveWithFencing, OtherAccessMode.class))
                .isEqualTo(OtherAccessMode.EXCLUSIVE_WITH_FENCING);
        assertThat(EnumNameConverter.mapByName(null, OtherAccessMode.class)).isNull();
    }

    private static SampleCommand parse(String... args) {
        SampleCommand command = new SampleCommand();
        CommandLine commandLine = new CommandLine(command);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.parseArgs(args);
        command.validate();
        return command;
    }

    @Command(name = "sample", sortOptions = false)
    static class SampleCommand {
        @Spec
        CommandSpec spec;

        @Parameters(arity = "1")
        List<String> topics;

        @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.COMMON_HEADING)
        Common common = new Common();

        @ArgGroup(exclusive = false, validate = false, order = 2, heading = ClientApiOptionGroups.V4_HEADING)
        V4 v4 = new V4();

        @ArgGroup(exclusive = false, validate = false, order = 3, heading = ClientApiOptionGroups.V5_HEADING)
        V5 v5 = new V5();

        ClientApi clientApi;

        void validate() {
            clientApi = ClientApi.resolve(common.clientApi, topics, spec.commandLine());
            ClientApiOptionGroups.validate(spec, clientApi);
        }
    }

    static class Common {
        @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
        ClientApi clientApi;

        @Option(names = {"-r", "--rate"}, defaultValue = "100")
        int rate = 100;
    }

    static class V4 implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = {"-o", "--max-outstanding"}, descriptionKey = "maxOutstanding", defaultValue = "1000")
        int maxOutstanding = 1000;
    }

    static class V5 implements ClientApiOptionGroups.V5ClientOptions {
        @Option(names = {"-sct", "--scalable-consumer-type"})
        String scalableConsumerType;
    }
}
