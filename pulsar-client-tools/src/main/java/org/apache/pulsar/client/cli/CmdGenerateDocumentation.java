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

import static org.apache.pulsar.internal.CommandDescriptionUtil.getArgDescription;
import static org.apache.pulsar.internal.CommandDescriptionUtil.getCommandDescription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.client.api.PulsarClientException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Getter
@Command(description = "Generate documentation automatically.")
@CustomLog
public class CmdGenerateDocumentation extends AbstractCmd {

    private static final String COMMON_SECTION = "Common options";

    @Spec
    private CommandSpec pulsarClientCommandSpec;

    @Option(names = {"-n", "--command-names"}, description = "List of command names")
    private List<String> commandNames = new ArrayList<>();

    public int run() throws PulsarClientException {
        if (commandNames == null || commandNames.isEmpty()) {
            pulsarClientCommandSpec.parent().subcommands().forEach((k, v) -> {
                if (k.equals("generate_documentation")) {
                    return;
                }
                this.generateDocument(k, v);
            });
        } else {
            commandNames.forEach(module -> {
                CommandLine commandLine = pulsarClientCommandSpec.parent().subcommands().get(module);
                if (commandLine == null) {
                    return;
                }
                if (commandLine.getCommandName().equals("generate_documentation")) {
                    return;
                }
                this.generateDocument(module, commandLine);
            });
        }

        return 0;
    }

    protected String generateDocument(String module, CommandLine parentCmd) {
        StringBuilder sb = new StringBuilder();
        sb.append("## ").append(module).append("\n\n");
        sb.append(commandDescription(parentCmd)).append("\n");
        sb.append("\n\n```shell\n")
                .append("$ pulsar-client ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");

        // Options are listed under the same sections as in --help: the ungrouped options first,
        // then one section per option group heading (for example the client-specific options).
        Map<String, List<OptionSpec>> sections = new LinkedHashMap<>();
        sections.put(COMMON_SECTION, new ArrayList<>());
        for (ArgSpec arg : parentCmd.getCommandSpec().args()) {
            if (arg.hidden() || !(arg instanceof OptionSpec option)) {
                continue;
            }
            String heading = ClientApiOptionGroups.sectionHeading(option);
            sections.computeIfAbsent(heading == null ? COMMON_SECTION : heading, k -> new ArrayList<>())
                    .add(option);
        }
        boolean singleSection = sections.size() == 1;
        sections.forEach((heading, sectionOptions) -> {
            if (!singleSection) {
                sb.append("### ").append(heading.endsWith(":") ? heading.substring(0, heading.length() - 1)
                        : heading).append("\n\n");
            }
            sb.append("|Flag|Description|Default|\n");
            sb.append("|---|---|---|\n");
            sectionOptions.forEach(option -> {
                String descriptions = getArgDescription(option).replace("\n", " ");
                sb.append("| `").append(Arrays.toString(option.names()))
                        .append("` | ").append(descriptions)
                        .append("|").append(option.defaultValue()).append("|\n");
                sb.append("|\n");
            });
            if (!singleSection) {
                sb.append("\n");
            }
        });
        System.out.println(sb.toString());
        return sb.toString();
    }

    /** The command description, with its paragraphs joined into one line. */
    private static String commandDescription(CommandLine cmd) {
        String[] description = cmd.getCommandSpec().usageMessage().description();
        if (description == null || description.length == 0) {
            return getCommandDescription(cmd);
        }
        return Arrays.stream(description).filter(line -> !line.isBlank()).collect(Collectors.joining(" "));
    }
}
