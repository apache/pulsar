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
package org.apache.pulsar.docs.tools;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.Getter;
import lombok.Setter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Getter
@Setter
@Command(showDefaultValues = true, scope = ScopeType.INHERIT)
public class CmdGenerateDocs implements Callable<Integer> {

    @Option(
            names = {"-h", "--help"},
            description = "Display help information",
            usageHelp = true
    )
    public boolean help;

    @Option(
            names = {"-n", "--command-names"},
            description = "List of command names"
    )
    private List<String> commandNames = new ArrayList<>();

    private static final String name = "gen-doc";
    private final CommandLine commander;

    public CmdGenerateDocs(String cmdName) {
        commander = new CommandLine(this);
        commander.setCommandName(cmdName);
    }

    public CmdGenerateDocs addCommand(String name, Object command) {
        commander.addSubcommand(name, command);
        return this;
    }

    public boolean run(String[] args) {
        if (args == null) {
            args = new String[]{};
        }
        return commander.execute(args) == 0;
    }

    private static String getCommandDescription(CommandLine commandLine) {
        String[] description = commandLine.getCommandSpec().usageMessage().description();
        if (description != null && description.length != 0) {
            return description[0];
        }
        return "";
    }

    private static String getArgDescription(ArgSpec argSpec) {
        String[] description = argSpec.description();
        if (description != null && description.length != 0) {
            return description[0];
        }
        return "";
    }

    private String generateDocument(String module, CommandLine commander) {
        StringBuilder sb = new StringBuilder();
        sb.append("# ").append(module).append("\n\n");
        String desc = getCommandDescription(commander);
        if (null != desc && !desc.isEmpty()) {
            sb.append(desc).append("\n");
        }
        sb.append("\n\n```shell\n")
                .append("$ ");
        String commandName = commander.getCommandName();
        sb.append(this.commander.getCommandName() + " " + commandName);
        if (!commander.getSubcommands().isEmpty()) {
            sb.append(" subcommand").append("\n```").append("\n\n");
            commander.getSubcommands().forEach((subK, subV) -> {
                if (!subK.equals(name)) {
                    sb.append("\n\n## ").append(subK).append("\n\n");
                    String subDesc = getCommandDescription(subV);
                    if (null != subDesc && !subDesc.isEmpty()) {
                        sb.append(subDesc).append("\n");
                    }
                    sb.append("```shell\n$ ");
                    sb.append(this.commander.getCommandName()).append(" ");
                    sb.append(module).append(" ").append(subK).append(" options").append("\n```\n\n");
                    List<ArgSpec> argSpecs = subV.getCommandSpec().args();
                    if (argSpecs.size() > 0) {
                        sb.append("|Flag|Description|Default|\n");
                        sb.append("|---|---|---|\n");
                    }

                    argSpecs.forEach(option -> {
                        if (option.hidden() || !(option instanceof OptionSpec)) {
                            return;
                        }
                        sb.append("| `").append(String.join(", ", ((OptionSpec) option).names()))
                                .append("` | ").append(getArgDescription(option).replace("\n", " "))
                                .append("|").append(option.defaultValueString()).append("|\n");
                    });
                }
            });
        } else {
            sb.append(" options").append("\n```").append("\n\n");
            sb.append("|Flag|Description|Default|\n");
            sb.append("|---|---|---|\n");
            List<ArgSpec> argSpecs = commander.getCommandSpec().args();
            argSpecs.forEach(option -> {
                if (option.hidden() || !(option instanceof OptionSpec)) {
                    return;
                }
                sb.append("| `")
                        .append(String.join(", ", ((OptionSpec) option).names()))
                        .append("` | ")
                        .append(getArgDescription(option).replace("\n", " "))
                        .append("|")
                        .append(option.defaultValueString()).append("|\n");
            });
        }
        return sb.toString();
    }

    @Override
    public Integer call() throws Exception {
        if (commandNames.size() == 0) {
            commander.getSubcommands().forEach((name, cmd) -> {
                commander.getOut().println(generateDocument(name, cmd));
            });
        } else {
            for (String commandName : commandNames) {
                if (commandName.equals(name)) {
                    continue;
                }
                CommandLine cmd = commander.getSubcommands().get(commandName);
                if (cmd == null) {
                    continue;
                }
                commander.getOut().println(generateDocument(commandName, cmd));
            }
        }
        return 0;
    }

    @VisibleForTesting
    CommandLine getCommander() {
        return commander;
    }
}
