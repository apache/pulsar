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
package org.apache.pulsar.common.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Parameters(commandDescription = "Generate documentation automatically.")
public class CmdGenerateDocs {

    @Parameter(
            names = {"-h", "--help"},
            description = "Display help information"
    )
    public boolean help;

    @Parameter(
            names = {"-n", "--command-names"},
            description = "List of command names"
    )
    private List<String> commandNames = new ArrayList<>();

    private static final String name = "gen-doc";
    private final JCommander jcommander;

    public CmdGenerateDocs(String cmdName) {
        jcommander = new JCommander(this);
        jcommander.setProgramName(cmdName);
    }

    public CmdGenerateDocs addCommand(String name, Object command) {
        jcommander.addCommand(name, command);
        return this;
    }

    public boolean run(String[] args) {
        JCommander tmpCmd = new JCommander(this);
        tmpCmd.setProgramName(jcommander.getProgramName() + " " + name);
        try {
            if (args == null) {
                args = new String[]{};
            }
            tmpCmd.parse(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            tmpCmd.usage();
            return false;
        }
        if (help) {
            tmpCmd.usage();
            return true;
        }

        if (commandNames.size() == 0) {
            for (Map.Entry<String, JCommander> cmd : jcommander.getCommands().entrySet()) {
                if (cmd.getKey().equals(name)) {
                    continue;
                }
                System.out.println(generateDocument(cmd.getKey(), jcommander));
            }
        } else {
            for (String commandName : commandNames) {
                if (commandName.equals(name)) {
                    continue;
                }
                if (!jcommander.getCommands().keySet().contains(commandName)) {
                    continue;
                }
                System.out.println(generateDocument(commandName, jcommander));
            }
        }
        return true;
    }

    private String generateDocument(String module, JCommander commander) {
        JCommander cmd = commander.getCommands().get(module);
        StringBuilder sb = new StringBuilder();
        sb.append("------------\n\n");
        sb.append("# ").append(module).append("\n\n");
        sb.append("### Usage\n\n");
        sb.append("`$").append(module).append("`\n\n");
        sb.append("------------\n\n");
        String desc = commander.getUsageFormatter().getCommandDescription(module);
        if (null != desc && !desc.isEmpty()) {
            sb.append(desc).append("\n");
        }
        sb.append("\n\n```bdocs-tab:example_shell\n")
                .append("$ ");
        if (null != jcommander.getProgramName() && !jcommander.getProgramName().isEmpty()) {
            sb.append(jcommander.getProgramName()).append(" ");
        }
        sb.append(module);
        if (cmd.getObjects().size() > 0
                && cmd.getObjects().get(0).getClass().getName().equals("com.beust.jcommander.JCommander")) {
            JCommander cmdObj = (JCommander) cmd.getObjects().get(0);
            sb.append(" subcommand").append("\n```").append("\n\n");
            for (String s : cmdObj.getCommands().keySet()) {
                if (s.equals(name)) {
                    continue;
                }
                sb.append("* `").append(s).append("`\n");
            }
            cmdObj.getCommands().forEach((subK, subV) -> {
                if (!subK.equals(name)) {
                    sb.append("\n\n## <em>").append(subK).append("</em>\n\n");
                    String subDesc = cmdObj.getUsageFormatter().getCommandDescription(subK);
                    if (null != subDesc && !subDesc.isEmpty()) {
                        sb.append(subDesc).append("\n");
                    }
                    sb.append("### Usage\n\n");
                    sb.append("------------\n\n\n");
                    sb.append("```bdocs-tab:example_shell\n$ ");
                    if (null != jcommander.getProgramName() && !jcommander.getProgramName().isEmpty()) {
                        sb.append(jcommander.getProgramName()).append(" ");
                    }
                    sb.append(module).append(" ").append(subK).append(" options").append("\n```\n\n");
                    List<ParameterDescription> options = cmdObj.getCommands().get(subK).getParameters();
                    if (options.size() > 0) {
                        sb.append("Options\n\n\n");
                        sb.append("|Flag|Description|Default|\n");
                        sb.append("|---|---|---|\n");
                    }
                    options.forEach((option) ->
                            sb.append("| `").append(option.getNames())
                                    .append("` | ").append(option.getDescription().replace("\n", " "))
                                    .append("|").append(option.getDefault()).append("|\n")
                    );
                }
            });
        } else {
            sb.append(" options").append("\n```").append("\n\n");
            sb.append("|Flag|Description|Default|\n");
            sb.append("|---|---|---|\n");
            List<ParameterDescription> options = cmd.getParameters();
            options.forEach((option) ->
                    sb.append("| `").append(option.getNames())
                            .append("` | ").append(option.getDescription().replace("\n", " "))
                            .append("|").append(option.getDefault()).append("|\n")
            );
        }
        return sb.toString();
    }
}
