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
package org.apache.pulsar.broker.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import java.util.List;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * The command to generate documents of broker-tool.
 */
public class GenDocCommand extends CliCommand<CliFlags, GenDocCommand.GenDocFlags> {

    private static final String NAME = "gen-doc";
    private static final String DESC = "Generate documents of broker-tool";

    /**
     * The CLI flags of gen docs command.
     */
    protected static class GenDocFlags extends CliFlags {
    }

    public GenDocCommand() {
        super(CliSpec.<GenDocFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(new GenDocFlags())
                .build());
    }

    @Override
    public Boolean apply(CliFlags globalFlags, String[] args) {
        CliSpec<GenDocFlags> newSpec = CliSpec.newBuilder(spec)
                .withRunFunc(cmdFlags -> apply(cmdFlags))
                .build();
        return 0 == Cli.runCli(newSpec, args);
    }

    private boolean apply(GenDocFlags flags) {
        JCommander commander = new JCommander();
        commander.setProgramName("broker-tool");
        commander.addCommand("load-report", new LoadReportCommand.Flags());
        generateDocument(commander);
        return true;
    }

    private void generateDocument(JCommander obj) {
        StringBuilder sb = new StringBuilder();
        sb.append("------------\n\n");
        sb.append("# ").append(BrokerTool.NAME).append("\n\n");
        sb.append("### Usage\n\n");
        sb.append("`$").append(BrokerTool.NAME).append("`\n\n");
        sb.append("------------\n\n");
        sb.append(BrokerTool.DESC).append("\n");
        sb.append("\n\n```bdocs-tab:example_shell\n")
                .append("$ pulsar ").append(BrokerTool.NAME).append(" subcommand")
                .append("\n```");
        sb.append("\n\n");
        for (String s : obj.getCommands().keySet()) {
            sb.append("* `").append(s).append("`\n");
        }
        obj.getCommands().forEach((subK, subV) -> {
            sb.append("\n\n## <em>").append(subK).append("</em>\n\n");
            String subDesc = obj.getUsageFormatter().getCommandDescription(subK);
            if (null != subDesc && !subDesc.isEmpty()) {
                sb.append(subDesc).append("\n");
            }
            sb.append("### Usage\n\n");
            sb.append("------------\n\n\n");
            sb.append("```bdocs-tab:example_shell\n$ pulsar ").append(BrokerTool.NAME).append(" ")
                    .append(subK).append(" options").append("\n```\n\n");
            List<ParameterDescription> options = obj.getCommands().get(subK).getParameters();
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
        });
        System.out.println(sb.toString());
    }
}
