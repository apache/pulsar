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

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Getter
@Parameters(commandDescription = "Generate documents automatically.")
@Slf4j
public class CmdGenerateDocument extends CmdBase {

    private final JCommander baseJcommander;
    private final IUsageFormatter usageFormatter;

    private PulsarAdminTool tool;

    public CmdGenerateDocument(Supplier<PulsarAdmin> admin) {
        super("documents", admin);
        baseJcommander = new JCommander();
        usageFormatter = new DefaultUsageFormatter(baseJcommander);
        try {
            tool = new PulsarAdminTool(new Properties());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            baseJcommander.usage();
            return;
        }
        for (Map.Entry<String, Class<?>> c : tool.commandMap.entrySet()) {
            try {
                if (!c.getKey().equals("documents")) {
                    baseJcommander.addCommand(
                            c.getKey(), c.getValue().getConstructor(Supplier.class).newInstance(admin));
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println();
                baseJcommander.usage();
                return;
            }
        }
        jcommander.addCommand("generate", new GenerateDocument());
    }

    @Parameters(commandDescription = "Generate document for modules")
    private class GenerateDocument extends CliCommand {

        @Parameter(description = "Please specify the module name, if not, documents will be generated for all modules."
                + "Optional modules(clusters, tenants, brokers, broker-stats, namespaces, topics, schemas, bookies,"
                + "functions, ns-isolation-policy, resource-quotas, functions, sources, sinks)")
        private java.util.List<String> modules;

        @Override
        void run() throws PulsarAdminException {
            StringBuilder sb = new StringBuilder();
            if (modules == null || modules.isEmpty()) {
                baseJcommander.getCommands().forEach((k, v) ->
                    this.generateDocument(sb, k, v)
                );
            } else {
                String module = getOneArgument(modules);
                JCommander obj = baseJcommander.getCommands().get(module);
                this.generateDocument(sb, module, obj);
            }
        }

        private void generateDocument(StringBuilder sb, String module, JCommander obj) {
            sb.append("------------\n\n");
            sb.append("# ").append(module).append("\n\n");
            sb.append("### Usage\n\n");
            sb.append("`$").append(module).append("`\n\n");
            sb.append("------------\n\n");
            sb.append(usageFormatter.getCommandDescription(module)).append("\n");
            sb.append("\n\n```bdocs-tab:example_shell\n")
                    .append("$ pulsar-admin ").append(module).append(" subcommand")
                    .append("\n```");
            sb.append("\n\n");
            CmdBase cmdObj = (CmdBase) obj.getObjects().get(0);
            for (String s : cmdObj.jcommander.getCommands().keySet()) {
                sb.append("* `").append(s).append("`\n");
            }
            cmdObj.jcommander.getCommands().forEach((subK, subV) -> {
                sb.append("\n\n## <em>").append(subK).append("</em>\n\n");
                sb.append(cmdObj.getUsageFormatter().getCommandDescription(subK)).append("\n\n");
                sb.append("### Usage\n\n");
                sb.append("------------\n\n\n");
                sb.append("```bdocs-tab:example_shell\n$ pulsar-admin ").append(module).append(" ")
                        .append(subK).append(" options").append("\n```\n\n");
                List<ParameterDescription> options = cmdObj.jcommander.getCommands().get(subK).getParameters();
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
}
