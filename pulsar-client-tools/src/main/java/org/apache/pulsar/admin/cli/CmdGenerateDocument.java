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
package org.apache.pulsar.admin.cli;

import static org.apache.pulsar.internal.CommandDescriptionUtil.getArgDescription;
import static org.apache.pulsar.internal.CommandDescriptionUtil.getCommandDescription;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Getter
@Command(description = "Generate documents automatically.")
@Slf4j
public class CmdGenerateDocument extends CmdBase {

    @Spec
    private CommandSpec pulsarAdminCommandSpec;

    public CmdGenerateDocument(Supplier<PulsarAdmin> admin) {
        super("documents", admin);
        addCommand("generate", new GenerateDocument());
    }

    @Command(description = "Generate document for modules")
    private class GenerateDocument extends CliCommand {

        @Parameters(description = "Please specify the module name, if not, documents will be generated for all modules."
                + "Optional modules(clusters, tenants, brokers, broker-stats, namespaces, topics, schemas, bookies,"
                + "functions, ns-isolation-policy, resource-quotas, functions, sources, sinks)")
        private java.util.List<String> modules;

        @Override
        void run() throws PulsarAdminException {
            StringBuilder sb = new StringBuilder();
            if (modules == null || modules.isEmpty()) {
                pulsarAdminCommandSpec.parent().subcommands().forEach((k, v) ->
                        this.generateDocument(sb, k, v)
                );
            } else {
                modules.forEach(module -> {
                    CommandLine commandLine = pulsarAdminCommandSpec.parent().subcommands().get(module);
                    if (commandLine == null) {
                        return;
                    }
                    this.generateDocument(sb, module, commandLine);
                });
            }
        }

        private boolean needsLangSupport(String module, String subK) {
            String[] langSupport = {"localrun", "create", "update"};
            return module.equals("functions") && Arrays.asList(langSupport).contains(subK);
        }

        private final Set<String> generatedModule = new HashSet<>();

        private void generateDocument(StringBuilder sb, String module, CommandLine obj) {
            // Filter the deprecated command
            if (generatedModule.contains(module)) {
                return;
            }
            String commandName = obj.getCommandName();
            generatedModule.add(commandName);

            sb.append("# ").append(module).append("\n\n");
            sb.append(getCommandDescription(obj)).append("\n");
            sb.append("\n\n```shell\n")
                    .append("$ pulsar-admin ").append(module).append(" subcommand")
                    .append("\n```");
            sb.append("\n\n");
            obj.getSubcommands().forEach((subK, subV) -> {
                sb.append("\n\n## ").append(subK).append("\n\n");
                sb.append(getCommandDescription(subV)).append("\n\n");
                sb.append("**Command:**\n\n");
                sb.append("```shell\n$ pulsar-admin ").append(module).append(" ")
                        .append(subK).append(" options").append("\n```\n\n");
                List<ArgSpec> options = obj.getCommandSpec().args();
                if (options.size() > 0) {
                    sb.append("**Options:**\n\n");
                    sb.append("|Flag|Description|Default|");
                    if (needsLangSupport(module, subK)) {
                        sb.append("Support|\n");
                        sb.append("|---|---|---|---|\n");
                    } else {
                        sb.append("\n|---|---|---|\n");
                    }
                }
                options.forEach(ele -> {
                    if (ele.hidden() || !(ele instanceof OptionSpec)) {
                        return;
                    }

                    String argDescription = getArgDescription(ele);
                    String[] descriptions = argDescription.replace("\n", " ").split(" #");
                    sb.append("| `").append(Arrays.toString(((OptionSpec) ele).names()))
                            .append("` | ").append(descriptions[0])
                            .append("|").append(ele.defaultValue()).append("|");
                    if (needsLangSupport(module, subK) && descriptions.length > 1) {
                        sb.append(descriptions[1]);
                    }
                    sb.append("|\n");
                });
                System.out.println(sb);
            });
        }
    }
}
